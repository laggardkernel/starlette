import asyncio
import typing

from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

RequestResponseEndpoint = typing.Callable[[Request], typing.Awaitable[Response]]
DispatchFunction = typing.Callable[
    [Request, RequestResponseEndpoint], typing.Awaitable[Response]
]


class BaseHTTPMiddleware:
    def __init__(self, app: ASGIApp, dispatch: DispatchFunction = None) -> None:
        self.app = app
        self.dispatch_func = self.dispatch if dispatch is None else dispatch

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)
        response = await self.dispatch_func(request, self.call_next)
        await response(scope, receive, send)

    async def call_next(self, request: Request) -> Response:
        """
        Co(lk):
        https://github.com/encode/starlette/issues/919#issuecomment-665477901
        Comment: breaks BackgroundTask cause StreamResponse doesn't exit until
        the app being wrapped has completed -- when task.result() is called.
        """
        loop = asyncio.get_event_loop()
        queue: "asyncio.Queue[typing.Optional[Message]]" = asyncio.Queue()

        scope = request.scope
        receive = request.receive
        send = queue.put

        async def coro() -> None:
            try:
                # Co(lk): intercepts send(), get response from inner middleware by queue
                # Comment: BackgroundTask is awaited, but in an async Task
                await self.app(scope, receive, send)
            finally:
                await queue.put(None)

        task = loop.create_task(coro())
        message = await queue.get()
        if message is None:
            task.result()
            raise RuntimeError("No response returned.")
        # TODO(lk): why stream, for better handling different res from inner handlers?
        # When subclass override dispatch(), a response should be provided by call_next()
        # to let the subclass modify the resp obj.
        # In fact, the inner Router send req to one of the Route, and call Response.__call__(),
        # which at last call "send()" to send the resp content.
        # BaseHTTPMiddleware replaces the real "send" and let Route cache the content
        # in a queue, later it's composed into StreamingResponse, which is more general
        # than the HTML, PlainText, File resp.
        assert message["type"] == "http.response.start"
        async def body_stream() -> typing.AsyncGenerator[bytes, None]:
            while True:
                message = await queue.get()
                if message is None:
                    break
                assert message["type"] == "http.response.body"
                yield message.get("body", b"")
            # Co(lk): StreamResponse doesn't finish sending until async Task is completed
            # the conn may be kept open on some client.
            task.result()

        response = StreamingResponse(
            status_code=message["status"], content=body_stream()
        )
        response.raw_headers = message["headers"]
        return response

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        raise NotImplementedError()  # pragma: no cover
