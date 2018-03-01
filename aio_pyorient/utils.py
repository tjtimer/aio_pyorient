import asyncio
import inspect
import typing
from collections import namedtuple


ODBSignalPayload = namedtuple('ODBSignalPayload', 'sender, extra')

class ODBSignal:

    def __init__(self, sender, extra=None):
        self._sender = sender
        self._extra = extra
        self._receiver = []

    @property
    def payload(self):
        return ODBSignalPayload(self._sender, self._extra)

    def __call__(self, coro):
        assert inspect.iscoroutinefunction(coro), \
            "First argument must be awaitable, e.g. coroutine or future."
        self._receiver.append(coro)

    async def send(self, sender=None, extra=None):
        sender = sender if sender else self._sender
        extra = self._extra if extra is None else extra
        return await asyncio.gather(
            *(coro(ODBSignalPayload(sender, extra)) for coro in self._receiver)
        )


class AsyncBase:

    def __init__(self, **kwargs):
        self._loop = kwargs.pop("loop", asyncio.get_event_loop())
        self._tasks = {}
        self._cancelled = asyncio.Event(loop=self._loop)
        self._done = asyncio.Event(loop=self._loop)
        self._waiting = asyncio.Event(loop=self._loop)

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def tasks(self):
        return self._tasks.keys()

    @property
    def done(self):
        return self._done.is_set()

    @property
    def waiting(self):
        return self._waiting.is_set()

    @property
    def pending_tasks(self):
        return [name for name, task in self._tasks.items() if not task.done()]

    def create_task(self,
                    coro: typing.Callable,
                    *coro_args: tuple or list):
        _task = self._loop.create_task(coro(*coro_args))
        self._tasks[coro.__name__] = _task
        return _task

    async def cancel(self):
        self._cancelled.set()
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
        self._done.set()

    async def cancel_task(self, name: str = ''):
        task = self._tasks[name]
        if not task.done():
            return task.cancel()
        return task.done()

    async def wait_for(self, fut, timeout=None):
        self._waiting.set()
        try:
            result = await asyncio.wait_for(fut, timeout, loop=self._loop)
            return result
        finally:
            self._waiting.clear()


class AsyncCtx(AsyncBase):
    def __init__(self, *,
                 on_open=None,
                 on_close=None,
                 oo_extra_payload=None,
                 oc_extra_payload=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.on_open = ODBSignal(self, oo_extra_payload)
        self.on_close = ODBSignal(self, oc_extra_payload)
        if on_open is not None:
            if isinstance(on_open, (tuple, list)):
                for sub in on_open:
                    self.on_open(sub)
            else: self.on_open(on_open)
        if on_close is not None:
            if isinstance(on_close, (tuple, list)):
                for sub in on_close:
                    self.on_close(sub)
            else: self.on_close(on_close)

    async def _close(self, *args, **kwargs):
        try:
            await self.cancel()
            await self.close(*args, **kwargs)
            await self.on_close.send(self)
        finally:
            self._done.set()

    async def __aenter__(self):
        await self.on_open.send(self)
        return self

    async def __aexit__(self, *exc_args):
        await self._close()
        return

    async def close(self, *args, **kwargs):
        """
        Overwrite this if you want pending tasks to be cancelled and
        on_close signal to be send.
        """
        return
