import asyncio
import inspect
import typing
from collections import namedtuple

import arrow as arrow


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


class TaskCreationError(BaseException):
    pass


class AsyncBase:

    def __init__(self, *,
                 on_setup=None,
                 on_shutdown=None,
                 on_setup_extra_payload=None,
                 on_shutdown_extra_payload=None,
                 **kwargs):
        self._loop = kwargs.get("loop", asyncio.get_event_loop())
        self._tasks = {}
        self._is_ready = asyncio.Event(loop=self._loop)
        self._cancelled = asyncio.Event(loop=self._loop)
        self._done = asyncio.Event(loop=self._loop)
        self._waiting = asyncio.Event(loop=self._loop)
        self.on_setup = ODBSignal(self, on_setup_extra_payload)
        self.on_shutdown = ODBSignal(self, on_shutdown_extra_payload)
        if on_setup is not None:
            if isinstance(on_setup, (tuple, list)):
                for sub in on_setup:
                    self.on_setup(sub)
            else: self.on_setup(on_setup)
        if on_shutdown is not None:
            if isinstance(on_shutdown, (tuple, list)):
                for sub in on_shutdown:
                    self.on_shutdown(sub)
            else: self.on_shutdown(on_shutdown)

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def tasks(self):
        return self._tasks.keys()
    @property
    def is_ready(self):
        return self._is_ready.is_set()

    @property
    def done(self):
        return self._done.is_set()

    @property
    def waiting(self):
        return self._waiting.is_set()

    @property
    def cancelled(self):
        return self._cancelled.is_set()

    @property
    def pending_tasks(self):
        return [name for name, task in self._tasks.items() if not task.done()]

    async def setup(self, *args, **kwargs):
        await self.on_setup.send(self)
        await self._setup(*args, **kwargs)
        self._is_ready.set()
        return self

    async def shutdown(self, *args, **kwargs):
        self._cancelled.set()
        await self._shutdown(*args, **kwargs)
        await self.on_shutdown.send(self)
        await self.cancel()
        self._done.set()

    def create_task(self,
                    coro: typing.Callable,
                    *coro_args: tuple or list,
                    **coro_kwargs: dict):
        if self.cancelled:
            raise TaskCreationError(
                f"""
                {self.__class__.__name__} was cancelled 
                and will no longer create new tasks!
                """
            )
        _task = self._loop.create_task(coro(*coro_args, **coro_kwargs))
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

    async def _setup(self, *args, **kwargs):
        """
        Overwrite this if you want _is_ready to be set and
        on_open signal to be send.
        """
        return self

    async def _shutdown(self, *args, **kwargs):
        """
        Overwrite this if you want pending tasks to be cancelled and
        on_close signal to be send.
        """
        return self

class AsyncCtx(AsyncBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def __aenter__(self, *args, **kwargs):
        await self.setup(*args, **kwargs)
        return self

    async def __aexit__(self, *exc_args):
        await self.shutdown()
