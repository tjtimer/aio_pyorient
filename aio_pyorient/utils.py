import asyncio
import concurrent.futures
import functools
import typing
from collections import namedtuple


SignalPayload = namedtuple('SignalPayload', 'sender, extra')

def is_coro(coro):
    return asyncio.iscoroutinefunction(coro)

class Signal:

    def __init__(self, sender, extra=None):
        self._sender = sender
        self._extra = extra
        self._receiver = []

    @property
    def payload(self):
        return SignalPayload(self._sender, self._extra)

    def __call__(self, *coros):
        assert all([is_coro(c) for c in coros])
        self._receiver += coros

    async def send(self, sender=None, extra=None):
        sender = self._sender if sender is None else sender
        extra = self._extra if extra is None else extra
        return await asyncio.gather(
            *(coro(SignalPayload(sender, extra)) for coro in self._receiver)
        )


class TaskCreationError(BaseException):
    pass


class AsyncBase:
    ALL_COMPLETED = asyncio.ALL_COMPLETED
    FIRST_COMPLETED = asyncio.FIRST_COMPLETED
    FIRST_EXCEPTION = asyncio.FIRST_EXCEPTION
    def __init__(self, *,
                 on_setup=None,
                 on_shutdown=None,
                 on_setup_extra_payload=None,
                 on_shutdown_extra_payload=None,
                 **kwargs):
        self._tasks = {}
        self._loop = asyncio.get_event_loop()
        self._is_ready = asyncio.Event(loop=self._loop)
        self._cancelled = asyncio.Event(loop=self._loop)
        self._done = asyncio.Event(loop=self._loop)
        self._waiting = asyncio.Event(loop=self._loop)
        self.on_setup = Signal(self, on_setup_extra_payload)
        self.on_shutdown = Signal(self, on_shutdown_extra_payload)
        if on_setup is not None:
            self.on_setup += on_setup
        if on_shutdown is not None:
            self.on_shutdown += on_shutdown

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

    async def setup(self, *args, **kwargs):
        await self.on_setup.send(self)
        await self._setup(*args, **kwargs)
        self._is_ready.set()
        return self

    async def shutdown(self, *args, **kwargs):
        self._is_ready.clear()
        await self.cancel()
        await self._shutdown(*args, **kwargs)
        await self.on_shutdown.send(self)
        self._done.set()

    async def cancel(self):
        self._cancelled.set()
        for t in self.pending_tasks:
            self._tasks[t].cancel()
        if len(self.pending_tasks) >= 1:
            await self.wait_for(*self._tasks.values(), timeout=0)

    async def wait_for(self, *futs, rw=asyncio.ALL_COMPLETED, timeout=None):
        self._waiting.set()
        try:
            if len(futs) is 1:
                return await asyncio.wait_for(futs, timeout)
            else:
                return await asyncio.wait(futs, timeout=timeout, return_when=rw)
        finally:
            self._waiting.clear()

    def fork(self,
              func: typing.Callable,
              *func_args: tuple or list,
              executor: concurrent.futures.Executor=None,
              **func_kwargs: dict):
        if self.cancelled:
            return
        _coro = self._loop.run_in_executor(
            executor, functools.partial(func, *func_args, **func_kwargs)
        )
        return self.spawn(_coro)

    def spawn(self, func: typing.Coroutine):
        if self.cancelled:
            return
        _task = asyncio.ensure_future(func)
        t_id = len(self._tasks)
        self._tasks[f"{t_id}_{func.__name__}"] =_task
        return _task

class AsyncCtx(AsyncBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def __aenter__(self, *args, **kwargs):
        await self.setup(*args, **kwargs)
        return self

    async def __aexit__(self, *exc_args):
        await self.shutdown()
