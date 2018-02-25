import asyncio
import inspect
import typing
from collections import namedtuple

from aio_pyorient.otypes import OrientRecordLink

ODBSignalPayload = namedtuple('ODBSignalPayload', 'sender, extra')

class ODBSignal:

    def __init__(self, sender, extra_type=None):
        self._sender = sender
        self._extra_type = extra_type
        self._receiver = []

    @property
    def payload(self):
        return ODBSignalPayload(self._sender, self._extra_type)

    def __call__(self, coro):
        assert inspect.isawaitable(coro), \
            "First argument must be awaitable, e.g. coroutine or future."
        self._receiver.append(coro)

    async def send(self, sender=None, extra=None):
        sender = sender if sender else self._sender
        has_extra = self._extra_type is not None and extra is not None
        if has_extra and not isinstance(extra, self._extra_type):
            extra = self._extra_type(*extra)
        return await asyncio.gather(
            *(coro(ODBSignalPayload(sender, extra)) for coro in self._receiver)
        )


class AsyncBase:

    def __init__(self, **kwargs):
        self._loop = kwargs.pop("loop", asyncio.get_event_loop())
        self._tasks = {}
        self._cancelled = asyncio.Event(loop=self._loop)
        self._done = asyncio.Event(loop=self._loop)

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
        result = await asyncio.wait_for(fut, timeout, loop=self._loop)
        return result

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_args):
        self._done.set()
        return
