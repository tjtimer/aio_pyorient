import asyncio
import typing

from aio_pyorient.otypes import OrientRecordLink


class AsyncObject(object):

    def __init__(self, **kwargs):
        self._loop = kwargs.pop("loop", asyncio.get_event_loop())
        self._tasks = {}
        self._cancelled = asyncio.Event(loop=self._loop)
        self._done = asyncio.Event(loop=self._loop)

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
                    *coro_args: tuple or list,
                    name: str = None):
        name = name if name else coro.__name__
        _task = self._loop.create_task(coro(*coro_args))
        self._tasks[name] = _task
        return self._tasks[name]

    def cancel(self):
        self._cancelled.set()
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
        self._done.set()

    def cancel_task(self, name: str = ''):
        task = self._tasks[name]
        if not task.done():
            return task.cancel()
        return task.done()

    async def wait_for(self, fut, timeout=None):
        result = await asyncio.wait_for(fut, timeout, loop=self._loop)
        return result

def parse_cluster_id(cluster_id):
    try:

        if isinstance(cluster_id, str):
            pass
        elif isinstance(cluster_id, int):
            cluster_id = str(cluster_id)
        elif isinstance( cluster_id, bytes ):
            cluster_id = cluster_id.decode("utf-8")
        elif isinstance( cluster_id, OrientRecordLink ):
            cluster_id = cluster_id.get()

        _cluster_id, _position = cluster_id.split( ':' )
        if _cluster_id[0] is '#':
            _cluster_id = _cluster_id[1:]
    except (AttributeError, ValueError):
        # String but with no ":"
        # so treat it as one param
        _cluster_id = cluster_id
    return _cluster_id


def parse_cluster_position(_cluster_position):
    try:

        if isinstance(_cluster_position, str):
            pass
        elif isinstance(_cluster_position, int):
            _cluster_position = str(_cluster_position)
        elif isinstance( _cluster_position, bytes ):
            _cluster_position = _cluster_position.decode("utf-8")
        elif isinstance( _cluster_position, OrientRecordLink ):
            _cluster_position = _cluster_position.get()

        _cluster, _position = _cluster_position.split( ':' )
    except (AttributeError, ValueError):
        # String but with no ":"
        # so treat it as one param
        _position = _cluster_position
    return _position
