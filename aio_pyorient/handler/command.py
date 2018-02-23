from aio_pyorient.handler.base import BaseHandler


class Query_Sync(BaseHandler):
    def __init__(self,
                 client,
                 query: str,
                 limit: int=-1,
                 fetch_plan: str='*:0',):
        super().__init__()
