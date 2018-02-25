from aio_pyorient.handler import db
from aio_pyorient.handler.response_types import OpenDbResponse
from tests.test_settings import TEST_DB_PASSWORD, TEST_USER, TEST_DB


async def test_db_open(client):
    async with db.OpenDb(client, TEST_DB, TEST_USER, TEST_DB_PASSWORD) as handler:
        assert handler.response_type is OpenDbResponse
        await handler.send()
        assert handler._sent.is_set()
        await handler.read()
        assert handler.done
        response = handler.response
        assert isinstance(response, OpenDbResponse)
        assert response.session_id > 0
        assert response.auth_token not in (b'', None)
        assert len(response.clusters) > 0
        assert response.server_version is not None
