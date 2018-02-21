from aio_pyorient.sock import ODBSocket


async def open(
        sock: ODBSocket,
        name: str, credentials: tuple, config: dict = None) -> ODBResponse:
    await sock.write(ODBRequest(name, credentials, **config).send()
    return ODBResponse(result)
