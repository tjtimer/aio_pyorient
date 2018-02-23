async def test_connect(client):
    session_id = await client.connect("root", "orient-pw")
    assert session_id >= 0

async def test_db_open(client):
    info, clusters, node_list = await client.open_db(
        "tjs-test", "root", "orient-pw"
    )
    assert info is not None
    assert len(clusters) > 0
    assert len(node_list) > 0

