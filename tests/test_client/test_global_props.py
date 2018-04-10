"""
test_global_props
"""
async def test_global_props(binary_db_client):
    response = await binary_db_client.get_global_props()
    print(response)
    assert response is not None
