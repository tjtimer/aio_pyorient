"""
test_binary
"""
async def test_binary(binary_db_client):
    response = await binary_db_client.execute("select globalProperties from #0:1")
    for record in response:
        print("record:")
        print(record)
        i = 0
        while True:
            next_b = record.data.read(1)
            if next_b == b'':
                break
            print(f'{i}: {next_b} -> ord: {ord(next_b)}' )
            i += 1
