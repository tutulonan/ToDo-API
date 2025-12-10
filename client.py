import asyncio
import httpx


async def fetch():
    client = httpx.AsyncClient()
    response = await client.request(
        method="GET",
        url="http://127.0.0.1:8000/add",
        params={
            "a": "2",
            "b": "3"
        },
        headers={
            "accept": "application/json"
        },

    )

    print(response.text)

    await client.aclose()


asyncio.run(fetch())
