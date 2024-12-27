import asyncio

async def get_ticker_values(api, trading_pairs):
    tasks = []
    for trading_pair in trading_pairs:
        task = asyncio.create_task(api.get_ticker(trading_pair))
        tasks.append(task)

    responses = await asyncio.gather(*tasks)

    return responses
