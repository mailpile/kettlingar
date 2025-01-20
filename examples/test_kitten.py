import asyncio
import sys

from .kitten import MyKitten


async def test_function():
    kitty = await MyKitten(args=sys.argv[1:]).connect(auto_start=True)

    print('%s' % kitty)
    print('Our first meow: %s' % (await kitty.meow()))

    async for result in kitty.purr(10):
        print(result['purr'])

    await kitty.quitquitquit()

asyncio.run(test_function())
