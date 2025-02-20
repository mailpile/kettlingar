import asyncio
import sys

from .kitten import MyKitten


async def test_function():
    args = sys.argv[1:]
    loopback = '--loopback' in args
    if loopback:
        args.remove('--loopback')

    kitty = MyKitten(args=args)
    if loopback:
        await kitty.loopback()
    else:
        await kitty.connect(auto_start=True)

    print('%s' % kitty)
    print('Pinged: %s' % await kitty.ping())
    print('Our first meow: %s' % (await kitty.meow()))

    async for result in kitty.purr(10):
        print(result['purr'])

    await kitty.quitquitquit()


asyncio.run(test_function())
