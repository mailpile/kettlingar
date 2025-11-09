"""
Demonstrate how to start/stop/interact with MyKitten
"""
import asyncio
import sys

from .kitten import MyKitten


async def call_slow_meow(kitty, which):
    """A task which slowly meows"""
    print("%s: %s" % (which, await kitty.slow_meow()))


async def test_function():
    """
    Test all the things!
    """
    # pylint: disable=no-member

    args = sys.argv[1:]
    loopback = '--loopback' in args
    if loopback:
        args.remove('--loopback')

    kitty = MyKitten(args=args)
    if loopback:
        await kitty.loopback()
    else:
        await kitty.connect(auto_start=True)

    print('Pinged: %s' % await kitty.ping())
    print('Our first meow: %s' % (await kitty.meow()))
    print('Our first stretch: %s' % (await kitty.stretch()))

    # Send overlapping requests (demonstrate things do not block)
    m1 = asyncio.create_task(call_slow_meow(kitty, "Meow 1"))
    m2 = asyncio.create_task(call_slow_meow(kitty, "Meow 2"))
    m3 = asyncio.create_task(call_slow_meow(kitty, "Meow 3"))
    await asyncio.gather(m1, m2, m3)

    # This is what incremental results look like!
    async for result in kitty.purr(10):
        print(result['purr'])

    await kitty.quitquitquit()


asyncio.run(test_function())
