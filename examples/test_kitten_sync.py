"""
Demonstrate how to start/stop/interact with MyKitten w/o async
"""
import sys

from .kitten import MyKitten


def test_function():
    """
    Test all the things!
    """
    # pylint: disable=no-member

    args = sys.argv[1:]
    loopback = '--loopback' in args
    if loopback:
        args.remove('--loopback')

    kitty = MyKitten(args=args).sync_proxy()
    if loopback:
        kitty.loopback()
    else:
        kitty.connect(auto_start=True)

    print('Pinged: %s' % kitty.ping())
    print('Our first meow: %s' % (kitty.meow()))
    print('Our first stretch: %s' % (kitty.stretch()))

    try:
        kitty.freakout()
    except (ValueError, RuntimeError) as e:
        print('Kitty freaked out: %s' % e)

    # This is what incremental results look like!
    for result in kitty.purr(10):
        print(result['purr'])

    kitty.quitquitquit()


test_function()
