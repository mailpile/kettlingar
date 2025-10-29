import sys

from .kitten import MyKitten


def test_function():
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

    try:
        kitty.freakout()
    except (ValueError, RuntimeError) as e:
        print('Kitty freaked out: %s' % e)

    # This is what incremental results look like!
    for result in kitty.purr(10):
        print(result['purr'])

    kitty.quitquitquit()


test_function()
