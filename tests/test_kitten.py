"""
Test basic RPC Kitten functionality, async style.
"""
import asyncio
import os
import sys
import tempfile

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# pylint: disable=wrong-import-position disable=import-error
from examples.kitten import MyKitten


OUTPUT = []
my_print = OUTPUT.append


async def call_slow_meow(kitty, which, delay):
    """Async task wrapper for calling kitty.slow_meow"""
    my_print("%s: %s" % (which, await kitty.slow_meow(delay=delay)))


async def full_test_function(*args):
    """
    Instanciate, connect to, test and shut down examples.MyKitten.
    """
    try:
        testdir = tempfile.mkdtemp(suffix='rpckittens')

        args = list(args)
        args.extend([
            '--app-state-dir=' + testdir,
            '--app-data-dir=' + testdir,
            '--sleep-time=0'])

        loopback = '--loopback' in args
        if loopback:
            args.remove('--loopback')

        kitty = MyKitten(args=args)

        # Make sure ping before connect fails
        try:
            assert not await kitty.ping()
        except kitty.NotRunning:
            pass

        # Connect!
        if loopback:
            await kitty.loopback()
        else:
            await kitty.connect(auto_start=True)

        # Make sure ping/pong works and includes all the right info
        pong = await kitty.ping(docs=True)
        assert pong['pong']
        assert pong.get('loopback', False) == loopback

        # Test text HTTP responses
        meow = await kitty.meow()
        assert meow['mimetype'] == 'text/plain'
        assert 'Meow' in meow['data']

        # Test binary HTTP responses
        meow = await kitty.blank()
        assert meow['mimetype'] == 'image/gif'
        assert isinstance(meow['data'], bytes)
        assert meow['data'].startswith(b'GIF89a')

        # Test: Ensure expose_methods(ExtraMethods()) did its job
        stretch = await kitty.stretch()
        assert stretch['mimetype'] == 'text/plain'
        assert 'Streeee' in stretch['data']

        # Send overlapping requests.
        # This tests:
        #   - Overlapping requests do not block each-other
        #
        OUTPUT[:] = []
        m1 = asyncio.create_task(call_slow_meow(kitty, "Meow 1", 0.2))
        m2 = asyncio.create_task(call_slow_meow(kitty, "Meow 2", 0.1))
        m3 = asyncio.create_task(call_slow_meow(kitty, "Meow 3", 0))
        await asyncio.gather(m1, m2, m3)
        assert OUTPUT[0].startswith('Meow 3')
        assert OUTPUT[1].startswith('Meow 2')
        assert OUTPUT[2].startswith('Meow 1')

        # This is what incremental results look like!
        # This also tests:
        #   - Type hinting based conversion of 0xa -> 10
        #   - Type hinting based conversion of 'n' -> False
        #
        OUTPUT[:] = []
        async for result in kitty.purr('0xa', caps='n'):
            my_print(result['purr'])
        assert len(OUTPUT) == 10
        assert OUTPUT[9] == 'purr' * 10

        # Ensure the input validation rejects elephants where we want ints
        try:
            async for result in kitty.purr('elephant'):
                assert result == 'not reached'
        except (RuntimeError, ValueError) as exc:
            assert 'invalid literal' in str(exc)

    finally:
        await kitty.quitquitquit()
        await asyncio.sleep(0.1)
        os.rmdir(testdir)


def test_kitten():
    """Test kitten running in separate process"""
    asyncio.run(full_test_function())


def test_kitten_nounix():
    """Test kitten with the unix domain socket disabled"""
    asyncio.run(full_test_function('--worker-use-unixdomain=No'))


def test_kitten_nomsgpack():
    """Test kitten with msgpack serialization disabled"""
    asyncio.run(full_test_function('--worker-use-msgpack=False'))


def test_kitten_loopback():
    """Test kitten as a module within this process"""
    asyncio.run(full_test_function('--loopback'))
