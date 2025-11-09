"""
Test basic RPC Kitten functionality, sync style.
"""
import os
import sys
import tempfile

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# pylint: disable=wrong-import-position disable=import-error
from examples.kitten import MyKitten


OUTPUT = []
my_print = OUTPUT.append


def full_test_function(*args):
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

        kitty = MyKitten(args=args).sync_proxy()

        # Make sure ping before connect fails
        try:
            assert not kitty.ping()
        except kitty.NotRunning:
            pass

        # Connect!
        if loopback:
            kitty.loopback()
        else:
            kitty.connect(auto_start=True)

        # Make sure ping/pong works and includes all the right info
        pong = kitty.ping(docs=True)
        assert pong['pong']
        assert pong.get('loopback', False) == loopback

        # Test basic API functionality
        meow = kitty.meow()
        assert meow['mimetype'] == 'text/plain'
        assert b'Meow' in meow['data']

        # Test: Ensure expose_methods(ExtraMethods()) did its job
        stretch = kitty.stretch()
        assert stretch['mimetype'] == 'text/plain'
        assert b'Streeee' in stretch['data']

        # This is what incremental results look like!
        # This also tests:
        #   - Type hinting based conversion of 0xa -> 10
        #   - Type hinting based conversion of 'n' -> False
        #
        OUTPUT[:] = []
        for result in kitty.purr('0xa', caps='n'):
            my_print(result['purr'])
        assert len(OUTPUT) == 10
        assert OUTPUT[9] == 'purr' * 10

    finally:
        kitty.quitquitquit()
        os.rmdir(testdir)


def test_kitten():
    """Test kitten running in separate process"""
    full_test_function()

def test_kitten_loopback():
    """Test kitten as a module within this process"""
    full_test_function('--loopback')
