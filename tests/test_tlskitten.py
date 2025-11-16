"""
Test the webkitten routes, templating and 404 handling.
"""
import asyncio
import os
import ssl
import tempfile

from urllib.request import urlopen
from urllib.error import URLError

from kettlingar import RPCKitten, HttpResult
from kettlingar.webkitten import WebKitten, Route
from kettlingar.tlskitten import TLSKitten


# pylint: disable=consider-using-with
# pylint: disable=protected-access


class TLSTestKitten(RPCKitten, TLSKitten, WebKitten):
    """A testable TLS kitten."""

    class Configuration(RPCKitten.Configuration, TLSKitten.Configuration):
        """Configuration!"""
        TLS_CERT = TLSKitten.SELF_SIGNED

    WEB_ROUTES = [Route('/zeroes/<int:n>/', 'api_zeroes', public=True)]

    async def api_zeroes(self, _ri, n:int=10):
        """Generate some zeroes"""
        yield HttpResult('text/plain', '0')
        for _i in range(n - 1):
            yield b'0'


async def run_tests(*args):
    """Create a webkitten and test it."""
    try:
        testdir = tempfile.mkdtemp(suffix='webkitten')

        args = list(args)
        args.extend([
            '--worker-secret=SECRET',
            '--app-state-dir=' + testdir,
            '--app-data-dir=' + testdir])

        kitty = await TLSTestKitten(args=args).connect(auto_start=True)

        # Verify the basic API works
        async for z in kitty.zeroes(n=5):
            assert(z == b'0')

        # Check the generator function
        zero_url = kitty.web_url('/zeroes/7/')
        assert(zero_url.startswith('http:'))
        zero_data = str(urlopen(zero_url).read(), 'utf-8')
        assert('0000000' == zero_data)

        # Try again over HTTPS
        zero_url = kitty.https_url('/zeroes/8/')
        assert(zero_url.startswith('https:'))
        try:
            assert(not str(urlopen(zero_url).read(), 'utf-8'))
        except URLError as e:
            assert('CERTIFICATE_VERIFY_FAILED' in str(e))
            assert('self-signed cert' in str(e))
        yolo_ctx = ssl._create_unverified_context()
        zero_data = str(urlopen(zero_url, context=yolo_ctx).read(), 'utf-8')
        assert('00000000' == zero_data)

    finally:
        await kitty.quitquitquit()
        await asyncio.sleep(0.1)
        os.remove(os.path.join(testdir, 'kettlingar.pem'))
        os.rmdir(testdir)


def test_tlskitten():
    """Test tlskitten"""
    asyncio.run(run_tests())
