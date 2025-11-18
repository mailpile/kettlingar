"""
Test the webkitten routes, templating and 404 handling.
"""
import asyncio
import os
import tempfile

from urllib.request import urlopen
from urllib.error import HTTPError

from kettlingar import RPCKitten, HttpResult
from kettlingar.webkitten import WebKitten, Route


# pylint: disable=consider-using-with
# pylint: disable=too-many-locals


class WebTestKitten(RPCKitten, WebKitten):
    """A testable webkitten."""

    WEB_PREFIX = '/test/'  # The slashes get stripped
    WEB_ROUTES = [
        Route('/halp', 'api_help'),
        Route('/echo', 'api_echo', qs=True),
        Route('/zeroes/<int:n>/', 'api_zeroes'),
        Route('/pages/', 'public_api_pages', 'base.jinja', public=True, subpaths='more'),
        Route('/static/', 'public_api_web_static', public=True, subpaths=True),
        Route('404', 'public_api_e404', public=True, subpaths=True)]

    class Configuration(WebKitten.Configuration):
        """Config"""

    async def api_echo(self, _ri, words:str=None, n:int=0):
        """Just echo back some words."""
        return HttpResult('text/plain', '%s => %s' % (words, n*2))

    async def api_zeroes(self, _ri, n:int=10):
        """Generate some zeroes"""
        yield HttpResult('text/plain', '0')
        for _i in range(n - 1):
            yield b'0'

    async def public_api_pages(self, request_info, more:str=None):
        """Default route, handle random pages."""
        return {
            'title': 'A page: %s' % more,
            'content': self.web_url(request_info)}

    async def public_api_e404(self, _request_info, *_args, **_kwa):
        """A fancy 404 error page."""
        return HttpResult('text/plain', 'Oh noes, it is missing!',
             http_code=404)


async def run_tests(*args):
    """Create a webkitten and test it."""
    try:
        testdir = tempfile.mkdtemp(suffix='webkitten')
        srcdir = os.path.dirname(__file__)

        args = list(args)
        args.extend([
            '--worker-secret=SECRET',
            '--web-static-dir=' + srcdir,
            '--app-state-dir=' + testdir,
            '--app-data-dir=' + testdir])

        kitty = await WebTestKitten(args=args).connect(auto_start=True)
        assert(kitty.web_url().endswith('test/'))

        print('Public URL: %s' % kitty.web_url(public=True))
        print('Private URL: %s' % kitty.web_url())
        print(await kitty.web_routes())

        # Test our web_url helper
        assert(kitty.web_url('/foo.html').endswith('/SECRET/test/foo.html'))
        assert(kitty.web_url('foo.html').endswith('/SECRET/test/foo.html'))
        assert(kitty.web_url('/foo/').endswith('/SECRET/test/foo/'))
        assert('SECRET' not in kitty.web_url('foo.html', public=True))

        # Check a private page
        private_url = kitty.web_url('/pages/foo.html')
        assert(private_url.endswith('/SECRET/test/pages/foo.html'))
        page_data = str(urlopen(private_url).read(), 'utf-8')
        assert('A page: /foo' in page_data)
        assert(private_url in page_data)
        assert(kitty.web_url() in page_data)

        # Check a public-facing page
        public_url = kitty.web_url('/pages/bar.html', public=True)
        page_data = str(urlopen(public_url).read(), 'utf-8')
        assert(public_url in page_data)
        assert(kitty.web_url() not in page_data)

        # Check a remapped API endpint (no template)
        halp_url = kitty.web_url('/halp')
        assert('Usage:' in str(urlopen(halp_url).read(), 'utf-8'))

        # Check query-string parsing and type conversion
        echo_url = kitty.web_url('/echo?n=6&words=wordifications')
        echo_data = str(urlopen(echo_url).read(), 'utf-8')
        assert('wordification' in echo_data)
        assert('12' in echo_data)

        # Check the generator function
        zero_url = kitty.web_url('/zeroes/7/')
        zero_data = str(urlopen(zero_url).read(), 'utf-8')
        assert('0000000' == zero_data)

        # Verify that our 404 route works!
        for pub in (True, False):
            e404_url = kitty.web_url('/no/such/page', public=pub)
            try:
                assert(not urlopen(e404_url).read())
            except HTTPError as e:
                assert(b'Oh noes' in e.read())

        # Verify that we correctly serve binary data
        async def collect(gen):
            data = []
            async for b in gen:
                data.append(b)
            return b''.join(data)

        # Verify that web_static correctly serves binary data
        test_data = open(os.path.join(srcdir, 'all-bytes.bin'), 'rb').read()
        bytes_url = kitty.web_url('/static/all-bytes.bin')
        assert test_data == urlopen(bytes_url).read()
        assert test_data == await collect(kitty.web_static('all-bytes.bin'))

    finally:
        await kitty.quitquitquit()
        await asyncio.sleep(0.1)
        os.rmdir(testdir)


def test_webkitten():
    """Test webkitten"""
    asyncio.run(run_tests())
