"""
Test the [open]metrics mix-in.
"""
import tempfile
import asyncio
import os

from urllib.request import urlopen

from kettlingar import RPCKitten, HttpResult
from kettlingar.webkitten import WebKitten, Route
from kettlingar.metrics import MetricsKitten


# pylint: disable=consider-using-with


class MetricsTester(RPCKitten, WebKitten, MetricsKitten):
    """A testable webkitten with metrics."""
    WEB_ROUTES = [
        Route('/echo', 'api_echo', qs=True),
        Route('/metrics', 'public_api_metrics', qs=True, public=True)]

    class Configuration(RPCKitten.Configuration):
        """Config"""
        APP_NAME = 'metrics'
        WORKER_NAME = 'tests'

    async def api_echo(self, _ri, words:str=None, n:int=0):
        """Just echo back some words."""
        return HttpResult('text/plain', '%s => %s' % (words, n*2))


async def run_tests(*args):
    """Create a webkitten and test it."""
    try:
        testdir = tempfile.mkdtemp(suffix='metricskitten')

        args = list(args)
        args.extend([
            '--worker-secret=SECRET',
            '--app-state-dir=' + testdir,
            '--app-data-dir=' + testdir])

        kitty = await MetricsTester(args=args).connect(auto_start=True)

        metrics = await kitty.metrics()
        assert('start_time,app="metrics",worker="tests"' in metrics)

        # Confirm the openmetrics has the correct MIME type and contains
        # plausible data.
        openmetrics = await kitty.metrics(openmetrics=True)
        assert(openmetrics['mimetype'].startswith('application/openmetrics-text'))
        assert(b'start_time{app="metrics",' in openmetrics['data'])

        # These should NOT be present yet, we don't have enough samples
        assert(b'http_elapsed_us_m90' not in openmetrics['data'])
        assert(b'http_elapsed_us_m50' not in openmetrics['data'])
        assert(b'http_elapsed_us_avg' not in openmetrics['data'])

        # Generate some traffic...
        echo_url = kitty.web_url('/echo?n=6&words=wordifications')
        for _ in range(MetricsTester.METRICS_MIN_SAMPLES):
            assert(b'wordification' in urlopen(echo_url).read())

        # The percentiles should be present now
        openmetrics = await kitty.metrics(openmetrics=True)
        assert(b'http_elapsed_us_m90' in openmetrics['data'])
        assert(b'http_elapsed_us_m50' in openmetrics['data'])
        assert(b'http_elapsed_us_avg' in openmetrics['data'])

        # Check public vs. private metrics: the api_echo method is not public,
        # so stats about it should be omitted from the public metrics as well.
        # This also tests requesting labels be applied to all output.
        for pub in (False, True):
            url = kitty.web_url('/metrics?openmetrics=Y&foo=bar', public=pub)
            data = urlopen(url).read()
            assert(b'http{foo="bar", code="200"}' in data)
            assert((b'api_echo{foo="bar", code="200"}' in data) != pub)

    finally:
        await kitty.quitquitquit()
        await asyncio.sleep(0.1)
        os.rmdir(testdir)


def test_metricskitten():
    """Test metrics"""
    asyncio.run(run_tests())
