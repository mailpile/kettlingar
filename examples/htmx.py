"""
This is a sample kitten which shows how to serve and interact with an
web page using the HTMX framework.
"""
import asyncio

from kettlingar import RPCKitten, HttpResult
from kettlingar.webkitten import Route, WebKitten


class HtmxKitten(RPCKitten, WebKitten):
    """htmxkitten - A sample kettlingar Jinja/HTMX app

    This microservice knows how to meow and how to purr. Purring is
    private and requires authentication, andmay go on for a while.

    There is also a public HTMX document served at / to demonstrate
    how to use HTMX kettlingar as a back-end for HTMX pages. There is
    a demo of how to use Server Sent Events with HTMX as well.

i   The HTMX document is rendered using Jinja, based on the templates
    found in `examples/htmx_templates` and static resources in
    `examples/htmx_static`.
    """
    WEB_JINJA_LOADER_PACKAGE = 'examples.htmx'
    WEB_STATIC_DIR = 'htmx_static'
    WEB_JINJA_DIR = 'htmx_templates'
    WEB_ROUTES = [
        Route('/', 'public_api_web_root', 'root.jinja', public=True),
        Route('/static', 'public_api_web_static', subpaths=True, public=True),
        Route('/events', 'api_events')]

    class Configuration(RPCKitten.Configuration):
        """MyKitten configuration"""
        WORKER_NAME = 'HTMXKitten'
        SLEEP_TIME = 0.5

    async def public_api_web_root(self, _request_info):
        """/

        A minimal public landing page.
        """
        return {
            'title': 'HTMX Kitten Test',
            'purr_sound': 'woof'}

    async def api_events(self, _request_info, count=10):
        """/events

        This is a Server Sent Events endpoint which will send a few events
        to the client before cleanly shutting down.
        """
        # Yielding the text/event-stream MIME-type tells kettlingar this
        # is a Server Sent Events endpoint.
        yield self.sse_start_result(
            {'event': 'startup', 'data': 'Here we go!'})

        # Send some hellos...
        for i in range(count):
            await asyncio.sleep(1)
            yield {
                'event': 'hello',
                'data': 'Hello\nworld %d/%d ...' % (i+1, count)}

        # OK, that's enough!
        await asyncio.sleep(1)
        yield {'event': 'hello', 'data': 'Goodbye\ncruel\nworld!\n\n'}

        # Without this, the page will reconnect automatically.
        yield {'event': 'eom', 'data': 'eom'}

    async def public_api_meow(self, _request_info):
        """/meow"""
        return HttpResult(
            'text/plain',           # Fixed MIME type of `text/plain`
            'Meow world, meow!\n')  # Meow!

    async def api_purr(self, _request_info,
            count:int=1,
            purr:str='purr',
            sleep:float=None,
            caps:bool=False):
        """/purr [--count=<N>] [--purr=<sound>] [--caps=Y]"""
        _format = self.config.worker_name + ' says %(purr)s'
        if caps:
            purr = purr.upper()
        for i in range(count):
            yield {
                'purr': purr * (i + 1),  # Purring!
                '_format': _format}      # Formatting rule for CLI interface
            await asyncio.sleep(sleep or self.config.sleep_time)


if __name__ == '__main__':
    HtmxKitten.Main()
