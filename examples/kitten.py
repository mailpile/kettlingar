"""
MyKitten example microservice.
"""
import asyncio
import random

from kettlingar import RPCKitten
from kettlingar.metrics import RPCKittenVarz


class ExtraMethods:
    """
    Class which defines extra API methods, to demonstrate lazy-loading.
    """
    async def api_stretch(self, _request_info):
        """/stretch

        Have a nice stretch.
        """
        return ('text/plain', 'Streeeeeeeetch!\n')


class MyKitten(RPCKitten, RPCKittenVarz):
    """mykitten - A sample kettlingar microservice

    This microservice knows how to meow and how to purr. Purring is
    private and requires authentication, and may go on for a while.
    """
    class Configuration(RPCKitten.Configuration):
        """MyKitten Configuration"""
        APP_NAME = 'mykitten'
        WORKER_NAME = 'Kitty'
        SLEEP_TIME = 1

    async def public_api_web_root(self, _request_info):
        """/

        Serve up a placeholder at the root of the web server.
        """
        return 'text/html', '<html><body><h1>Hello Kitty World!</h1></body></html>'

    async def public_api_meow(self, _request_info):
        """/meow

        This endpoint requires no authentication!

        This endpoint returns `text/plain` content, which will either be
        rendered directly or embedded in a {"mimetype":..., "data": ...}
        object when invoked as an RPC.
        """
        return (
            'text/plain',           # Fixed MIME type of `text/plain`
            'Meow world, meow!\n')  # Meow!

    async def public_api_slow_meow(self, _request_info, delay:float=None):
        """/slow_meow

        Same as above, but with a random delay before responding.
        """
        if delay is None:
            delay = self.config.sleep_time * random.randint(0, 75) / 100.0
        await asyncio.sleep(delay)
        return ('text/plain', 'Meow world, meow after %.2fs!\n' % delay)

    async def api_purr(self, _request_info,
            count:int=1,
            purr:str='purr',
            caps:bool=False):
        """/purr [--count=<N>] [--purr=<sound>] [--caps=Y]

        Authenticated endpoint taking a single argument. The response
        will be encoded as JSON or using msgpack, depending on what the
        caller requested.

        The generated client convenience function, MyKitten.purr() will
        be an async generator yielding results as they are received.
        """
        _format = self.config.worker_name + ' says %(purr)s'
        for i in range(count):
            result = {
                'purr': purr * (i + 1),  # Purring!
                '_format': _format}      # Formatting rule for CLI interface
            if caps:
                result['purr'] = result['purr'].upper()

            yield (
                None,                    # No MIME-type, let framework choose
                result)                  # Result object
            await asyncio.sleep(self.config.sleep_time)

    async def api_both(self, _request_info):
        """/both

        Meow and purr. This demonstrates API methods invoking each-other.
        """
        # pylint: disable=no-member
        yield None, await self.meow()
        async for purr in self.purr(1):
            yield None, purr

    async def api_freakout(self, _request_info):
        """/freakout

        Raise an exception.
        """
        raise ValueError('Nothing is good enough for me!')

    def get_default_methods(self, _request_info):
        """/*

        Respond to any otherwise unrecognized request with a meow.
        """
        return (self.public_api_meow, None)

    async def init_servers(self, servers):
        """Initialize servers

        Dynamically create a `mrow` method during initialization.
        """
        # pylint: disable=attribute-defined-outside-init
        self.api_mrow = self.api_both
        self.expose_methods(ExtraMethods())
        return servers


if __name__ == '__main__':
    MyKitten.Main()
