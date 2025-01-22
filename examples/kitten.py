import asyncio

from kettlingar import RPCKitten, RPCKittenVarz


class MyKitten(RPCKitten, RPCKittenVarz):
    """mykitten - A sample kettlingar microservice

    This microservice knows how to meow and how to purr. Purring is
    private and requires authentication, and may go on for a while.
    """

    COMMAND_KWARGS = {
        'purr': ['purr', 'count']}  # Enable sanity checks in the CLI

    class Configuration(RPCKitten.Configuration):
        APP_NAME = 'mykitten'
        WORKER_NAME = 'Kitty'

    async def public_api_meow(self, method, headers, body):
        """
        This endpoint requires no authentication!
        """
        return 'text/plain', 'Meow world, meow!\n'

    async def api_purr(self, method, headers, body, count=1, purr='purr'):
        """
        Authenticated endpoint taking a single argument. The response
        will be encoded as JSON or using msgpack, depending on what the
        caller requested.

        The generated convenience function, MyKitten.purr() will be an
        async generator yielding results as they are sent over the wire.
        """
        for i in range(0, int(count)):
            yield None, {
                'purr': purr * (i + 1),
                '_format': '%s says %%(purr)s' % self.config.worker_name}
            await asyncio.sleep(1)


if __name__ == '__main__':
    import sys
    MyKitten.Main(sys.argv[1:])
