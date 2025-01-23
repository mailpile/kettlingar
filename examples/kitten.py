import asyncio

from kettlingar import RPCKitten


class MyKitten(RPCKitten):
    """mykitten - A sample kettlingar microservice

    This microservice knows how to meow and how to purr. Purring is
    private and requires authentication, and may go on for a while.
    """
    class Configuration(RPCKitten.Configuration):
        APP_NAME = 'mykitten'
        WORKER_NAME = 'Kitty'

    async def public_api_meow(self, method, headers, body):
        """
        This endpoint requires no authentication!

        This endpoint returns `text/plain` content, which will either be
        rendered directly or embedded in a {"mimetype":..., "data": ...}
        object when invoked as an RPC.
        """
        return (
            'text/plain',           # Fixed MIME type of `text/plain`
            'Meow world, meow!\n')  # Meow!

    async def api_purr(self, method, headers, body, count=1, purr='purr'):
        """
        Authenticated endpoint taking a single argument. The response
        will be encoded as JSON or using msgpack, depending on what the
        caller requested.

        The generated client convenience function, MyKitten.purr() will
        be an async generator yielding results as they are received.
        """
        _format = self.config.worker_name + ' says %(purr)s'
        for i in range(0, int(count)):
            result = {
                'purr': purr * (i + 1),  # Purring!
                '_format': _format}      # Formatting rule for CLI interface

            yield (
                None,                    # No MIME-type, let framework choose
                result)                  # Result object
            await asyncio.sleep(1)


if __name__ == '__main__':
    import sys
    MyKitten.Main(sys.argv[1:])
