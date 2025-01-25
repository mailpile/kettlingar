import asyncio

from kettlingar import RPCKitten


class FileCat(RPCKitten):
    """filecat - A kettlingar microservice sharing file descriptors"""
    class Configuration(RPCKitten.Configuration):
        APP_NAME = 'filecat'
        WORKER_NAME = 'milton'

    async def api_cat(self, method, headers, body, fd):
        """/cat <fd>

        Returns the output read from an open file descriptor.
        """
        return None, fd.read()

    async def api_read(self, method, headers, body, path):
        """/read <path>

        Returns a file descriptor opened for reading.
        """
        return self.FDS_MIMETYPE, open(path, 'rb')


if __name__ == '__main__':
    import sys
    FileCat.Main(sys.argv[1:])
