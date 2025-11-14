"""
Demonstrate services which receive or send open file descriptors.
"""
from kettlingar import RPCKitten


class FileCat(RPCKitten):
    """filecat - A kettlingar microservice sharing file descriptors"""

    class Configuration(RPCKitten.Configuration):
        """FileCat configuration"""
        APP_NAME = 'filecat'
        WORKER_NAME = 'milton'

    async def api_cat(self, _request_info, fd):
        """/cat <fd>

        Returns the output read from an open file descriptor.
        """
        return fd.read()

    async def api_read(self, _request_info, path):
        """/read <path>

        Returns a file descriptor opened for reading.
        """
        return self.fds_result(open(path, 'rb'))


if __name__ == '__main__':
    FileCat.Main()
