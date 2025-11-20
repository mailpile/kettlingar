"""
Test services which receive or send open file descriptors.
"""
import asyncio
import copy
import os
import tempfile

from kettlingar import RPCKitten


class FileCat(RPCKitten):
    """filecat - A kettlingar microservice sharing file descriptors"""

    class Configuration(RPCKitten.Configuration):
        """FileCat configuration"""
        APP_NAME = 'filecat'
        WORKER_NAME = 'milton'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._other_filecat = None

    async def api_cat(self, _request_info, fd):
        """Returns the output read from an open file descriptor."""
        return fd.read()

    async def api_read(self, _request_info, path):
        """Returns a file descriptor opened for reading."""
        return self.fds_result(open(path, 'rb'))

    async def api_play_with(self, _request_info, other_worker_name):
        """Configure self to play ping-pong with another filecat"""
        self._other_filecat = await FileCat(args=[
                '--app-state-dir=' + self.config.app_state_dir,
                '--worker-name=' + other_worker_name]
            ).connect(auto_start=False)
        return True

    async def api_ping_pong(self, request_info, ball:str, count:int=5):
        """Play ping-pong with another file-cat!"""
        count -= 1
        if count < 1 or not self._other_filecat:
            yield "I am %s, I am keeping %s" % (self.config.worker_name, ball)
            return

        yield "I am %s, sending %s back!" % (self.config.worker_name, ball)
        async for res in self._other_filecat.ping_pong(ball, count,
                call_reply_to=request_info):
            if not res.get('replied_to_first_fd'):
                yield {'error': 'Delegating reply failed'}


async def run_tests(*args):
    """Create a pair of FileCats and test!"""
    try:
        testdir = tempfile.mkdtemp(suffix='filecat')

        args1 = list(args)
        args1.extend([
            '--worker-secret=SECRET',
            '--app-state-dir=' + testdir,
            '--app-data-dir=' + testdir])
        args2 = copy.copy(args1) + [
            '--worker-name=meowzer']

        kitty1 = await FileCat(args=args1).connect(auto_start=True)
        kitty2 = await FileCat(args=args2).connect(auto_start=True)

        # Send a file descriptor over and let kitty read it...
        with open(__file__, 'rb') as fd:
            contents = str(await kitty1.cat(fd), 'utf-8')
            assert('THIS STRING' in contents)

        # Make kitty do the opening...
        for fd in await kitty2.read(__file__):
            contents = str(fd.read(), 'utf-8')
            assert('THAT STRING' in contents)

        assert(kitty2.config.worker_name != kitty1.config.worker_name)
        assert(await kitty1.play_with(kitty2.config.worker_name))
        assert(await kitty2.play_with(kitty1.config.worker_name))

        results = []
        async for pp in kitty1.ping_pong('mousey', 4):
            assert('mousey' in pp)
            results.append(pp)
        assert(len(results) == 4)
        assert(kitty1.config.worker_name in results[0])
        assert(kitty2.config.worker_name in results[1])
        assert(kitty1.config.worker_name in results[2])
        assert(kitty2.config.worker_name in results[3])

    finally:
        await kitty1.quitquitquit()
        await kitty2.quitquitquit()
        await asyncio.sleep(0.1)
        os.rmdir(testdir)


def test_fd_passing():
    """Test the FileCats"""
    asyncio.run(run_tests())
