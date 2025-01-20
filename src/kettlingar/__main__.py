import asyncio
import time

from . import RPCKitten


class TestKitten(RPCKitten):
    async def api_tick(self, method, headers, body):
        while True:
            now = int(time.time())
            yield None, {
                'tick': now,
                'data': '10' * (now % 100000)}
            if not (now % 30):
                break
            await asyncio.sleep(1)

    async def tick(self):
        ticker = await self.call('tick', reconnect=0)
        async for tick in ticker():
            yield tick

async def tester():
    w = TestKitten(args=['--app-state-dir=/tmp'])
    print('State dir = %s' % w.config.app_state_dir)
    await w.connect(auto_start=True)
    print('We are so happy now! Our URL is: ' + w._url)
    async for tick in w.tick():
        print('Tick is: %s, data is %d bytes'
            % (tick['tick'], len(tick['data'])))
    try:
        await w.quitquitquit()
    except ConnectionRefusedError:
        pass

try:
    asyncio.run(tester())
except KeyboardInterrupt:
    pass
