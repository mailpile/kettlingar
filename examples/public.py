from kettlingar import RPCKitten


class PublicKitten(RPCKitten):
    """public - A public facing upagekite managed kitten

    This is a kitten which exposes MyKitten to the public Internet, by
    configuring and using upagekite. This service takes care of starting
    and stopping MyKitten as needed.
    """
    class Configuration(RPCKitten.Configuration):
        APP_NAME = 'public_kitten'
        WORKER_NAME = 'upagekite'

        KITE_NAME = 'yourkite.pagekite.me'
        KITE_SECRET = 'abcdef123456'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if ((self.config.kite_name == self.Configuration.KITE_NAME) or
                (self.config.kite_secret == self.Configuration.KITE_SECRET)):
            raise RuntimeError(
                'Please provide both --kite-name= and --kite-secret=')

    async def init_upagekite(self, host_port):
        from upagekite import uPageKite
        from upagekite.proto import Kite, uPageKiteDefaults
        from upagekite.proxy import ProxyManager

        class myPageKiteCfg(uPageKiteDefaults):
            info = self.info
            error = self.error
            #debug = self.debug

        p80 = ProxyManager('www', host_port[0], host_port[1], myPageKiteCfg)
        k80 = Kite(self.config.kite_name, self.config.kite_secret,
            proto='http',
            handler=p80.handle_proxy_frame)

        upk = uPageKite([k80], uPK=myPageKiteCfg)
        upk.serve_forever = upk.main

        return upk

    async def init_servers(self, servers):
        from .htmx import HtmxKitten
        self.mykitten = await HtmxKitten(args=[
            '--app-name=%s' % self.config.app_name]).connect(auto_start=True)

        upk = self.upagekite = await self.init_upagekite(self.mykitten.api_addr)
        servers.append(upk)

        return servers

    async def shutdown(self):
        await self.mykitten.quitquitquit()


if __name__ == '__main__':
    PublicKitten.Main()
