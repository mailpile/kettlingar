"""
Mix-in for RPCKittens that also listen on a TLS-enabled socket.
"""
import asyncio
import os


class TLSKitten:
    """
    A mix-in class for RPCKittens that want to also listen on a
    TLS-enabled socket.

    Users of this class should also ensure their Configuration
    class subclasses TLSKitten.Configuration.
    """
    SELF_SIGNED = 'self-signed'

    # FIXME: Create a ACME_SIGNED mode where we automatically
    #        provision certs from Let's Encrypt or ZeroSSL.

    class Configuration:
        """Configuration for TLS Kittens.

        Set `tls_cert = self-signed` to make TLSKitten auto-create a
        key and self-signed certificate on startup, using the parameters
        defined by the `tls_cert_*` variables.

        Creating certificates and keys requires the `openssl` command-line
        tool be installed and on $PATH.
        """
        TLS_LISTEN_PORT = None
        TLS_LISTEN_HOST = None
        TLS_CERT = None
        TLS_KEY = None
        TLS_CERT_NAME = 'tlskitten'
        TLS_CERT_DAYS = 3650
        TLS_CERT_BITS = 2048
        TLS_RECREATE_CERT = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # Req'd for cooperative inheritance

        # pylint: disable=access-member-before-definition
        self._rpc_start_servers = self._start_servers
        self._start_servers = self._tls_start_servers
        self._tls_context = None
        self._tls_sock = None
        self._tls_desc = None

    def https_url(self, *args, **kwargs):
        """Return an HTTPS URL for communicating with this kitten."""
        _, _, _, path = self.web_url(*args, **kwargs).split('/', 3)
        if self._tls_desc is None:
            urlfile2 = self._urlfile + 's'
            with open(urlfile2, 'r', encoding='utf-8') as fd:
                self._tls_desc = fd.read()

        return 'https://%s/%s' % (self._tls_desc, path)

    def _wrap_make_ssl_transport(self):
        """
        Python's asyncio SSL support completely loses the underlying socket,
        but we would like access to it, so we can log the remote IP address.

        This function monkey-patches the SSL transport creation function to
        preserve the underlying socket and make it accessible.
        """
        loop = asyncio.get_running_loop()

        # pylint: disable=protected-access
        orig_make_ssl_transport = loop._make_ssl_transport
        def _wrapped_make_ssl_transport(rawsock, *args, **kwargs):
            transport = orig_make_ssl_transport(rawsock, *args, **kwargs)
            transport._sock = rawsock
            return transport

        loop._make_ssl_transport = _wrapped_make_ssl_transport

    def _make_self_signed_cert(self, cert_path):
        import subprocess

        tmpkey = '%s.key' % cert_path
        subprocess.run([
            'openssl', 'genrsa', '-out', tmpkey,
                '%s' % self.config.tls_cert_bits],
            check=True)
        subprocess.run([
            'openssl', 'req', '-new', '-x509',
                '-days', '%s' % self.config.tls_cert_days,
                '-subj', '/CN=%s' % self.config.tls_cert_name,
                '-key', tmpkey,
                '-out', cert_path],
            check=True)

        with open(cert_path, 'ab') as pemfile:
            with open(tmpkey, 'rb') as keyfile:
                pemfile.write(b'\n')
                pemfile.write(keyfile.read())

        os.chmod(cert_path, 0o600)
        os.remove(tmpkey)

    def _get_tls_context(self):
        if self._tls_context:
            return self._tls_context

        if not self.config.tls_cert:
            return None

        if self.config.tls_cert == self.SELF_SIGNED:
            key_path = cert_path = os.path.join(
                self.config.app_state_dir,
                self.config.app_name + '.pem')
            if self.config.tls_recreate_cert or not os.path.exists(cert_path):
                self._make_self_signed_cert(cert_path)
        else:
            cert_path = self.config.tls_cert
            key_path = self.config.tls_key or None

        import ssl
        ctx = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert_path, keyfile=key_path)
        self._tls_context = ctx
        return ctx

    async def _tls_start_servers(self):
        servers = await self._rpc_start_servers()

        tls_mode = getattr(self.config, 'tls_cert', None)
        if tls_mode is None:
            self.warning(
                'self.config.tls_cert is unset, not starting TLS server')
            return servers

        self._tls_desc, self._tls_sock = self._make_server_socket(
            listen_host=self.config.tls_listen_host or '0.0.0.0',
            listen_port=self.config.tls_listen_port or 0)

        urlfile2 = self._urlfile + 's'
        with open(urlfile2, 'w', encoding='utf-8') as fd:
            fd.flush()
            os.chmod(urlfile2, 0o600)
            fd.write(self._tls_desc)
            self._clean_files.append(urlfile2)

        servers.append(await asyncio.start_server(self._serve_http,
            sock=self._tls_sock,
            ssl=self._get_tls_context()))

        self._wrap_make_ssl_transport()

        return servers
