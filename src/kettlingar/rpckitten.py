"""
RPC Kittens! Cute asyncio HTTP-RPC microservices for fun and profit.
"""
import asyncio
import array
import copy
import inspect
import json
import logging
import os
import socket
import sys
import time
import types
import urllib.parse

try:
    import msgpack
except ImportError:
    msgpack = None

try:
    from setproctitle import setproctitle
except ImportError:
    setproctitle = None

try:
    import signal
except ImportError:
    signal = None

from .asynctools import create_background_task, await_success, FileWriter
from .str_utils import str_args


# Apparently, this code is just too much!
#
# pylint: disable=too-many-lines
# pylint: disable=too-many-locals
# pylint: disable=too-many-branches
# pylint: disable=too-many-arguments
# pylint: disable=too-many-statements
# pylint: disable=too-many-public-methods
# pylint: disable=too-many-return-statements
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-positional-arguments
# pylint: disable=bare-except
# pylint: disable=broad-exception-caught


def _mkdirp(path, mode):
    if not os.path.exists(path):
        _mkdirp(os.path.dirname(path), mode)
        os.mkdir(path, mode)


class HttpResult(dict):
    """An HTTP result consists of an explicit MIME-type and some data."""
    http_code = property(lambda s: s.get('http_code', 200))
    http_redirect_to = property(lambda s: s.get('http_redirect_to'))

    mimetype = property(lambda s: s['mimetype'])
    data = property(lambda s: s['data'])

    def __init__(self, mimetype, data,
            redirect_to=None,
            http_code=None):
        self['mimetype'] = mimetype
        self['data'] = data
        if redirect_to:
            self['http_redirect_to'] = redirect_to
            self['http_code'] = 302
        if http_code:
            self['http_code'] = http_code
        if mimetype:
            if mimetype[:5] == 'text/':
                if isinstance(data, (bytes, bytearray)):
                    self['data'] = str(data, 'utf-8')
            else:
                if isinstance(data, str):
                    self['data'] = bytes(data, 'utf-8')
                elif isinstance(data, bytearray):
                    self['data'] = bytes(data)


class RequestInfo:
    """
    Class describing a specific request.
    """
    def __init__(self,
            authed=False,
            reader=None,
            writer=None,
            mimetype=None,
            encoder=None,
            method=None,
            headers=None,
            path=None,
            body=None,
            fds=None,
            t0=None):
        self.t0 = t0 or time.time()
        self.authed = authed
        self.reader = reader
        self.writer = writer
        self.peer = self.getpeername()
        self.mimetype = mimetype
        self.encoder = encoder or (lambda d: d)
        self.method = method
        self.headers = headers
        self.path = path
        self.body = body
        self.fds = [] if (fds is None) else fds
        self.sent = 0
        self.code = 500  # Failing to update this is an error
        self.handler = None
        self.is_generator = False

    socket = property(lambda s: getattr(s.writer._transport, '_sock', None))
    fileno = property(lambda s: s.socket.fileno())
    via_unix_domain = property(lambda s: s.peer[0] == RPCKitten.PEER_UNIX_DOMAIN)

    def getpeername(self):
        """Run getpeername() if we have a socket, otherwise emulate it."""
        # pylint: disable=protected-access
        if hasattr(self.writer._transport, '_sock'):
            return self.socket.getpeername() or [RPCKitten.PEER_UNIX_DOMAIN]
        if hasattr(self.writer._transport, '_ssl_protocol'):
            return [RPCKitten.PEER_SSL_SOCKET]
        return [RPCKitten.PEER_UNKNOWN]

    def write(self, *data, writer=None):
        """Write data directly to the underlying connection."""
        data = b''.join(
            (bytes(d, 'utf-8') if isinstance(d, str) else d) for d in data)
        (writer or self.writer).write(data)
        self.sent += len(data)
        return len(data)


class RPCKitten:
    """
    This is a generic worker process; it runs in the background listening
    for RPC calls over HTTP/1.1.

    Subclasses can expose private methods by creating async functions named
    `api_FUNCTION_NAME` (or `raw_FUNCTION_NAME`), or public (unauthenticated)
    methods by creating async functions named `public_api_FUNCTION_NAME`
    (or `public_raw_FUNCTION_NAME`)

    FIXME: Write more!

    FIXME: Implement a local-code-only mode, where the backend is not a
           separate process.
    """
    _MAGIC_FD = '_FD_BRE_MAGIC_'
    _MAGIC_SOCK = '_SO_BRE_MAGIC_'

    FDS_MIMETYPE = 'application/x-fd-magic'
    SSE_MIMETYPE = 'text/event-stream'
    REPLY_TO_FIRST_FD = 'reply_to_first_fd'

    PEER_SSL_SOCKET = 'ssl-socket'
    PEER_UNIX_DOMAIN = 'unix-domain'
    PEER_UNKNOWN = 'unknown'

    CALL_USE_JSON   = 'call_use_json'
    CALL_REPLY_TO   = 'call_reply_to'
    CALL_MAX_TRIES  = 'call_max_tries'
    CALL_ALLOW_UNIX = 'call_allow_unix'

    TRUE_STRINGS = ['true', 't', 'yes', 'y', '1']

    _HTTP_200_OK = (b'HTTP/1.1 200 OK\n'
                   b'Content-Type: %s\n'
                   b'Connection: close\n\n')
    _HTTP_202_REPLIED_TO_FIRST_FD = (b'HTTP/1.1 202 Accepted\n'
                   b'Content-Type: application/json\n'
                   b'Connection: close\n\n'
                   b'{"replied_to_first_fd": true}\n')
    _HTTP_200_CHUNKED_OK = (b'HTTP/1.1 200 OK\n'
                   b'Transfer-Encoding: chunked\n'
                   b'Content-Type: %s\n'
                   b'Connection: close\n\n')
    _HTTP_200_STATIC_PONG = (b'HTTP/1.1 200 OK\n'
                   b'Content-Type: text/plain\n'
                   b'Connection: close\n\nPong\n')

    _HTTP_CHUNK_BEG = b'%x\r\n'
    _HTTP_CHUNK_END = b'\r\n'

    _HTTP_RESPONSE_UNKNOWN = b'HTTP/1.1 %d Unknown\n'
    _HTTP_RESPONSE = {
        200: b'HTTP/1.1 200 OK\n',
        302: b'HTTP/1.1 302 Moved Permanently\n',
        400: b'HTTP/1.1 400 Invalid Request\n',
        401: b'HTTP/1.1 401 Unauthorized\n',
        403: b'HTTP/1.1 403 Access Denied\n',
        404: b'HTTP/1.1 404 Not Found\n',
        423: b'HTTP/1.1 423 Locked\n',
        500: b'HTTP/1.1 500 Internal Error\n'}

    _HTTP_MIMETYPE = (b'Content-Type: %s\n'
                     b'Connection: close\n\n')
    _HTTP_JSON = (b'Content-Type: application/json\n'
                 b'Connection: close\n\n')
    _HTTP_NOT_FOUND = (b'Content-Type: application/json\n'
                  b'Connection: close\n\n'
                  b'{"error": "Not Found"}\n')
    _HTTP_MOVED = (b'Content-Type: application/json\n'
                  b'Connection: close\n'
                  b'Location: %s\n\n'
                  b'{"redirect": %s}\n')

    class NotRunning(OSError):
        """Exception raised if the service is not running."""

    class Configuration:
        """Default RPC Kitten configuration class."""
        APP_NAME = 'kettlingar'
        APP_DATA_DIR = None
        APP_STATE_DIR = None

        WORKER_NAME = 'worker'
        WORKER_CONFIG = ''

        WORKER_NICE = 0
        WORKER_UMASK = 0o770
        WORKER_SECRET = ''
        WORKER_LISTEN_QUEUE = 5
        WORKER_ACCEPT_TIMEOUT = 1
        WORKER_LISTEN_HOST = '127.0.0.1'
        WORKER_LISTEN_PORT = 0
        WORKER_URL_PATH = ''
        WORKER_USE_TCP = True
        WORKER_USE_UNIXDOMAIN = True
        WORKER_USE_MSGPACK = True
        WORKER_LOG_LEVEL = 0  # 20 sets the default to info

        WORKER_HTTP_REQUEST_TIMEOUT1 = 1.0
        WORKER_HTTP_REQUEST_TIMEOUT2 = 15.0
        WORKER_HTTP_REQUEST_MAX_SIZE = 1024*1024

        _METHODS = ('as_dict', 'configure')

        # pylint: disable=access-member-before-definition
        # pylint: disable=attribute-defined-outside-init

        def __init__(self):
            for cls in self.__class__.__mro__:
                for akey in cls.__dict__:
                    if akey.upper() == akey and akey[:1] != '_':
                        setattr(self, akey.lower(), None)

        def __str__(self):
            out = {}
            for akey, val in self.as_dict().items():
                if isinstance(val, (dict, list)):
                    out[akey] = '--%s=%s' % (akey, json.dumps(val))
                else:
                    out[akey] = '--%s=%s' % (akey, val)
            return '\n'.join(out.values())

        def as_dict(self):
            """Current configuration represented as a dict."""
            results = {}
            for cls in self.__class__.__mro__:
                for akey in cls.__dict__:
                    if akey.upper() == akey and akey[:1] != '_':
                        akey = akey.lower()
                        results[akey] = getattr(self, akey, None)
            return results

        def default_config_file(self):
            """Default configuration file location."""
            return os.path.join(self.app_data_dir, self.worker_name + '.cfg')

        def _set_defaults(self):
            for akey in self.__dict__:
                if akey[:1] != '_' and getattr(self, akey, None) is None:
                    dflt = getattr(self.__class__, akey.upper(), None)
                    if dflt is not None:
                        setattr(self, akey, copy.copy(dflt))

            if self.app_state_dir is None:
                import appdirs
                self.app_state_dir = appdirs.user_state_dir(self.app_name)
                if not os.path.exists(self.app_state_dir):
                    _mkdirp(self.app_state_dir, self.worker_umask)

            if self.app_data_dir is None:
                import appdirs
                self.app_data_dir = appdirs.user_data_dir(self.app_name)
                if not os.path.exists(self.app_data_dir):
                    _mkdirp(self.app_data_dir, self.worker_umask)

        def _configure_from_file(self, path):
            def _clean_line(line):
                if line[:2] == '--':
                    line = line[2:]
                if '#' in line:
                    line = line[:line.index('#')]
                return line.strip()

            file_config = []
            with open(path, 'r', encoding='utf-8') as fd:
                for line in (_clean_line(l) for l in fd):
                    if line:
                        key, val = (p.strip() for p in line.split('='))
                        file_config.append('--%s=%s' % (key, val))

            self.configure(file_config, strict=True, set_defaults=False)

        def configure(self, args=None, strict=True, set_defaults=True):
            """Set defaults and parse arguments to generate a configuration."""
            consumed = set()

            if not args:
                args = []
            for arg in args:
                if arg[:2] == '--' and '=' in arg:
                    key, val = arg[2:].split('=', 1)
                    key = key.replace('-', '_')
                    if key.startswith('_'):
                        raise KeyError(key)
                    if key in self.__dict__:
                        dflt = getattr(self.__class__, key.upper(), None)
                        if val == '':
                            val = dflt
                        elif isinstance(dflt, bool):
                            val = RPCKitten.Bool(val[:1])
                        elif isinstance(dflt, float):
                            val = float(val)
                        elif isinstance(dflt, int):
                            val = int(val)
                        elif isinstance(dflt, list):
                            val = json.loads(val)
                            if not isinstance(val, list):
                                raise ValueError('Not a list: %s' % val)
                        elif isinstance(dflt, dict):
                            val = json.loads(val)
                            if not isinstance(val, dict):
                                raise ValueError('Not a dict: %s' % val)
                        setattr(self, key, val)
                        consumed.add(arg)
                        if key == 'worker_config' and val:
                            if RPCKitten.Bool(val):
                                self._configure_from_file(
                                    self.default_config_file())
                            else:
                                self._configure_from_file(val)

            unconsumed = [a for a in args if a not in consumed]
            if unconsumed and strict:
                raise ValueError('Unrecognized arguments: %s' % unconsumed)

            if set_defaults:
                self._set_defaults()

            if not consumed:
                try:
                    self._configure_from_file(self.default_config_file())
                except OSError:
                    pass

            return unconsumed

    class NeedInfoException(Exception):
        """An exception requesting the user provide more information."""
        http_code = 423

        class Var(dict):
            """A missing variable."""
            def __init__(self, varname,
                    vartype='text', default=None, comment=None):
                super().__init__()
                self['name'] = varname
                self['type'] = vartype
                self['default'] = default
                self['comment'] = comment

        def __init__(self, comment, needed_vars=None, resource=None):
            super().__init__(comment)
            self.resource = resource or False
            self.needed_vars = [self.Var(
                    v['name'], v['type'], v.get('default'), v.get('comment')
                ) for v in (needed_vars or [])]

    class RedirectException(Exception):
        """Raising this triggers an HTTP redirect."""
        def __init__(self, target, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.target = (bytes(target, 'utf-8')
                if isinstance(target, str)
                else target)

    def __init__(self, **kwargs):
        args = kwargs.pop('args', None)
        config = kwargs.pop('config', None)
        super().__init__(**kwargs)  # Cooperate with mixins

        if config is None:
            config = self.Configuration()
            config.configure(args or [])

        self.config = config
        self.name = config.worker_name

        self._servers = []
        self._urlfile = os.path.join(
            config.app_state_dir,
            config.worker_name + '.url')
        self._unixfile = os.path.join(
            config.app_state_dir,
            config.worker_name + '.sock')
        self._clean_files = [self._urlfile, self._unixfile]
        self._peeraddr = None
        self._url = None
        self._sock = None
        self._secret = None
        self._process = None

        self.start_time = None
        self.is_client = True
        self.is_service = False
        self._convenience_methods = set()

    def _command_fullargspec(self, command):
        # This order matters, it should match _handle_http_request rules
        for prefix in ('raw_', 'api_', 'public_raw_', 'public_api_'):
            try:
                return inspect.getfullargspec(getattr(self, prefix+command))
            except AttributeError:
                pass
        raise ValueError('No such command: %s' % command)

    def _command_kwargs(self, command):
        try:
            info = self._command_fullargspec(command)
            if info.varkw:
                return []
            return info.args[-len(info.defaults or ''):] + info.kwonlyargs
        except ValueError:
            pass
        return []

    def _fnames_and_api_funcs(self, obj=None):
        if obj is None:
            obj = self
        for api_fname in dir(obj):
            # This order matters, it should match _handle_http_request rules
            for prefix in ('raw_', 'api_', 'public_raw_', 'public_api_'):
                if api_fname.startswith(prefix):
                    api_func = getattr(obj, api_fname)
                    fname = api_fname[len(prefix):]
                    if ((not hasattr(obj, fname))
                            or (fname in self._convenience_methods)):
                        yield fname, api_func
                        break

    def _all_commands(self, obj=None):
        def _typename(typ):
            if typ is None:
                return None
            if isinstance(typ, types.UnionType):
                return repr(typ)
            return typ.__name__

        def _exa(args, fullargspec):
            explained = {}
            annotations = fullargspec.annotations or {}
            for arg in args:
                explained[arg] = _typename(annotations.get(arg))
            return explained

        found = {}
        for fname, api_func in self._fnames_and_api_funcs(obj=obj):
            try:
                fullargspec = inspect.getfullargspec(api_func)
                found[fname] = {
                    'api_method': api_func,
                    'args': _exa(fullargspec.args[2:], fullargspec),
                    'options': _exa(fullargspec.kwonlyargs, fullargspec),
                    'returns': _typename(fullargspec.annotations.get('return')),
                    'is_generator': inspect.isasyncgenfunction(api_func)}
            except TypeError:
                pass
        return found

    def __str__(self):
        return '%s on %s' % (
            self.__class__.__name__,
            self.api_url or '(not connected)')

    api_url = property(lambda s: s._url)
    api_addr = property(lambda s: s._peeraddr)
    api_secret = property(lambda s: s._secret)

    def _mk_lcfunc(self, _fn, api_method=None, is_generator=False, **_kwa):
        if is_generator:
            async def _func(*args, **kwargs):
                if api_method.__annotations__:
                    args = list(args)
                    self._apply_annotations(api_method, args, kwargs)
                async for result in api_method(None, *args, **kwargs):
                    yield result
        else:
            async def _func(*args, **kwargs):
                if api_method.__annotations__:
                    args = list(args)
                    self._apply_annotations(api_method, args, kwargs)
                return await api_method(None, *args, **kwargs)
        return _func

    def _mk_rcfunc(self, fname, is_generator=False, **_kwa):
        if is_generator:
            async def _func(*args, **kwargs):
                _generator = await self.call(fname, *args, **kwargs)
                if isinstance(_generator, dict):
                    # This happens with call_reply_to
                    yield _generator
                else:
                    async for result in _generator():
                        yield result
        else:
            async def _func(*args, **kwargs):
                return await self.call(fname, *args, **kwargs)
        return _func

    def _local_convenience_methods(self):
        self._make_convenience_methods(self._mk_lcfunc)

    def _remote_convenience_methods(self, extra_methods=None):
        self._make_convenience_methods(self._mk_rcfunc,
            extra_methods=extra_methods)

    def _make_convenience_methods(self, mk_func, extra_methods=None):
        method_info = self._all_commands()
        if extra_methods:
            method_info.update(extra_methods)
        for fname, finfo in method_info.items():
            self._convenience_methods.add(fname)
            setattr(self, fname, mk_func(fname, **finfo))

    def sync_proxy(self):
        """
        Return a proxy object which wraps all the async methods so they can
        be used synchronously (not async). This creates and starts a helper
        thread which runs an asyncio event loop behind the scenes.
        """
        from .asynctools import SyncProxy
        return SyncProxy(self)

    async def loopback(self):
        """
        Use this instead of connect() to behave as a library, handling
        everything locally (without launching a service process).
        """
        self._servers = await self.init_servers([])
        self._setup_service()

    async def connect(self, auto_start=False, retry=3):
        """
        Establish a connection with the running service, optionally
        launching the service if it isn't yet running.
        """
        self._init_logging()

        for tried in range(0, retry + 1):
            try:
                with open(self._urlfile, 'r', encoding='utf-8') as fd:
                    self._url = fd.read().strip()

                pong = await self.call('ping', call_max_tries=0)
                if pong:
                    methods = pong.get('methods')
                    self._remote_convenience_methods(extra_methods=methods)
                    return self

            except (OSError, RuntimeError):
                pass

            if auto_start:
                from multiprocessing import Process
                auto_start = False
                if self._process:
                    self._process.join()
                self.info('Launching worker process')
                self._process = Process(target=self._process_run)
                self._process.start()
                self.name = self.config.worker_name + ':cli'

            if tried < retry - 1:
                await asyncio.sleep(0.05 + (tried * 0.15))

        self.error('Failed to connect to %s', self._url or self._urlfile)
        raise self.NotRunning()

    def _proc_title(self, sockdesc):
        return '%s/%s on %s' % (
            self.config.app_name, self.config.worker_name, sockdesc)

    def _remove_files(self):
        for f in self._clean_files:
            try:
                os.remove(f)
            except (FileNotFoundError, OSError):
                pass

    async def shutdown(self):
        """
        Cleanup code run on shutdown. Subclasses can override this.
        """

    def _real_shutdown(self, exitcode=0):
        self._remove_files()
        for _server in self._servers:
            try:
                _server.close()
            except:
                pass
        self.info('Stopped %s(%s), pid=%d',
            type(self).__name__, self.name, os.getpid())
        logging.shutdown()
        os._exit(exitcode)

    def _init_logging(self):
        root_logger = logging.getLogger()
        root_logger.setLevel(self.config.worker_log_level)
        if not root_logger.hasHandlers():
            fmt = "%(asctime)s %(process)d %(levelname).1s %(message)s"
            stderr_log = logging.StreamHandler()
            stderr_log.setFormatter(logging.Formatter(fmt))
            root_logger.addHandler(stderr_log)

    def logger(self, level, fmt, *args):
        """
        Convenience function for logging.
        """
        msg = (fmt % args) if args else fmt
        logging.log(level, '[%s] %s', self.name, msg)
        return msg

    def exception(self, fmt, *args):
        """
        Convenience function for logging exceptions.
        """
        import traceback
        self.error(fmt, *args)
        self.debug('%s', traceback.format_exc())

    def error(self, fmt, *args):
        """
        Convenience function for logging errors.
        """
        return self.logger(40, fmt, *args)

    def warning(self, fmt, *args):
        """
        Convenience function for logging warnings.
        """
        return self.logger(30, fmt, *args)

    def info(self, fmt, *args):
        """
        Convenience function for logging app information.
        """
        return self.logger(20, fmt, *args)

    def debug(self, fmt, *args):
        """
        Convenience function for logging debug information.
        """
        return self.logger(10, fmt, *args)

    def trace(self, fmt, *args):
        """
        Convenience function for logging traces (very detailed debug logs).
        """
        return self.logger(1, fmt, *args)

    def sse_start_result(self, data):
        """
        Helper to generate the first packet of a server-sent-events stream.
        """
        return HttpResult(self.SSE_MIMETYPE, data)

    def fds_result(self, data):
        """
        Helper to generate a file-descriptor result.
        """
        return HttpResult(self.FDS_MIMETYPE, data)

    def _setup_service(self):
        logging.getLogger().setLevel(self.config.worker_log_level)

        # Override the convenience methods with more efficient local variants
        self._local_convenience_methods()
        self.is_client = False
        self.is_service = True

    def _process_run(self):
        asyncio.set_event_loop_policy(None)
        try:
            self._setup_service()
            if signal is not None:
                signal.signal(signal.SIGUSR2, self._log_more)
            if self.config.worker_nice and hasattr(os, 'nice'):
                os.nice(self.config.worker_nice)

            if self.config.worker_use_tcp:
                sock_desc, self._sock = self._make_server_socket()
            else:
                sock_desc = self.PEER_UNIX_DOMAIN + ':0'

            self._url = self._make_url(sock_desc)
            with open(self._urlfile, 'w', encoding='utf-8') as fd:
                fd.flush()
                os.chmod(self._urlfile, 0o600)
                fd.write(self._url)

            if setproctitle:
                setproctitle(self._proc_title(sock_desc))

            asyncio.run(self._main_httpd_loop())

        except (asyncio.exceptions.CancelledError, KeyboardInterrupt):
            pass
        except:
            self.exception('Crashed!')
        finally:
            asyncio.run(self.shutdown())
            self._real_shutdown(exitcode=1)

    async def _http11(self, reader, writer, fds=False):
        loop = asyncio.get_running_loop()
        cfg = self.config

        hend = header = headers = None
        request = bytearray()

        def _closer():
            self.debug('Timed out, closing %s' % writer)
            writer.close()

        to1 = loop.call_later(cfg.worker_http_request_timeout1, _closer)
        to2 = loop.call_later(cfg.worker_http_request_timeout2, _closer)

        cr = ord(b'\r')
        want_bytes = 8192
        while want_bytes > 0:
            new_data, fds = await self._recv_data_and_fds(reader, fds=fds)
            if not new_data:
                break
            want_bytes -= len(new_data)

            request.extend(new_data)
            if len(request) > cfg.worker_http_request_max_size:
                raise ValueError('Request too large')

            if header is None:
                try:
                    nl1 = request.index(b'\n')
                    eol = b'\r\n' if (request[nl1-1] == cr) else b'\n'
                    eom = eol + eol
                    hend = request.index(eom)
                    header = str(request[:hend], 'utf-8')
                except ValueError:
                    pass
                if header:
                    hlines = header.splitlines()
                    headers = dict(l.split(': ', 1) for l in hlines[1:])
                    clen = int(headers.get('Content-Length', 0))
                    if clen:
                        want_bytes = hend + len(eom) + clen - len(request)
                    else:
                        want_bytes = 0
                    to1.cancel()

        to2.cancel()
        if not header:
            raise ValueError('Header not found in HTTP data')

        if want_bytes:
            raise IOError(
                'HTTP data incomplete, expected %d more bytes' % want_bytes)

        return header, headers, request[hend+2:], fds

    async def _serve_http(self, reader, writer):
        req = RequestInfo(reader=reader, writer=writer)

        def _w(*data):
            # Make sure we keep writing to the original writer, even if
            # the request_obj target changes (response redirection).
            return req.write(*data, writer=writer)

        # pylint: disable=protected-access
        method = path = _version = sent = ''
        req.code = 500
        try:
            head, hdrs, body, fds = await self._http11(reader, writer, fds=True)
            method, path, _version = head.split(None, 3)[:3]

            req.method = method
            req.headers = hdrs
            req.body = body
            req.fds = fds
            try:
                if self.config.worker_url_path:
                    if path.startswith('/' + self.config.worker_url_path):
                        path = path[len(self.config.worker_url_path) + 1:]
                    else:
                        raise AttributeError()

                path = await self.validate_request_header(path, head)
                req.authed = True
            except PermissionError:
                req.authed = False
            req.path = path or '/'

            (writer, req.code, mimetype, response
                ) = await self._handle_http_request(req)

            if mimetype is None and response is None:
                sent = writer = False
            elif str(mimetype, 'utf-8') == self.FDS_MIMETYPE:
                if not req.via_unix_domain:
                    raise ValueError('Cannot send file descriptors over TCP')
                if not isinstance(response, list):
                    response = [response]

                if req.sent:
                    # Avoid repeating the HTTP header, hope this is fine
                    http_res = b''
                else:
                    http_res = self._HTTP_200_OK % b'application/json'

                fd_list = [r.fileno() for r in response]
                http_res += self.to_json([
                    self._fd_to_magic_arg(a) for a in response])

                await self._send_data_and_fds(writer, http_res, fd_list)
                req.sent += len(http_res)
                sent = http_res
            else:
                l1 = (self._HTTP_RESPONSE.get(req.code) or
                    (self._HTTP_RESPONSE_UNKNOWN % req.code))
                if req.sent:
                    # Avoid repeating the HTTP header, hope this is fine
                    sent = _w(response)
                else:
                    sent = _w(l1, (self._HTTP_MIMETYPE % mimetype), response)

        except RPCKitten.RedirectException as e:
            req.code = 302
            sent = _w(
                self._HTTP_RESPONSE[req.code],
                self._HTTP_MOVED % (e.target, e.target))
        except RPCKitten.NeedInfoException as e:
            req.code = e.http_code
            sent = _w(
                self._HTTP_RESPONSE[req.code],
                self._HTTP_JSON,
                self.to_json({
                    'error': str(e),
                    'resource': e.resource,
                    'needed_vars': e.needed_vars}))
        except PermissionError as e:
            req.code = 403
            sent = _w(
                self._HTTP_RESPONSE[req.code],
                self._HTTP_JSON,
                self.to_json({'error': str(e)}))
        except AttributeError:
            req.code = 404
            sent = _w(self._HTTP_RESPONSE[req.code], self._HTTP_NOT_FOUND)
        except (TypeError, UnicodeDecodeError) as e:
            req.code = 400
            sent = _w(
                self._HTTP_RESPONSE[req.code],
                self._HTTP_JSON,
                self.to_json({'error': str(e)}))
            self.exception('Error serving %s: %s', method, e)
        except Exception as e:
            import traceback
            req.code = 500
            sent = _w(
                self._HTTP_RESPONSE[req.code],
                self._HTTP_JSON,
                self.to_json({
                    'error': str(e),
                    'traceback': traceback.format_exc()}))
            self.exception('Error serving %s: %s', method, e)
        finally:
            try:
                if writer:
                    await writer.drain()
                    writer.close()
            except:
                pass
            if sent is not False:
                self._log_http_request(req, sent=sent)

    def _log_http_request(self, req, sent=None):
        if sent is False:
            elapsed_us = None
        else:
            elapsed_us = int(1e6 * (time.time() - req.t0))

        peer = ':'.join(str(i) for i in req.peer[:2])

        log_func = self.debug if (200 <= req.code < 300) else self.warning
        log_func('HTTP %s %s %d %s %s %s',
            req.method, req.path, req.code,
            sent or '-',
            elapsed_us or '-',
            peer)

        # FIXME: We shouldn't need to hard-code this?
        if hasattr(self, 'metrics_http_request'):
            self.metrics_http_request(req,
                sent=(sent or False),
                elapsed_us=elapsed_us)

    async def validate_request_header(self, path, header):
        """
        This checks a request header for authentication. Subclasses can override this
        to implement their own access control policies.

        Raises a PermissionError if access is denied, otherwise it returns the URL
        path (with any access tokens removed).
        """
        if self._secret not in header:
            raise PermissionError()
        if (path + '/').startswith('/' + self._secret + '/'):
            return path[len(self._secret) + 1:]
        return path

    def get_api_methods(self, request_obj):
        """
        Fetch the API methods matching the request. This implements our
        default mapping of `/foo` HTTP paths to `public_api_foo` or `api_foo`.
        Override with care!
        """
        method_name = self.get_method_name(request_obj)
        if request_obj.authed:
            raw_method = getattr(self, 'raw_' + method_name, None)
            api_method = getattr(self, 'api_' + method_name, None)
        else:
            raw_method = api_method = None

        if not raw_method and not api_method:
            raw_method = getattr(self, 'public_raw_' + method_name, None)
            api_method = getattr(self, 'public_api_' + method_name, None)

        if not raw_method and not api_method:
            # Note: This raises by default, but subclasses may override
            #       and give us something to work with.
            #
            # pylint: disable=assignment-from-no-return
            # pylint: disable=unpacking-non-sequence
            api_method, raw_method = self.get_default_methods(request_obj)

        return api_method, raw_method, [], {}

    def get_method_name(self, request_obj):
        """
        This function derives the basic method name (e.g. `ping`), from
        `request_obj.path`. It returns the first component of the path,
        or the name `web_root` if the path is empty (`/`).

        Subclasses can override this to implement their own routing logic.
        """
        return request_obj.path[1:].split('/', 1)[0] or 'web_root'

    def get_default_methods(self, request_obj):
        """
        If the default function lookup mechanism fails, this function is
        called as a last-resort effort to route the request. By default
        it raises an exception to trigger a 404 or 403 error.

        Subclasses can override this with a function that returns a tuple
        of (api_method, raw_method), one of which should be `None` and the
        other an async API method function.
        """
        exc = (AttributeError if request_obj.authed else PermissionError)
        raise exc(request_obj.path)

    def guarantee_type(self, rule, v):
        """
        Guarantee value `v` has the class `rule`. This method is used
        to translate type annotations on API functions, into argument
        type validation and/or conversion.

        In particular, when API methods are invoked from the shell using
        the built-in CLI, they all arrive as strings and this function
        should be able to convert reasonably formatted strings into the
        appropriate data-type.

        Input which is invalid or cannot be converted should raise an
        exception.

        Subclasses can override this to implement their own rules.
        """
        if rule in (None, 'any'):
            return v
        if rule in (bool, 'bool'):
            return self.Bool(v)
        if rule in (int, 'int'):
            if isinstance(v, bytes):
                v = str(v, 'utf-8')
            if isinstance(v, str):
                if v[:2] == '0x':
                    return int(v[2:], 16)
                if v[:2] == '0o':
                    return int(v[2:], 8)
                if v[:2] == '0b':
                    return int(v[2:], 2)
            return int(v)
        if rule in (str, 'str'):
            if isinstance(v, str):
                return v
            return str(v, 'utf-8')
        if rule in (bytes, 'bytes') and isinstance(v, str):
            return bytes(v, 'utf-8')
        return rule(v)

    def _apply_annotations(self, func, args, kwargs):
        annotations = func.__annotations__
        if args:
            for i, k in enumerate(inspect.getfullargspec(func).args):
                if k in annotations:
                    annotations[i-2] = annotations[k]
            for i, a in enumerate(args):
                args[i] = self.guarantee_type(annotations.get(i), a)

        if kwargs:
            for k, v in kwargs.items():
                kwargs[k] = self.guarantee_type(annotations.get(k), v)

    async def _handle_http_request(self, request_obj):
        def _b(v):
            return v if isinstance(v, bytes) else bytes(v, 'utf-8')

        writer = request_obj.writer
        api_method, raw_method, args, kwargs = self.get_api_methods(request_obj)

        if raw_method:
            request_obj.handler = raw_method.__name__
            has_annotations = raw_method if raw_method.__annotations__ else None
        else:
            request_obj.handler = api_method.__name__
            has_annotations = api_method if api_method.__annotations__ else None

        mt = request_obj.mimetype = 'application/json'
        enc = request_obj.encoder = self.to_json
        if request_obj.method == 'POST':
            ctype = request_obj.headers.get('Content-Type', None)
            if request_obj.body[:1] == b'{' and not ctype:
                ctype = 'application/json'

            if ctype == 'application/x-msgpack':
                if not (self.config.worker_use_msgpack and msgpack):
                    raise ValueError('msgpack is unavailable')
                request_obj.mimetype = mt = ctype
                request_obj.encoder = enc = self.to_msgpack
                body = request_obj.body = self.from_msgpack(request_obj.body)
                args = body.pop('_args', [])
                kwargs = body
            elif ctype == 'application/json':
                body = request_obj.body = self.from_json(request_obj.body)
                args = body.pop('_args', [])
                kwargs = body
            elif ctype == 'application/x-www-form-urlencoded':
                body = str(request_obj.body, 'utf-8').strip()
                kwargs = urllib.parse.parse_qs(body)
                for k, v in kwargs.items():
                    if len(v) == 1:
                        kwargs[k] = v[0]
                args = kwargs.pop('_args', [])
            else:
                self.warning('Unhandled POST MIME type: %s' % ctype)

        if request_obj.fds:
            args = [self._fd_from_magic_arg(a, request_obj.fds) for a in args]
            reply_to_fd1 = request_obj.body.pop(self.REPLY_TO_FIRST_FD, None)
            if reply_to_fd1 is not None:
                fd = args.pop(0)
                if isinstance(fd, socket.socket):
                    _, writer = await asyncio.open_connection(sock=fd)
                else:
                    writer = FileWriter(fd)
                request_obj.write(self._HTTP_202_REPLIED_TO_FIRST_FD)
                await request_obj.writer.drain()
                request_obj.writer.close()
                request_obj.writer = writer
                request_obj.sent = int(reply_to_fd1)

        if has_annotations:
            self._apply_annotations(has_annotations, args, kwargs)

        if inspect.isasyncgenfunction(api_method):
            raw_method = self._wrap_async_generator(api_method)
            request_obj.is_generator = True

        if raw_method is not None:
            _wrapped = self._wrap_drain_and_close(raw_method)
            create_background_task(_wrapped(request_obj, *args, **kwargs))
            return None, 200, None, None

        try:
            resp = await api_method(request_obj, *args, **kwargs)
            if isinstance(resp, HttpResult):
                code = resp.http_code
                if (code == 302) and resp.redirect_to:
                    raise RPCKitten.RedirectException(resp.redirect_to)
                mimetype = resp.mimetype
                resp = resp.data
            else:
                mimetype = request_obj.mimetype
                resp = request_obj.encoder(resp)
                code = 200

            return writer, code, _b(mimetype), resp
        except RPCKitten.RedirectException:
            raise
        except RPCKitten.NeedInfoException:
            raise
        except PermissionError:
            raise
        except Exception as e:
            import traceback
            if request_obj.authed:
                return writer, 500, _b(mt), enc({
                    'error': str(e),
                    'traceback': traceback.format_exc()})
            return writer, 500, _b(mt), enc({'error': str(e)})

    def expose_methods(self, obj):
        """
        Add API methods defined in `obj` to our exposed API.

        This will also set `obj.config = self.config` so API methods
        defined this way can still see the kitten configuration. The
        naming convention for exposed methods is the same as for methods
        written directly on the kitten class (`api_` etc).
        """
        if not hasattr(obj, 'config'):
            obj.config = self.config
        for cmd in self._all_commands(obj).values():
            fname = cmd['api_method'].__name__
            if hasattr(self, fname):
                raise KeyError('Conflict, method already exists: %s' % fname)
            setattr(self, fname, cmd['api_method'])

    async def init_servers(self, servers):
        """
        Subclasses can override this with initialization logic.

        This function should return the (potentially modified) list of
        server instances. It only runs in the service process.

        Note: that one of the things that can happen here, is to add
              `api_*` methods to the class, directly or by using the
        `.expose_methods()` helper.  These new methods will be exposed
        for remote invocation and advertised during .connect()/.ping().
        This allows for lazy-loading bulky code, keeping the client
        slim and fast.
        """
        return servers

    async def _start_servers(self):
        return [await asyncio.start_server(self._serve_http,
            sock=self._sock)]

    async def _start_unix_server(self):
        return await asyncio.start_unix_server(self._serve_http,
            path=self._unixfile)

    async def _main_httpd_loop(self):
        self.start_time = int(time.time())
        if self.config.worker_use_tcp:
            self._servers.extend(await self._start_servers())
        if self.config.worker_use_unixdomain:
            self._servers.append(await self._start_unix_server())
        self._servers = await self.init_servers(self._servers)
        return await asyncio.gather(*[s.serve_forever() for s in self._servers])

    def _make_server_socket(self, listen_host=None, listen_port=None):
        _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _sock.bind((
            listen_host or self.config.worker_listen_host,
            listen_port or self.config.worker_listen_port))
        _sock.settimeout(self.config.worker_accept_timeout)
        _sock.listen(self.config.worker_listen_queue)
        _sock_desc = '%s:%d' % _sock.getsockname()[:2]
        return _sock_desc, _sock

    def _make_url(self, host_port):
        self._secret = self.config.worker_secret
        if not self._secret:
            from base64 import b64encode
            secret = str(b64encode(os.urandom(18), b'-_').strip(), 'utf-8')
            self._secret = secret
        if self.config.worker_url_path:
            path = '%s/' % self.config.worker_url_path.rstrip('/')
        else:
            path = ''
        return 'http://%s/%s%s' % (host_port, path, self._secret)

    def _log_more(self, *ignored):
        pass  # FIXME

    def to_server_sent_event(self, event):
        """
        Serializes the data to a Server Sent Event, returning the event as bytes.

        Override this if you need de/serialization for app-specific data.
        """
        chunk = []
        for key in ('id', 'event', 'retry'):
            val = event.pop(key, None)
            if val is not None:
                chunk.append(bytes('%s: %s' % (key, val), 'utf-8'))

        data = event.pop('data', event)
        if data:
            if isinstance(data, str):
                data = bytes(data, 'utf-8')
            if not isinstance(data, bytes):
                data = self.to_json(data)
            for line in data.splitlines():
                chunk.append(b'data: %s' % line)
            if data.endswith(b'\n'):
                chunk.append(b'data: ')

        chunk.append(b'\n')
        return b'\n'.join(chunk)

    def from_server_sent_event(self, ed):
        """
        Deserialize Server Sent Events, returning the event data as a dict.

        Override this if you need de/serialization for app-specific data.
        """
        event = {}
        for l in str(ed, 'utf-8').rstrip().splitlines():
            k, v = l.split(':', 1)
            if k in event:
                event[k] += '\n' + v[1:]
            else:
                event[k] = v[1:]
        return event

    def to_json(self, data, friendly=False):
        """
        Serializes the data to JSON, returning the generated JSON as bytes.

        Override this if you need de/serialization for app-specific data.
        """
        def _enc(obj):
            from base64 import b64encode
            if isinstance(obj, (bytes, bytearray)):
                if friendly:
                    try:
                        return {'__bytes__': str(obj, 'utf-8')}
                    except UnicodeDecodeError:
                        pass
                return {'__base64__': str(b64encode(obj), 'latin-1')}
            raise TypeError('Unhandled data type: %s' % (type(obj).__name__,))
        return bytes(json.dumps(data, default=_enc) + '\n', 'utf-8')

    def from_json(self, jd):
        """
        Deserializes data from JSON.

        Override this if you need de/serialization for app-specific data.
        """
        def _dec(obj):
            from base64 import b64decode
            if len(obj) == 1:
                # pylint: disable=unnecessary-lambda
                for key, decode in (
                        ('__base64__', lambda d: b64decode(d)),
                        ('__bytes__',  lambda d: bytes(d, 'utf-8'))):
                    data = obj.get(key)
                    if data:
                        return decode(data)
            return obj
        return json.loads(
            jd if isinstance(jd, str) else str(jd, 'utf-8'),
            object_hook=_dec)

    def to_msgpack(self, data, default=None):
        """
        Serializes the data to msgpack, returning the packed data as bytes.

        This augments the standard msgpack with (crude) support for arbitrary
        sized BigInts - if msgpack cannot handle the int, it will be encoded
        as a hexadecimal string using ExtType(1).

        Override this if you need de/serialization for app-specific data.
        """
        def _to_exttype(obj):
            if isinstance(obj, int):
                return msgpack.ExtType(1, b'%x' % obj)
            if default is not None:
                return default(obj)
            raise TypeError('Unhandled data type: %s' % (type(obj).__name__,))
        try:
            return msgpack.packb(data, default=_to_exttype)
        except Exception as exc:
            raise ValueError('to_msgpack failed: %s' % (exc,)) from exc

    def from_msgpack(self, d, ext_hook=None):
        """
        Deserializes data from msgpack.

        See to_msgpack() for details on BigInt support.

        Override this if you need de/serialization for app-specific data.
        """
        def _from_exttype(code, data):
            if code == 1:
                return int(data, 16)
            if ext_hook is not None:
                return ext_hook(code, data)
            return msgpack.ExtType(code, data)

        if not d:
            return None
        d = d if isinstance(d, (bytes, bytearray)) else bytes(d, 'latin-1')
        return msgpack.unpackb(d, ext_hook=_from_exttype)

    async def _url_connect(self, url, allow_unix=True):
        # pylint: disable=protected-access
        _proto, _, host_port, path = url.split('/', 3)

        allow_unix &= bool(self._peeraddr or not self.config.worker_use_tcp)
        if self._unixfile and allow_unix:
            try:
                self._peeraddr = self._unixfile
                sock = await asyncio.open_unix_connection(self._unixfile)
                return ('/' + path), True, sock
            except (FileNotFoundError, ConnectionRefusedError):
                pass
            except:
                self.exception('Unix socket connection failed')

        host, port = host_port.rsplit(':', 1)
        rd_wr = await asyncio.open_connection(host, int(port), limit=8192)
        self._peeraddr = rd_wr[0]._transport._sock.getpeername()
        return ('/' + path), False, rd_wr

    async def _send_data_and_fds(self, writer, data, fds):
        # pylint: disable=protected-access
        try:
            sock = writer._transport._sock
            sock.setblocking(1) # Won't block for long...
            if fds:
                fds = array.array('i', fds)
                extras = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)]
            else:
                extras = []
            return sock.sendmsg([data], extras)
        finally:
            sock.setblocking(0)
        return 0

    async def _recv_data_and_fds(self, reader, bufsize=64*1024, fds=False):
        # pylint: disable=protected-access
        sock = getattr(reader._transport, '_sock', None)
        if fds and sock and (sock.family == socket.AF_UNIX):
            try:
                reader._transport.pause_reading()

                data, ancdata, _, _ = await await_success(
                    sock.recvmsg, bufsize, bufsize // 8,
                    sleeptime=0.005,  # Initial sleep, give other end time
                    sleepstep=0.01,   # Slow down by this much on each try
                    sleepmax=0.25,    # Max polling interval
                    timeout=120,
                    retried_exceptions=(BlockingIOError,))

                fds = array.array('i')
                for cmsg_lvl, cmsg_typ, cmsg_data in ancdata:
                    if (cmsg_lvl == socket.SOL_SOCKET
                            and cmsg_typ == socket.SCM_RIGHTS):
                        fds.frombytes(cmsg_data)

                return data, list(fds)
            except:
                self.exception(
                    '%s.recvmsg(%d) with FDs failed', sock, bufsize)
            finally:
                reader._transport.resume_reading()
            return None, []

        return (await reader.read(bufsize)), []

    def _fd_to_magic_arg(self, a):
        if hasattr(a, 'fileno'):
            if isinstance(a, socket.socket):
                return '%s-%d-%d-%d' % (
                    self._MAGIC_SOCK, a.family, a.type, a.proto)
            return '%s-%s' % (self._MAGIC_FD, a.mode)
        return a

    def _fd_from_magic_arg(self, a, fds):
        if isinstance(a, str):
            if a.startswith(self._MAGIC_SOCK):
                family, typ, proto = (int(i) for i in a.split('-')[1:])
                try:
                    fd = os.fdopen(fds.pop(0))
                    return socket.fromfd(fd.fileno(), family, typ, proto)
                finally:
                    fd.close()
            if a.startswith(self._MAGIC_FD):
                _, mode = a.split('-')
                return os.fdopen(fds.pop(0), mode=mode)
        return a

    def _get_exception(self, resp_code, result):
        logging.debug('_get_exception(%s, %s)', resp_code, result)
        exc = RuntimeError
        try:
            resp_code = int(resp_code)
        except:
            pass

        needed_vars = result.get('needed_vars')
        if needed_vars or (resp_code == self.NeedInfoException.http_code):
            return self.NeedInfoException(
                result['error'], needed_vars, result.get('resource'))

        if resp_code in (401, 403, 407):
            exc = PermissionError
        elif resp_code in (404, ):
            exc = KeyError
        elif 300 <= resp_code < 400:
            exc = IOError
        elif 400 <= resp_code < 500:
            exc = ValueError
        return exc('HTTP %d: %s' % (resp_code, result.get('error')))

    async def call(self, fn, *args, **kwargs):
        """
        Invoke function `fn` on the microservice, sending over the args
        and keyword arguments using `msgpack` and HTTP POST.

        The response will either be returned directly as deserialized
        Python objects or, if the response is incremental, the function
        will return a generator function yielding deserialized objects.

        This function is used internally by the autogenerated convenience
        functions (`RPCKitten.help()` and `RPCKitten.quitquitquit()`).
        """
        t0 = time.time()

        if not self._url:
            raise self.NotRunning('Please .connect() first')

        use_json = (self.Bool(kwargs.pop(self.CALL_USE_JSON, False))
            or (msgpack is None)
            or (not self.config.worker_use_msgpack))

        reply_to = kwargs.pop(self.CALL_REPLY_TO, None)
        if reply_to:
            sent_bytes = 0
            if isinstance(reply_to, RequestInfo):
                if (not use_json) and ('/json' in reply_to.mimetype):
                    use_json = True
                sent_bytes = reply_to.sent
                reply_to = reply_to.socket
            args = [reply_to] + list(args)
            kwargs[self.REPLY_TO_FIRST_FD] = sent_bytes

        allow_unix = self.Bool(kwargs.pop(self.CALL_ALLOW_UNIX, True))
        retries = int(kwargs.pop(self.CALL_MAX_TRIES, 2))
        for attempt in range(0, retries + 1):
            try:
                url = self._url +'/'+ fn
                (url_path, fds_ok, io_pair
                    ) = await self._url_connect(url, allow_unix)
                break
            except:
                if attempt >= retries:
                    raise
                await self.connect(auto_start=(retries > 0), retry=1)

        reader, writer = io_pair

        fds = [a.fileno() for a in args if hasattr(a, 'fileno')]
        kwargs['_args'] = [self._fd_to_magic_arg(a) for a in args]

        if use_json:
            mimetype = 'application/json'
            payload = self.to_json(kwargs or {})
        else:
            mimetype = 'application/x-msgpack'
            payload = self.to_msgpack(kwargs or {})

        http_req = bytes("""\
POST %s HTTP/1.1
Connection: close
Content-Type: %s
Content-Length: %d

""" % (url_path, mimetype, len(payload)), 'utf-8') + payload

        try:
            chunked, resp_code, body, result = False, 500, b'', {}

            if fds:
                if not fds_ok:
                    raise ValueError('Cannot send file descriptors over TCP')
                await self._send_data_and_fds(writer, http_req, fds)
            else:
                writer.write(http_req)
                await writer.drain()

            try:
                head, hdrs, body, rfds = await self._http11(reader, writer,
                    fds=fds_ok)

                resp_code = int(head.split(None, 2)[1])
                chunked = (hdrs.get('Transfer-Encoding') == 'chunked')
                if body and not chunked:
                    ctype = hdrs['Content-Type']
                    if ctype == 'application/x-msgpack':
                        result = self.from_msgpack(body)
                    elif ctype == 'application/json':
                        result = self.from_json(body)
                    elif ctype == self.SSE_MIMETYPE:
                        result = self.from_server_sent_event(body)
                    else:
                        result = HttpResult(ctype, body)
                if rfds:
                    result = [
                        self._fd_from_magic_arg(a, rfds)
                        for a in result]
            except IOError as e:
                result = {'error': 'Read failed: %s' % e}
            except Exception as e:
                if not body:
                    result = {'error': 'Empty response body'}
                else:
                    import traceback
                    result = {
                        'error': 'Failed to unpack %s: %s' % (body, e),
                        'traceback': traceback.format_exc()}

            if 200 <= resp_code < 300:
                if (not isinstance(result, dict)) or ('error' not in result):
                    if chunked:
                        return self._chunk_decoder(reader, writer, hdrs, body, rfds)
                    return result
                resp_code = 500

            exc = self._get_exception(resp_code, result)
            if 'traceback' in result:
                self.error('Remote %s', result['traceback'])
            raise exc
        finally:
            rtime = 1000 * (time.time() - t0)
            (self.debug if (200 <= resp_code < 300) else self.error)(
                'CALL %s(%s)%s %s %.2fms',
                fn, str_args(args), fds, resp_code, rtime)

    async def ping(self, allow_unix=True, docs=False):
        """
        Check whether the service is running.
        """
        if self.is_client:
            self._peeraddr = None
            return await self.call('ping',
                docs=docs,
                call_max_tries=0,
                call_allow_unix=allow_unix)
        return {'pong': True, 'loopback': True}

    async def quitquitquit(self):
        """
        Shut down the service.
        """
        if self.is_client:
            return await self.call('quitquitquit', call_max_tries=0)
        await self.shutdown()
        return True

    def _http11_chunk(self, buffer):
        try:
            ln, data = buffer.split(b'\r\n', 1)
            ln = int(ln, 16)
            if len(data) >= ln+2:
                return data[:ln], data[ln+2:]
        except (ValueError, IndexError):
            pass
        return None, buffer

    def _chunk_decoder(self, reader, writer, hdrs, buffer, rfds):
        # pylint: disable=unnecessary-lambda-assignment

        ctype = hdrs['Content-Type']
        if ctype == 'application/x-msgpack':
            decode = self.from_msgpack
        elif ctype == 'application/json':
            decode = self.from_json
        elif ctype == self.SSE_MIMETYPE:
            decode = self.from_server_sent_event
        else:
            decode = lambda v: v

        async def decoded_chunk_generator():
            nonlocal reader, writer, buffer
            if rfds:
                yield {'received_fds': rfds}

            finished = False
            while True:
                chunk, buffer = self._http11_chunk(buffer)
                if chunk is None:
                    if not reader:
                        # Note: This use of the writer mainly keeps it in
                        # scope.  As it owns the socket, if it gets GC'ed
                        # then the connection closes prematurely.
                        writer.close()
                        break
                    data = await reader.read(8192)
                    if data:
                        buffer += data
                    else:
                        reader = None
                elif chunk == b'':
                    finished = True
                else:
                    yield decode(chunk)
                    finished = False

            if not finished:
                # FIXME: This needs documenting!
                raise IOError('Incomplete result, missing end-of-stream marker')

        return decoded_chunk_generator

    def _wrap_drain_and_close(self, raw_method):
        async def draining_raw_method(request_obj, *args, **kwargs):
            try:
                if inspect.isasyncgenfunction(raw_method):
                    async for _r in raw_method(request_obj, *args, **kwargs):
                        pass
                else:
                    await raw_method(request_obj, *args, **kwargs)
            except Exception as e:
                import traceback
                err = {'error': str(e)}
                if request_obj.authed:
                    err['traceback'] = traceback.format_exc()
                mt = bytes(request_obj.mimetype, 'utf-8')
                request_obj.write(
                    self._HTTP_RESPONSE[500],
                    self._HTTP_MIMETYPE % mt,
                    request_obj.encoder(err))
            finally:
                try:
                    await request_obj.writer.drain()
                except:
                    pass
                self._log_http_request(request_obj, sent=request_obj.sent)

                # As we may be passing the underlying file descriptor to
                # another worker, try not to be too hasty about closing.
                await asyncio.sleep(0.01)
                asyncio.get_running_loop().call_soon(request_obj.writer.close)
        return draining_raw_method

    def _send_chunked(self, request_obj, data):
        request_obj.write(
            self._HTTP_CHUNK_BEG % len(data),
            data,
            self._HTTP_CHUNK_END)

    def _wrap_async_generator(self, api_method):
        # Use chunked encoding or text/event-stream to send multiple results
        class RemoteError(Exception):
            """Exception raised in service process."""
            http_code = 500
        async def raw_method(request_obj, *args, **kwargs):
            # pylint: disable=unnecessary-lambda-assignment
            enc = request_obj.encoder
            resp_mimetype = request_obj.mimetype
            events = False
            first = True
            try:
                async for r in api_method(request_obj, *args, **kwargs):
                    if isinstance(r, HttpResult):
                        if not first:
                            raise RuntimeError(
                                'HttpResult must be first emitted result')
                        mimetype, resp = r.mimetype, r.data
                    else:
                        mimetype, resp = None, r

                    if resp is None and mimetype is None:
                        return

                    if mimetype and first:
                        resp_mimetype = mimetype
                        if mimetype == self.SSE_MIMETYPE:
                            events = enc = self.to_server_sent_event
                        else:
                            enc = lambda d: d  # Should already be encoded!

                    if first:
                        if not mimetype and isinstance(resp, dict):
                            if 'finished' in resp and 'error' in resp:
                                if 'needed_vars' in resp:
                                    re = self.NeedInfoException(resp['error'])
                                    re.needed_vars = resp['needed_vars']
                                    re.resource = resp['resource']
                                else:
                                    re = RemoteError(resp['error'])
                                re.http_code = resp['finished']
                                raise re

                        if not request_obj.sent:
                            mt = bytes(resp_mimetype, 'utf-8')
                            request_obj.write(self._HTTP_200_CHUNKED_OK % mt)

                    self._send_chunked(request_obj, enc(resp))
                    await request_obj.writer.drain()
                    first = False

                # Send an empty chunk, to delimit a completed stream
                # FIXME: This needs documenting!
                self._send_chunked(request_obj, b'')

            except (IOError, BrokenPipeError) as exc:
                self.debug('Broken pipe: %s' % exc)
            except Exception as e:
                data = {'error': str(e)}

                if hasattr(e, 'needed_vars'):
                    # NeedInfoException
                    data['needed_vars'] = e.needed_vars
                    data['resource'] = e.resource
                elif request_obj.authed:
                    import traceback
                    data['traceback'] = traceback.format_exc()

                if events:
                    data = {'event': 'error', 'data': data}

                if first:
                    try:
                        http_resp = self._HTTP_RESPONSE[int(e.http_code)]
                    except (AttributeError, ValueError, KeyError):
                        http_resp = self._HTTP_RESPONSE[500]

                    mt = bytes(resp_mimetype, 'utf-8')
                    request_obj.write(
                        http_resp,
                        self._HTTP_MIMETYPE % mt,
                        enc(data))
                else:
                    self._send_chunked(request_obj, enc(data))
                    # Deliberatly not sending the completed marker, we exploded

            finally:
                pass  #print('E> Goodbye loopy')

        return raw_method

    async def public_raw_ws(self, request_obj):
        """/ws

        Create a persistent websocket connection.
        """
        # Check headers; if they match
        # Send back websocket Upgrade: headers etc
        # ...
        raise RuntimeError('FIXME: Not implemented')

    async def public_raw_ping(self, request_obj, **kwa):
        """/ping [--docs=y]

        Check whether the microservice is running (public) and which
        services it currently offers (requires authentication). Send
        `--docs=y` to include method docstrings.
        """
        if request_obj.authed:
            mt = request_obj.mimetype
            enc = request_obj.encoder
            body = request_obj.body
            if not isinstance(body, dict):
                body = {}

            all_commands = self._all_commands()
            for _k, i in all_commands.items():
                if self.Bool(kwa.get('docs')):
                    try:
                        i['help'] = i['api_method'].__doc__.strip()
                    except AttributeError:
                        pass
                del i['api_method']

            body['pong'] = True
            body['conn'] = ':'.join(str(v) for v in request_obj.peer)
            body['methods'] = all_commands
            body['_format'] = (
                'Pong via %(conn)s! (see JSON for full method list)')

            request_obj.write(
                self._HTTP_200_OK % bytes(mt, 'utf-8'),
                enc(body))
        else:
            # Not authed: do less work and don't let caller influence output
            request_obj.write(self._HTTP_200_STATIC_PONG)

    async def api_config(self, _request_obj,
            reset=None,
            key=None,
            val=None,
            append=None,
            remove=None,
            pop=None,
            save=None):
        """/config [<options ...>]

        Returns the current configuration, optionally after editing it.

        Options:
            --reset=<K>   Reset K to default values
            --key=<K>     Key to manipulate (requries one of the below)
            --val=<V>     Set K=V
            --append=<V>  Append string V to K (must be a list)
            --remove=<V>  Remove string V from K (must be a list)
            --pop=<N>     Pop item at offset N from K (must be a list)
            --save=<P>    Save the configuration to path P. If P is one of
                          the recognized boolean strings (t, true, y, yes),
                          save to the default location.
        """
        results = {}

        async def call_config_hook(key):
            hookname = 'on_config_%s' % key
            if hasattr(self, hookname):
                return await getattr(self, hookname)()
            return key

        if reset:
            default = getattr(self.config, reset.upper())
            setattr(self.config, reset.lower(), default)
            results['reset'] = await call_config_hook(reset)

        if 1 < sum((1 if a else 0) for a in (val, append, remove, pop)):
            raise ValueError('Please only perform one operation at a time!')

        if key:
            default = getattr(self.config, key.upper())  # Raises if key is bogus
            if val:
                self.config.configure(
                    args=['--%s=%s' % (key, val)],
                    set_defaults=False)
                results['val'] = await call_config_hook(key)
            elif append:
                getattr(self.config, key.lower()).append(append)
                results['append'] = await call_config_hook(key)
            elif remove:
                getattr(self.config, key.lower()).remove(remove)
                results['remove'] = await call_config_hook(key)
            elif pop:
                getattr(self.config, key.lower()).pop(int(pop))
                results['pop'] = await call_config_hook(key)

        if save and await call_config_hook(save):
            if self.Bool(save):
                filepath = self.config.default_config_file()
            else:
                filepath = save
            with open(filepath, 'w', encoding='utf-8') as fd:
                fd.write(str(self.config))

        results.update({
            'config': self.config.as_dict(),
            '_format': str(self.config).replace('%', '%%')})

        return results

    async def api_quitquitquit(self, _request_obj):
        """/quitquitquit

        Shut down the microservice.
        """
        try:
            await self.shutdown()
        except:
            pass
        asyncio.get_running_loop().call_soon(self._real_shutdown)
        return "Goodbye forever"

    def get_docstring(self, method):
        """
        Fetch the docstring for a given method.

        Submodules can override this if they want to provide custom help.
        """
        return getattr(method, '__doc__', None)

    async def api_help(self, _request_obj, command=None):
        """/help [command]

        Returns docstring-based help for existing API methods, or an
        introduction to the CLI interface if no command is specified.
        """
        def doc(obj):
            docstring = (
                    self.get_docstring(obj) or 'No Help Available'
                ).strip()
            return docstring.replace('\n    ', '\n') + '\n'

        if not command:
            main_doc = doc(self.Main).replace('rpckitten', self.config.app_name)

            cmd_list = ['API Commands: ']
            first = True
            for cmd in sorted(list(self._all_commands().keys())):
                if cmd in ('ping', 'help', 'config', 'quitquitquit', 'ws'):
                    continue
                if len(cmd_list[-1]) + len(cmd) > 75:
                    cmd_list.append('   ')
                elif not first:
                    cmd_list[-1] += ', '
                cmd_list[-1] += cmd
                first = False
            main_doc = main_doc.replace('__API_COMMANDS__', '\n'.join(cmd_list))

            return doc(self) +'\n'+ main_doc

        api_command = getattr(self, 'api_' + command, None)
        if not api_command:
            api_command = getattr(self, 'public_api_' + command, None)
        if api_command:
            return doc(api_command)

        raw_command = getattr(self, 'raw_' + command, None)
        if not raw_command:
            raw_command = getattr(self, 'public_raw_' + command, None)
        if raw_command:
            return doc(raw_command)

        return doc(None)

    @classmethod
    def Bool(cls, value):  # pylint: disable=invalid-name
        """
        This is a convenience method for API functions to convert an
        incoming argument to boolean. It recognizes the strings
        "1", "true", "t", "yes" and "y" as True values. Anything else
        is considered to be False.
        """
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in cls.TRUE_STRINGS
        return bool(value)

    @classmethod
    def extract_kwargs(cls, args, allowed=None):
        """
        This is a helper function which will convert --key=val arguments
        found in the `args` list, into a dictionary of keys and values.
        Only arguments in the allowed set (or list or tuple) will be
        converted.

        Returns a tuple of the remaining args, and the new keyword dict.

        Example:

            args = [1, 2, '--three=four', 4]
            args, kwargs = RPCKitten.extract_kwargs(args, ['three'])

            # args == [1, 2, 4]
            # kwargs == {'three': 'four'}

        """
        def _is_arg(a):
            return isinstance(a, str) and a[:2] == '--'

        def _split(a):
            k, v = a[2:].split('=', 1)
            return k.replace('-', '_'), v

        kwargs = dict(_split(a) for a in args if _is_arg(a))
        for k in kwargs:
            if allowed and k not in allowed:
                raise ValueError('Unrecognized option: --%s' % k)

        return [a for a in args if not _is_arg(a)], kwargs

    def print_result(self, result, print_raw=False, print_json=False):
        """Print results as text."""
        if print_raw:
            return print('%s' % result)

        if isinstance(result, HttpResult):
            result = result['data']

        if print_json:
            if isinstance(result, dict) and '_format' in result:
                del result['_format']
            return print(str(self.to_json(result, friendly=True), 'utf-8'))

        if isinstance(result, (bytearray, bytes)):
            sys.stdout.buffer.write(result)
            sys.stdout.buffer.flush()
            return None

        if isinstance(result, dict):
            if '_format' in result:
                return print(result.pop('_format') % result)
            try:
                return print(str(self.to_json(result, friendly=True), 'utf-8'))
            except:
                pass

        return print('%s' % result)

    @classmethod
    def _cli_collect_needed_info(cls, message, needed_vars, kwargs):
        import getpass
        print(message)
        for nv in needed_vars:
            vname = nv['name']
            vtype = (nv.get('type') or 'info').lower()
            if vtype[:4] == 'pass':
                kwargs[vname] = getpass.getpass((nv.get('comment') or 'Password') + ': ')
            elif vtype[:4] == 'user':
                kwargs[vname] = input((nv.get('comment') or 'Username') + ': ')
            else:
                kwargs[vname] = input((nv.get('comment') or vname) + ': ')

    @classmethod
    def Main(cls, args=False):  # pylint: disable=invalid-name
        """Usage: rpckitten [--json|--raw|--tcp] <command> [<args ...>]

    Commands:

        config          - Display the current configuration
        help <command>  - Get help about a rpckitten commands
        ping            - Check whether rpckitten is running
        start           - Start the background service
        stop            - Stop the background service
        restart         - Stop and Start!

    __API_COMMANDS__

    You can also treat any API method as a command and invoke it from
    the command line. Add --json/-j or --raw/-r to alter whether/how the
    output gets formatted. Pass --tcp/-t to avoid the Unix domain socket,
    and --json-rpc/-J to use JSON instead of msgpack serialization.

    Examples:
        rpckitten ping --json
        rpckitten help ping
        """
        if args is False:
            args = sys.argv[1:]

        if not args:
            print(cls.__doc__.strip().replace('\n    ', '\n'))
            print()
            print('Try this: %s help' % sys.argv[0])
            sys.exit(1)

        async def async_main(config, args):
            def _extract_bool_arg(args, *matches):
                for a in args:
                    if not a.startswith('-'):
                        break
                    if a in matches:
                        args.remove(a)
                        return True
                return False

            print_json = _extract_bool_arg(args, '-j', '--json')
            print_raw = _extract_bool_arg(args, '-r', '--raw')
            no_unix = _extract_bool_arg(args, '-t', '--tcp')
            no_msgpack = _extract_bool_arg(args, '-J', '--json-rpc')

            # pylint: disable=protected-access
            self = cls(config=config)
            name = '%s/%s' % (config.app_name, self.name)
            command = args.pop(0)

            def _print_result(result):
                self.print_result(result, print_raw=print_raw, print_json=print_json)

            def _fail(code, e):
                sys.stderr.write('%s %s failed: %s\n' % (self.name, command, e))
                sys.exit(code)

            try:
                if args and command in ('start', 'stop', 'restart'):
                    raise ValueError('invalid arguments: %s' % ' '.join(args))

                args, kwargs = self.extract_kwargs(args,
                    allowed=self._command_kwargs(command))
                if no_unix:
                    kwargs[self.CALL_ALLOW_UNIX] = False
                if no_msgpack:
                    kwargs[self.CALL_USE_JSON] = True

                if command == 'start':
                    await self.connect(auto_start=True)
                    if self._url:
                        msg = '%s: Running at %s' % (name, self._url)
                        self.info(msg)
                        print(msg)
                    if self._unixfile and os.path.exists(self._unixfile):
                        msg = '%s: Running at %s' % (name, self._unixfile)
                        self.info(msg)
                        print(msg)
                    return os._exit(0)

                if command == 'stop':
                    try:
                        await self.connect(auto_start=False, retry=0)
                        await self.quitquitquit()
                        print('%s: Stopped' % name)
                    except cls.NotRunning:
                        print('%s: Not running' % name)
                    return

                if command == 'restart':
                    await async_main(config, ['stop'])
                    await async_main(config, ['start'])
                    return

                try:
                    await self.connect()
                except cls.NotRunning:
                    if command == 'help':
                        result = await self.api_help(None, *args, **kwargs)
                        return _print_result(result)
                    raise

                for _tries in (1, 2, 3):
                    try:
                        result = await self.call(command, *args, **kwargs)
                        if inspect.isasyncgenfunction(result):
                            async for res in result():
                                _print_result(res)
                            return
                        return _print_result(result)
                    except cls.NeedInfoException as e:
                        cls._cli_collect_needed_info(str(e), e.needed_vars, kwargs)

            except KeyboardInterrupt:
                pass

            except cls.NotRunning:
                sys.stderr.write(
                    '%s: Not running: Start it first?\n' % self.name)
                sys.exit(1)
            except PermissionError as e:  # Note: order matters here!
                _fail(5, e)
            except KeyError as e:
                _fail(2, e)
            except ValueError as e:
                _fail(3, e)
            except IOError as e:
                _fail(4, e)
            except RuntimeError as e:
                _fail(6, e)

        try:
            # FIXME: Allow options or environment variables that tweak
            #        how we instanciate the class here.
            config = cls.Configuration()
            args = config.configure(list(args), strict=False)
            task = async_main(config, args)
        except:
            import traceback
            traceback.print_exc()

        asyncio.set_event_loop_policy(None)
        asyncio.run(task)
