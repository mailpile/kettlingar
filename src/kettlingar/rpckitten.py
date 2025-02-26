import asyncio
import array
import copy
import inspect
import json
import logging
import msgpack
import os
import socket
import sys
import time
import traceback
import urllib.parse

try:
    from setproctitle import setproctitle
except ImportError:
    setproctitle = None

try:
    import signal
except ImportError:
    signal = None

from multiprocessing import Process
from base64 import b64encode

from .asynctools import create_background_task, FileWriter
from .str_utils import str_addr, str_args


def _mkdirp(path, mode):
    if not os.path.exists(path):
        _mkdirp(os.path.dirname(path), mode)
        os.mkdir(path, mode)


class RequestInfo:
    """
    Class describing a specific request.
    """
    def __init__(self,
            peer=None,
            authed=False,
            reader=None,
            writer=None,
            mimetype=None,
            encoder=None,
            method=None,
            headers=None,
            path=None,
            body=None,
            fds=None):
        self.peer = peer
        self.authed = authed
        self.reader = reader
        self.writer = writer
        self.mimetype = mimetype
        self.encoder = encoder
        self.method = method
        self.headers = headers
        self.path = path
        self.body = body
        self.fds = [] if (fds is None) else fds

    socket = property(lambda s: s.writer._transport._sock)
    fileno = property(lambda s: s.writer._transport._sock.fileno())


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
    REPLY_TO_FIRST_FD = 'reply_to_first_fd'

    CALL_USE_JSON = 'call_use_json'
    CALL_REPLY_TO = 'call_reply_to'
    CALL_MAX_TRIES = 'call_max_tries'

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
        400: b'HTTP/1.1 400 Invalid Request\n',
        403: b'HTTP/1.1 403 Access Denied\n',
        404: b'HTTP/1.1 404 Not Found\n',
        500: b'HTTP/1.1 500 Internal Error\n'}

    _HTTP_MIMETYPE = (b'Content-Type: %s\n'
                     b'Connection: close\n\n')
    _HTTP_JSON = (b'Content-Type: application/json\n'
                 b'Connection: close\n\n')
    _HTTP_NOT_FOUND = (b'Content-Type: application/json\n'
                  b'Connection: close\n\n'
                  b'{"error": "Not Found"}\n')
    _HTTP_SORRY = (b'Content-Type: application/json\n'
                  b'Connection: close\n\n'
                  b'{"error": "Sorry"}\n')

    class NotRunning(OSError):
        pass

    class Configuration:
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
        WORKER_LOG_LEVEL = 0  # 20 sets the default to info

        WORKER_HTTP_REQUEST_TIMEOUT1 = 1.0
        WORKER_HTTP_REQUEST_TIMEOUT2 = 15.0
        WORKER_HTTP_REQUEST_MAX_SIZE = 1024*1024

        _METHODS = ('as_dict', 'configure')

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
            results = {}
            for cls in self.__class__.__mro__:
                for akey in cls.__dict__:
                    if akey.upper() == akey and akey[:1] != '_':
                        akey = akey.lower()
                        results[akey] = getattr(self, akey, None)
            return results

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
            with open(path, 'r') as fd:
                for line in (_clean_line(l) for l in fd):
                    if line:
                        key, val = (p.strip() for p in line.split('='))
                        file_config.append('--%s=%s' % (key, val))

            self.configure(file_config, strict=True, set_defaults=False)

        def configure(self, args=None, strict=True, set_defaults=True):
            consumed = set()

            def _kv(i):
                k, v = i.strip().split(':')
                return k.strip, v.strip()

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
                            val = val[:1] in ('y', 'Y', 't', 'T', '1')
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
                        if key == 'worker_config':
                            self._configure_from_file(val)

            unconsumed = [a for a in args if a not in consumed]
            if unconsumed and strict:
                raise ValueError('Unrecognized arguments: %s' % unconsumed)

            if set_defaults:
                self._set_defaults()

            return unconsumed

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
        self._peeraddr = None
        self._url = None
        self._sock = None
        self._secret = None
        self._process = None

        self.is_client = True
        self.is_service = False
        self._convenience_methods = set()

    def _command_fullargspec(self, command):
        for prefix in ('public_api_', 'public_raw_', 'api_', 'raw_'):
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

    def _all_commands(self):
        found = {}
        for key in dir(self):
             for prefix in ('public_api_', 'public_raw_', 'api_', 'raw_'):
                 if key.startswith(prefix):
                     try:
                         func = getattr(self, key)
                         name = key[len(prefix):]
                         found[name] = (func, inspect.getfullargspec(func))
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

    def _local_convenience_methods(self):
        def _mk_func(fname, api_method):
            if inspect.isasyncgenfunction(api_method):
                async def _func(*args, **kwargs):
                    async for result in api_method(None, *args, **kwargs):
                        yield result[1]
            else:
                async def _func(*args, **kwargs):
                    ctype, data = await api_method(None, *args, **kwargs)
                    if ctype is None:
                        return data
                    else:
                        data = bytearray(data, 'utf-8') if isinstance(data, str) else data
                        return {'mimetype': ctype, 'data': data}
            return _func
        self._make_convenience_methods(_mk_func)

    def _remote_convenience_methods(self):
        def _mk_func(fname, api_method):
            if inspect.isasyncgenfunction(api_method):
                async def _func(*args, **kwargs):
                    _generator = await self.call(fname, *args, **kwargs)
                    if isinstance(_generator, dict):
                        # This happens whith call_reply_to
                        yield _generator
                    else:
                        async for result in _generator():
                            yield result
            else:
                async def _func(*args, **kwargs):
                    return await self.call(fname, *args, **kwargs)
            return _func
        self._make_convenience_methods(_mk_func)

    def _make_convenience_methods(self, mk_func):
        for api_fname in dir(self):
            for prefix in ('public_api_', 'public_raw_', 'api_', 'raw_'):
                if api_fname.startswith(prefix):
                    api_func = getattr(self, api_fname)
                    fname = api_fname[len(prefix):]
                    if (not hasattr(self, fname)) or fname in self._convenience_methods:
                        self._convenience_methods.add(fname)
                        setattr(self, fname, mk_func(fname, api_func))

    async def loopback(self):
        """
        Use this instead of connect() to behave as a library, handling
        everything locally (without launching a service process).
        """
        self._setup_service()
        self._servers = await self.init_servers([])

    async def connect(self, auto_start=False, retry=3):
        """
        Establish a connection with the running service, optionally
        launching the service if it isn't yet running.
        """
        self._remote_convenience_methods()
        self._init_logging()

        for tried in range(0, retry + 1):
            try:
                with open(self._urlfile, 'r') as fd:
                    self._url = fd.read().strip()

                if await self.ping():
                    return self

            except (OSError, RuntimeError) as e:
                pass

            if auto_start:
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
        for f in (self._unixfile, self._urlfile):
            try:
                os.remove(f)
            except (FileNotFoundError, OSError):
                pass

    async def shutdown(self):
        """
        Cleanup code run on shutdown. Subclasses can override this.
        """
        pass

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
        rootLogger = logging.getLogger()
        rootLogger.setLevel(self.config.worker_log_level)
        if not rootLogger.hasHandlers():
            fmt = "%(asctime)s %(process)d %(levelname).1s %(message)s"
            stderrLog = logging.StreamHandler()
            stderrLog.setFormatter(logging.Formatter(fmt))
            rootLogger.addHandler(stderrLog)

    def exception(self, fmt, *args):
        """
        Convenience function for logging exceptions.
        """
        logging.exception(
            '[%s] %s', self.name, (fmt % args) if args else fmt)

    def error(self, fmt, *args):
        """
        Convenience function for logging errors.
        """
        logging.log(40, '[%s] %s', self.name, (fmt % args) if args else fmt)

    def warning(self, fmt, *args):
        """
        Convenience function for logging warnings.
        """
        logging.log(30, '[%s] %s', self.name, (fmt % args) if args else fmt)

    def info(self, fmt, *args):
        """
        Convenience function for logging app information.
        """
        logging.log(20, '[%s] %s', self.name, (fmt % args) if args else fmt)

    def debug(self, fmt, *args):
        """
        Convenience function for logging debug information.
        """
        logging.log(10, '[%s] %s', self.name, (fmt % args) if args else fmt)

    def trace(self, fmt, *args):
        """
        Convenience function for logging traces (very detailed debug logs).
        """
        logging.log(1, '[%s] %s', self.name, (fmt % args) if args else fmt)

    def _setup_service(self):
        logging.getLogger().setLevel(self.config.worker_log_level)

        # Override the convenience methods with more efficient local variants
        self._local_convenience_methods()
        self.is_client = False
        self.is_service = True

    def _process_run(self):
        try:
            self._setup_service()
            if signal is not None:
                signal.signal(signal.SIGUSR2, self._log_more)
            if self.config.worker_nice and hasattr(os, 'nice'):
                os.nice(self.config.worker_nice)

            if self.config.worker_use_tcp:
                sock_desc, self._sock = self._make_server_socket()
            else:
                sock_desc = 'unix-domain:0'

            self._url = self._make_url(sock_desc)
            with open(self._urlfile, 'w') as fd:
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

        close = writer.close
        to1 = loop.call_later(cfg.worker_http_request_timeout1, close)
        to2 = loop.call_later(cfg.worker_http_request_timeout2, close)

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
                except ValueError as e:
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

    def _make_request_info(self, **kwargs):
        return RequestInfo(**kwargs)

    async def _serve_http(self, reader, writer):
        def _w(*data):
            data = b''.join(
                (bytes(d, 'utf-8') if isinstance(d, str) else d) for d in data)
            writer.write(data)
            return len(data)

        t0 = time.time()
        code = 500
        fds_ok = False
        method = path = version = ''
        try:
            head, hdrs, body, fds = await self._http11(reader, writer, fds=True)
            method, path, version = head.split(None, 3)[:3]
            peer = writer._transport._sock.getpeername()
            if not peer:
                peer = ['unix-domain']
                fds_ok = True

            try:
                if self.config.worker_url_path:
                    if path.startswith('/' + self.config.worker_url_path):
                        path = path[len(self.config.worker_url_path) + 1:]
                    else:
                        raise AttributeError()

                path = await self.validate_request_header(path, head)
                authed = True
            except PermissionError:
                authed = False

            (writer, code, mimetype, response
                ) = await self._handle_http_request(self._make_request_info(
                    peer=peer,
                    reader=reader,
                    writer=writer,
                    authed=authed,
                    method=method,
                    headers=hdrs,
                    path=path,
                    body=body,
                    fds=fds))

            if mimetype is None and response is None:
                sent = writer = None
            elif str(mimetype, 'utf-8') == self.FDS_MIMETYPE:
                if not fds_ok:
                    raise ValueError('Cannot send file descriptors over TCP')
                if not isinstance(response, list):
                    response = [response]

                fd_list = [r.fileno() for r in response]
                http_res = (
                    self._HTTP_200_OK % b'application/json' +
                    self.to_json([self._fd_to_magic_arg(a) for a in response]))

                await self._send_data_and_fds(writer, http_res, fd_list)
            else:
                l1 = (self._HTTP_RESPONSE.get(code) or
                    (self._HTTP_RESPONSE_UNKNOWN % code))
                sent = _w(l1, (self._HTTP_MIMETYPE % mimetype), response)

        except PermissionError:
            code = 403
            sent = _w(self._HTTP_RESPONSE[code], self._HTTP_SORRY)
        except AttributeError:
            code = 404
            sent = _w(self._HTTP_RESPONSE[code], self._HTTP_NOT_FOUND)
        except (TypeError, UnicodeDecodeError) as e:
            code = 400
            sent = _w(
                self._HTTP_RESPONSE[code],
                self._HTTP_JSON,
                self.to_json({'error': str(e)}))
        except Exception as e:
            code = 500
            sent = _w(
                self._HTTP_RESPONSE[code],
                self._HTTP_JSON,
                self.to_json({
                    'error': str(e),
                    'traceback': traceback.format_exc()}))
            self.exception('Error serving %s: %s', method, e)
        finally:
            try:
                await writer.drain()
                writer.close()
            except:
                pass

            peer = ':'.join(str(i) for i in peer[:2])
            elapsed_us = int(1000000 * (time.time() - t0)) if sent else None

            log_func = self.debug if (200 <= code < 300) else self.warning
            log_func('HTTP %s %s %d %s %s %s',
                method, path, code, sent or '-', elapsed_us or '-', peer)

            if hasattr(self, 'metrics_http_request'):
                self.metrics_http_request(method, path, code, sent, elapsed_us, peer)

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

    def get_method_name(self, request_info):
        """
        This function derives the basic method name (e.g. `ping`), from
        `request_info.path`. It returns the first component of the path,
        or the name `web_root` if the path is empty (`/`).

        Subclasses can override this to implement their own routing logic.
        """
        return request_info.path[1:].split('/', 1)[0] or 'web_root'

    def get_default_methods(self, request_info):
        """
        If the default function lookup mechanism fails, this function is
        called as a last-resort effort to route the request. By default
        it raises an exception to trigger a 404 or 403 error.

        Subclasses can override this with a function that returns a tuple
        of (api_method, raw_method), one of which should be `None` and the
        other an async API method function.
        """
        exc = (AttributeError if request_info.authed else PermissionError)
        raise exc(request_info.path)

    async def _handle_http_request(self, request_info):
        def _b(v):
            return v if isinstance(v, bytes) else bytes(v, 'utf-8')

        authed = request_info.authed
        writer = request_info.writer
        method_name = self.get_method_name(request_info)
        if authed:
            raw_method = getattr(self, 'raw_' + method_name, None)
            api_method = getattr(self, 'api_' + method_name, None)
        else:
            raw_method = api_method = None

        if not raw_method and not api_method:
            raw_method = getattr(self, 'public_raw_' + method_name, None)
            api_method = getattr(self, 'public_api_' + method_name, None)

        if not raw_method and not api_method:
            api_method, raw_method = self.get_default_methods(request_info)

        args, kwargs = [], {}
        mt = request_info.mimetype = 'application/json'
        enc = request_info.encoder = self.to_json
        if request_info.method == 'POST':
            ctype = request_info.headers['Content-Type']
            if ctype == 'application/x-msgpack':
                request_info.mimetype = mt = ctype
                request_info.encoder = enc = self.to_msgpack
                body = request_info.body = self.from_msgpack(request_info.body)
                args = body.pop('_args', [])
                kwargs = body
            elif ctype == 'application/json':
                body = request_info.body = self.from_json(request_info.body)
                args = body.pop('_args', [])
                kwargs = body
            elif ctype == 'application/x-www-form-urlencoded':
                body = str(request_info.body, 'utf-8').strip()
                kwargs = urllib.parse.parse_qs(body)
                for k in kwargs.keys():
                    if len(kwargs[k]) == 1:
                        kwargs[k] = kwargs[k][0]
                args = kwargs.pop('_args', [])
            else:
                self.warning('Unhandled POST MIME type: %s' % ctype)

        if request_info.fds:
            args = [self._fd_from_magic_arg(a, request_info.fds) for a in args]
            if request_info.body.pop(self.REPLY_TO_FIRST_FD, False):
                fd = args.pop(0)
                loop = asyncio.get_running_loop()
                if isinstance(fd, socket.socket):
                    _, writer2 = await asyncio.open_connection(sock=fd)
                else:
                    writer2 = FileWriter(fd)
                writer.write(self._HTTP_202_REPLIED_TO_FIRST_FD)
                await writer.drain()
                writer.close()
                request_info.writer = writer = writer2

        if inspect.isasyncgenfunction(api_method):
            raw_method = self._wrap_async_generator(api_method)

        if raw_method is not None:
            _wrapped = self._wrap_drain_and_close(raw_method)
            create_background_task(_wrapped(request_info, *args, **kwargs))
            return writer, 200, None, None

        try:
            mimetype, resp = await api_method(request_info, *args, **kwargs)
            if not mimetype:
                mimetype = request_info.mimetype
                resp = request_info.encoder(resp)

            return writer, 200, _b(mimetype), resp
        except Exception as e:
            if authed:
                return writer, 500, _b(mt), enc({
                    'error': str(e),
                    'traceback': traceback.format_exc()})
            else:
                return writer, 500, _b(mt), enc({'error': str(e)})

    async def init_servers(self, servers):
        """
        Subclasses can override this with initialization logic.
        This function should return the (potentially modified) list of
        server instances.
        """
        return servers

    async def _start_server(self):
        return await asyncio.start_server(self._serve_http,
            sock=self._sock)

    async def _start_unix_server(self):
        return await asyncio.start_unix_server(self._serve_http,
            path=self._unixfile)

    async def _main_httpd_loop(self):
        if self.config.worker_use_tcp:
            self._servers.append(await self._start_server())
        if self.config.worker_use_unixdomain:
            self._servers.append(await self._start_unix_server())
        self._servers = await self.init_servers(self._servers)
        return await asyncio.gather(*[s.serve_forever() for s in self._servers])

    def _make_server_socket(self):
        _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _sock.bind((
            self.config.worker_listen_host,
            self.config.worker_listen_port))
        _sock.settimeout(self.config.worker_accept_timeout)
        _sock.listen(self.config.worker_listen_queue)
        _sock_desc = '%s:%d' % _sock.getsockname()[:2]
        return _sock_desc, _sock

    def _make_url(self, host_port):
        self._secret = self.config.worker_secret
        if not self._secret:
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

    def to_json(self, data):
        """
        Serializes the data to JSON, returning the generated JSON as bytes.

        Override this if you need de/serialization for app-specific data.
        """
        return bytes(json.dumps(data) + '\n', 'utf-8')

    def from_json(self, jd):
        """
        Deserializes data from JSON.

        Override this if you need de/serialization for app-specific data.
        """
        return json.loads(jd if isinstance(jd, str) else str(jd, 'utf-8'))

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
            raise ValueError('to_msgpack failed: %s' % (exc,))

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

        d = d if isinstance(d, (bytes, bytearray)) else bytes(d, 'latin-1')
        return msgpack.unpackb(d, ext_hook=_from_exttype)

    async def _url_connect(self, url):
        proto, _, host_port, path = url.split('/', 3)

        allow_unix = bool(self._peeraddr or not self.config.worker_use_tcp)
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
        rd_wr = await asyncio.open_connection(host, int(port))
        self._peeraddr = rd_wr[0]._transport._sock.getpeername()
        return ('/' + path), False, rd_wr

    async def _send_data_and_fds(self, writer, data, fds):
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
        sock = reader._transport._sock
        if fds and (sock.family == socket.AF_UNIX):
            try:
                reader._transport.pause_reading()
                sock.settimeout(2)
                sock.setblocking(1)
                data, ancdata, _, _ = sock.recvmsg(bufsize, bufsize // 8)
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
                sock.setblocking(0)
                sock.settimeout(0)
                reader._transport.resume_reading()
            return None, []
        else:
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

    def _get_exception(self, resp_code):
        exc = RuntimeError
        try:
            resp_code = int(resp_code)
        except:
            pass
        if resp_code in (401, 403, 407):
            exc = PermissionError
        elif resp_code in (404, ):
            exc = KeyError
        elif 300 <= resp_code < 400:
            exc = IOError
        elif 400 <= resp_code < 500:
            exc = ValueError
        return exc

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

        use_json = kwargs.pop(self.CALL_USE_JSON, False)

        reply_to = kwargs.pop(self.CALL_REPLY_TO, None)
        if reply_to:
            if isinstance(reply_to, RequestInfo):
                if (not use_json) and ('/json' in reply_to.mimetype):
                    use_json = True
                reply_to = reply_to.socket
            args = [reply_to] + list(args)
            kwargs[self.REPLY_TO_FIRST_FD] = True

        retries = kwargs.pop(self.CALL_MAX_TRIES, 2)
        for attempt in range(0, retries + 1):
            try:
                url = self._url +'/'+ fn
                url_path, fds_ok, io_pair = await self._url_connect(url)
                break
            except:
                if attempt >= retries:
                    raise
                else:
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
                    elif ctype == 'text/event-stream':
                        result = self.from_server_sent_event(body)
                    else:
                        result = {'mimetype': ctype, 'data': body}
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
                    result = {
                        'error': 'Failed to unpack %s: %s' % (body, e),
                        'traceback': traceback.format_exc()}

            if 200 <= resp_code < 300:
                if (not isinstance(result, dict)) or ('error' not in result):
                    if chunked:
                        return self._chunk_decoder(reader, hdrs, body, rfds)
                    return result
                resp_code = 500

            exc = self._get_exception(resp_code)
            if 'traceback' in result:
                self.error('Remote %s', result['traceback'])
            raise exc('HTTP %d: %s' % (resp_code, result.get('error')))
        finally:
            rtime = 1000 * (time.time() - t0)
            (self.debug if (200 <= resp_code < 300) else self.error)(
                'CALL %s(%s)%s %s %.2fms',
                fn, str_args(args), fds, resp_code, rtime)

    async def ping(self):
        """
        Check whether the service is running.
        """
        if self.is_client:
            self._peeraddr = None
            return await self.call('ping', call_max_tries=0)
        else:
            return {'pong': True, 'loopback': True}

    async def quitquitquit(self):
        """
        Shut down the service.
        """
        if self.is_client:
            return await self.call('quitquitquit', call_max_tries=0)
        else:
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

    def _chunk_decoder(self, reader, hdrs, buffer, rfds):
        ctype = hdrs['Content-Type']
        if ctype == 'application/x-msgpack':
            decode = self.from_msgpack
        elif ctype == 'application/json':
            decode = self.from_json
        elif ctype == 'text/event-stream':
            decode = self.from_server_sent_event
        else:
            decode = lambda v: v

        async def decoded_chunk_generator():
            nonlocal reader, buffer
            if rfds:
                yield {'received_fds': rfds}

            finished = False
            while True:
                chunk, buffer = self._http11_chunk(buffer)
                if chunk is None:
                    if reader:
                        data = await reader.read(8192)
                        if data:
                            buffer += data
                        else:
                            reader = None
                    else:
                        break
                elif chunk == b'':
                    finished = True
                else:
                    yield decode(chunk)

            if not finished:
                # FIXME: This needs documenting!
                raise IOError('Incomplete result, missing end-of-stream marker')

        return decoded_chunk_generator

    def _wrap_drain_and_close(self, raw_method):
        async def draining_raw_method(request_info, *args, **kwargs):
            writer = request_info.writer
            try:
                await raw_method(request_info, *args, **kwargs)
                # As we may be passing the underlying file descriptor to
                # another worker, try not to be too hasty about closing.
                return await asyncio.sleep(0.01)
            except Exception as e:
                err = {'error': str(e)}
                if request_info.authed:
                    err['traceback'] = traceback.format_exc()
                mt = bytes(request_info.mimetype, 'utf-8')
                writer.write(self._HTTP_RESPONSE[500])
                writer.write(self._HTTP_MIMETYPE % mt)
                writer.write(request_info.encoder(err))
            finally:
                try:
                    await writer.drain()
                except:
                    pass
                # Defer this slightly, again avoiding premature closing
                asyncio.get_running_loop().call_soon(writer.close)
        return draining_raw_method

    def _send_chunked(self, writer, data):
        writer.write(self._HTTP_CHUNK_BEG % len(data))
        if data:
            writer.write(data)
        writer.write(self._HTTP_CHUNK_END)

    def _wrap_async_generator(self, api_method):
        # Use chunked encoding or text/event-stream to send multiple results
        class RemoteError(Exception):
            http_code = 500
        async def raw_method(request_info, *args, **kwargs):
            enc = request_info.encoder
            writer = request_info.writer
            resp_mimetype = request_info.mimetype
            events = False
            first = True
            try:
                async for m, r in api_method(request_info, *args, **kwargs):
                    mimetype, resp = m, r
                    if resp is None and mimetype is None:
                        return

                    if mimetype and first:
                        resp_mimetype = mimetype
                        if mimetype == 'text/event-stream':
                            events = enc = self.to_server_sent_event

                    if first:
                        if not mimetype and isinstance(resp, dict):
                            if 'finished' in resp and 'error' in resp:
                                re = RemoteError(resp['error'])
                                re.http_code = resp['finished']
                                raise re

                        mt = bytes(resp_mimetype, 'utf-8')
                        writer.write(self._HTTP_200_CHUNKED_OK % mt)

                    self._send_chunked(writer, enc(resp))
                    await writer.drain()
                    first = False

                # Send an empty chunk, to delimit a completed stream
                # FIXME: This needs documenting!
                self._send_chunked(writer, b'')

            except (IOError, BrokenPipeError) as exc:
                self.debug('Broken pipe: %s' % exc)
            except Exception as e:
                data = {'error': str(e)}
                if request_info.authed:
                    data['traceback'] = traceback.format_exc()
                if events:
                    data = {'event': 'error', 'data': data}
                if first:
                    try:
                        http_resp = self._HTTP_RESPONSE[int(e.http_code)]
                    except (AttributeError, ValueError, KeyError):
                        http_resp = self._HTTP_RESPONSE[500]

                    mt = bytes(resp_mimetype, 'utf-8')
                    writer.write(http_resp)
                    writer.write(self._HTTP_MIMETYPE % mt)
                    writer.write(enc(data))
                else:
                    self._send_chunked(writer, enc(data))
                    # Deliberatly not sending the completed marker, we exploded

        return raw_method

    async def public_raw_websocket(self, request_info):
        """/websocket

        Create a persistent websocket connection.
        """
        # Check headers; if they match
        # Send back websocket Upgrade: headers etc
        # ...
        raise RuntimeError('FIXME: Not implemented')

    async def public_raw_ping(self, request_info, **kwa):
        """/ping

        Check whether the microservice is running.
        """
        writer = request_info.writer
        if request_info.authed:
            mt = request_info.mimetype
            enc = request_info.encoder
            body = request_info.body
            if not isinstance(body, dict):
                body = {}

            body['pong'] = True
            body['conn'] = str_addr(writer._transport._sock.getsockname())
            body['_format'] = 'Pong via %(conn)s!'

            writer.write((self._HTTP_200_OK % bytes(mt, 'utf-8')) + enc(body))
        else:
            # Not authed: do less work and don't let caller influence output
            writer.write(self._HTTP_200_STATIC_PONG)

        await writer.drain()
        writer.close()

    async def api_config(self, request_info):
        """/config

        Returns the current configuration.
        """
        return None, {
            'config': self.config.as_dict(),
            '_format': str(self.config).replace('%', '%%')}

    async def api_quitquitquit(self, request_info):
        """/quitquitquit

        Shut down the microservice.
        """
        try:
            await self.shutdown()
        except:
            pass
        asyncio.get_running_loop().call_soon(self._real_shutdown)
        return None, "Goodbye forever"

    def get_docstring(self, method):
        """
        Fetch the docstring for a given method.

        Submodules can override this if they want to provide custom help.
        """
        return getattr(method, '__doc__', None)

    async def api_help(self, request_info, command=None):
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

            commands = self._all_commands()
            cmd_list = ['API Commands: ']
            first = True
            for command in sorted(list(commands.keys())):
                if command in ('ping', 'help', 'config', 'quitquitquit'):
                    continue
                if len(cmd_list[-1]) + len(command) > 75:
                    cmd_list.append('   ')
                elif not first:
                    cmd_list[-1] += ', '
                cmd_list[-1] += command
                first = False
            main_doc = main_doc.replace('__API_COMMANDS__', '\n'.join(cmd_list))

            return None, doc(self) +'\n'+ main_doc

        api_command = getattr(self, 'api_' + command, None)
        if not api_command:
            api_command = getattr(self, 'public_api_' + command, None)
        if api_command:
            return None, doc(api_command)

        raw_command = getattr(self, 'raw_' + command, None)
        if not raw_command:
            raw_command = getattr(self, 'public_raw_' + command, None)
        if raw_command:
            return None, doc(raw_command)

        return None, doc(None)

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

    @classmethod
    def TextFormat(cls, result):
        if isinstance(result, dict) and '_format' in result:
            return result.pop('_format')
        else:
            return '%s'

    def print_result(self, result, print_raw=False, print_json=False):
        if print_raw:
            print('%s' % result)
        elif print_json:
            print(str(self.to_json(result), 'utf-8'))
        elif isinstance(result, bytearray):
            sys.stdout.buffer.write(result)
            sys.stdout.buffer.flush()
        else:
            print(self.TextFormat(result) % result)

    @classmethod
    def Main(cls, args):
        """Usage: rpckitten <command> [--json|--raw] [<args ...>]

    Commands:

        config          - Display the current configuration
        help <command>  - Get help about a rpckitten commands
        ping            - Check whether rpckitten is running
        start           - Start the background service
        stop            - Stop the background service
        restart         - Stop and Start!

    __API_COMMANDS__

    You can also treat any API method as a command and invoke it from
    the command line. Add --json or --raw to alter whether/how the
    output gets formatted.

    Examples:
        rpckitten ping --json
        rpckitten help ping
        """
        if not args:
            print(cls.__doc__.strip().replace('\n    ', '\n'))
            print()
            print('Try this: %s help' % sys.argv[0])
            sys.exit(1)

        async def async_main(config, args):
            def _extract_bool_arg(args, arg):
                val = (arg in args)
                if val:
                    args.remove(arg)
                return val

            print_json = _extract_bool_arg(args, '--json')
            print_raw = _extract_bool_arg(args, '--raw')

            self = cls(config=config)
            name = '%s/%s' % (config.app_name, self.name)
            command = args.pop(0)

            def _print_result(result):
                self.print_result(result, print_raw=print_raw, print_json=print_json)

            try:
                if args and command in ('start', 'stop', 'restart'):
                    raise ValueError('invalid arguments: %s' % ' '.join(args))

                args, kwargs = self.extract_kwargs(args,
                    allowed=self._command_kwargs(command))

                if command == 'help':
                    _, result = await self.api_help(None, *args, **kwargs)
                    return _print_result(result)

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

                await self.connect()
                result = await self.call(command, *args, **kwargs)
                if inspect.isasyncgenfunction(result):
                    async for res in result():
                        _print_result(res)
                    return
                else:
                    return _print_result(result)

            except cls.NotRunning:
                sys.stderr.write(
                    '%s: Not running: Start it first?\n' % self.name)
                sys.exit(1)
            except KeyError as e:
                sys.stderr.write('%s %s failed: %s\n' % (self.name, command, e))
                sys.exit(2)
            except ValueError as e:
                sys.stderr.write('%s %s failed: %s\n' % (self.name, command, e))
                sys.exit(3)
            except IOError as e:
                sys.stderr.write('%s %s failed: %s\n' % (self.name, command, e))
                sys.exit(4)
            except PermissionError as e:
                sys.stderr.write('%s %s failed: %s\n' % (self.name, command, e))
                sys.exit(5)
            except RuntimeError as e:
                sys.stderr.write('%s %s failed: %s\n' % (self.name, command, e))
                sys.exit(6)

        try:
            # FIXME: Allow options or environment variables that tweak
            #        how we instanciate the class here.
            config = cls.Configuration()
            args = config.configure(list(args), strict=False)
            task = async_main(config, args)
        except:
            traceback.print_exc()

        asyncio.run(task)

