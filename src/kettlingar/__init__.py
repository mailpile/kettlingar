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

from .asynctools import create_background_task
from .str_utils import str_addr, str_args


__version__ = "0.0.1"


def _mkdirp(path, mode):
    if not os.path.exists(path):
        _mkdirp(os.path.dirname(path), mode)
        os.mkdir(path, mode)


class RPCKitten:
    """
    This is a generic worker process; it runs in the background listening
    for RPC calls over HTTP/1.1. Subclasses can expose methods by creating
    async functions named `api_FUNCTION_NAME` (or `raw_FUNCTION_NAME`).

    TODO: Document this a bit more.
    """
    MAGIC_FD = '_FD_BRE_MAGIC_'
    MAGIC_SOCK = '_SO_BRE_MAGIC_'

    HTTP_200_OK = (b'HTTP/1.1 200 OK\n'
                   b'Content-Type: %s\n'
                   b'Connection: close\n\n')
    HTTP_200_CHUNKED_OK = (b'HTTP/1.1 200 OK\n'
                   b'Transfer-Encoding: chunked\n'
                   b'Content-Type: %s\n'
                   b'Connection: close\n\n')

    HTTP_CHUNK_BEG = b'%x\r\n'
    HTTP_CHUNK_END = b'\r\n'

    HTTP_RESPONSE_UNKNOWN = b'HTTP/1.1 %d Unknown\n'
    HTTP_RESPONSE = {
        200: b'HTTP/1.1 200 OK\n',
        400: b'HTTP/1.1 400 Invalid Request\n',
        403: b'HTTP/1.1 403 Access Denied\n',
        404: b'HTTP/1.1 404 Not Found\n',
        500: b'HTTP/1.1 500 Internal Error\n'}

    HTTP_MIMETYPE = (b'Content-Type: %s\n'
                     b'Connection: close\n\n')
    HTTP_JSON = (b'Content-Type: application/json\n'
                 b'Connection: close\n\n')
    HTTP_SORRY = (b'Content-Type: application/json\n'
                  b'Connection: close\n\n'
                  b'{"error": "Sorry"}\n')

    class NotRunning(OSError):
        pass

    class Configuration:
        APP_NAME = 'kettlingar'
        APP_DATA_DIR = None
        APP_STATE_DIR = None

        WORKER_NAME = 'worker'

        WORKER_NICE = 0
        WORKER_UMASK = 0o770
        WORKER_LISTEN_QUEUE = 5
        WORKER_ACCEPT_TIMEOUT = 1
        WORKER_LISTEN_HOST = '127.0.0.1'
        WORKER_LISTEN_PORT = 0
        WORKER_LISTEN_PATH = ''
        WORKER_LOG_LEVEL = 0  # 20 sets the default to info

        WORKER_HTTP_REQUEST_TIMEOUT1 = 1.0
        WORKER_HTTP_REQUEST_TIMEOUT2 = 15.0
        WORKER_HTTP_REQUEST_MAX_SIZE = 1024*1024

        _METHODS = ('configure', 'set_defaults')

        def __init__(self):
            for cls in self.__class__.__mro__:
                for akey in cls.__dict__:
                    if akey.upper() == akey and akey[:1] != '_':
                        setattr(self, akey.lower(), None)

        def __str__(self):
            out = []
            for akey in self.__dict__:
                if akey.lower() == akey and akey not in self._METHODS:
                    val = getattr(self, akey)
                    if isinstance(val, (dict, list)):
                        out.append('--%s=%s' % (akey, json.dumps(val)))
                    else:
                        out.append('--%s=%s' % (akey, val))
            return '\n'.join(out)

        def set_defaults(self):
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

        def configure(self, args, strict=True):
            consumed = set()

            def _kv(i):
                k, v = i.strip().split(':')
                return k.strip, v.strip()

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

            unconsumed = [a for a in args if a not in consumed]
            if unconsumed and strict:
                raise ValueError('Unrecognized arguments: %s' % unconsumed)

            self.set_defaults()

            return unconsumed

    def __init__(self, config=None, args=None):
        super().__init__()

        if config is None:
            config = self.Configuration()
            if args:
                config.configure(args)

        self.config = config
        self.name = config.worker_name

        self._server = None
        self._unix_server = None
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

        self._init_logging()

    api_url = property(lambda s: s._url)
    api_addr = property(lambda s: s._peeraddr)

    async def connect(self, auto_start=False, retry=3):
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
                self._process = Process(target=self.run)
                self._process.start()
                self.name = self.config.worker_name + ':cli'

            if tried < retry - 1:
                await asyncio.sleep(0.05 + (tried * 0.15))

        self.error('Failed to connect to %s', self._urlfile)
        raise self.NotRunning()

    def _proc_title(self, sockdesc):
        return '%s/%s on %s' % (
            self.config.app_name, self.config.worker_name, sockdesc)

    def remove_files(self):
        for f in (self._unixfile, self._urlfile):
            try:
                os.remove(f)
            except (FileNotFoundError, OSError):
                pass

    async def shutdown(self):
        self.remove_files()

    def _init_logging(self):
        rootLogger = logging.getLogger()
        rootLogger.setLevel(self.config.worker_log_level)
        if not rootLogger.hasHandlers():
            fmt = "%(asctime)s %(process)d %(levelname).1s %(message)s"
            stderrLog = logging.StreamHandler()
            stderrLog.setFormatter(logging.Formatter(fmt))
            rootLogger.addHandler(stderrLog)

    def exception(self, f, *a):
        logging.exception('[%s] %s', self.name, (f % a) if a else f)

    def error(self, f, *a):
        logging.log(40, '[%s] %s', self.name, (f % a) if a else f)

    def warning(self, f, *a):
        logging.log(30, '[%s] %s', self.name, (f % a) if a else f)

    def info(self, f, *a):
        logging.log(20, '[%s] %s', self.name, (f % a) if a else f)

    def debug(self, f, *a):
        logging.log(10, '[%s] %s', self.name, (f % a) if a else f)

    def trace(self, f, *a):
        logging.log(1, '[%s] %s', self.name, (f % a) if a else f)

    def run(self):
        try:
            logging.getLogger().setLevel(self.config.worker_log_level)

            if signal is not None:
                signal.signal(signal.SIGUSR2, self._log_more)
            if self.config.worker_nice and hasattr(os, 'nice'):
                os.nice(self.config.worker_nice)

            sock_desc, self._sock = self._make_server_socket()

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
            self.info('Stopped %s(%s), pid=%d',
                type(self).__name__, self.name, os.getpid())
            logging.shutdown()

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

        return header, headers, request[hend+2:], fds

    async def _serve_http(self, reader, writer):
        def _w(data):
            writer.write(data)
            return len(data)
        method = path = version = ''
        try:
            head, hdrs, body, fds = await self._http11(reader, writer, fds=True)
            method, path, version = head.split(None, 3)[:3]
            peer = writer._transport._sock.getpeername() or ['unix-domain']

            try:
                path = await self._validate_request_header(path, head)
                authed = True
            except PermissionError:
                authed = False
            code, mimetype, response = await self._handle_http_request(
                authed, writer, method, path, hdrs, body, fds)

            if mimetype is None and response is None:
                sent = '-'
                writer = None
            else:
                l1 = (self.HTTP_RESPONSE.get(code) or
                    (self.HTTP_RESPONSE_UNKNOWN % code))
                sent = _w(l1 + (self.HTTP_MIMETYPE % mimetype) + response)

        except PermissionError:
            code = 403
            sent = _w(self.HTTP_RESPONSE[code] + self.HTTP_SORRY)
        except AttributeError:
            code = 404
            sent = _w(self.HTTP_RESPONSE[code] + self.HTTP_SORRY)
        except (TypeError, UnicodeDecodeError) as e:
            code = 400
            sent = _w(self.HTTP_RESPONSE[code]
                + self.HTTP_JSON
                + self._to_json({'message': str(e)}))
        except Exception as e:
            code = 500
            sent = _w(self.HTTP_RESPONSE[code]
                + self.HTTP_JSON
                + self._to_json({
                    'message': str(e),
                    'traceback': traceback.format_exc()}))
            self.exception('Error serving %s: %s', method, e)
        finally:
            log_func = self.debug if (code == 200) else self.warning
            log_func('HTTP %s %s %d %s %s',
                method, path, code, sent,
                ':'.join(str(i) for i in peer[:2]))
            if writer:
                await writer.drain()
                writer.close()

    async def _validate_request_header(self, path, header):
        if self._secret not in header:
            raise PermissionError()
        if path.startswith('/' + self._secret + '/'):
            return path[len(self._secret) + 1:]
        return path

    async def _handle_http_request(self,
            authed, writer, method, path, headers, body, fds):
        if authed:
            raw_method = getattr(self, 'raw_' + path[1:], None)
            api_method = getattr(self, 'api_' + path[1:], None)
        else:
            raw_method = getattr(self, 'public_raw_' + path[1:], None)
            api_method = getattr(self, 'public_api_' + path[1:], None)

        if not raw_method and not api_method:
            raise (AttributeError if authed else PermissionError)(path)

        args = []
        mt, enc = 'application/json', self._to_json
        if method == 'POST':
            if headers['Content-Type'] == 'application/x-msgpack':
                mt, enc = 'application/x-msgpack', self._to_msgpack
                body = self._from_msgpack(body)
                args = body.pop('_args', [])
            elif headers['Content-Type'] == 'application/json':
                body = self._from_json(body)
                args = body.pop('_args', [])

        if fds:
            args = [self._fd_from_magic_arg(a, fds) for a in args]

        if inspect.isasyncgenfunction(api_method):
            raw_method = self._wrap_async_generator(api_method)

        if raw_method is not None:
            create_background_task(
                raw_method(writer, mt, enc, method, headers, body, *args))
            return 200, None, None

        try:
            mimetype, resp = await api_method(method, headers, body, *args)
            if not mimetype:
                mimetype, resp = mt, enc(resp)

            return 200, bytes(mimetype, 'utf-8'), resp
        except Exception as e:
            if authed:
                return 500, bytes(mt, 'utf-8'), enc({
                    'error': str(e),
                    'traceback': traceback.format_exc()})
            else:
                return 500, bytes(mt, 'utf-8'), enc({'error': str(e)})

    async def init_server(self, server):
        return server

    async def start_server(self):
        return await asyncio.start_server(self._serve_http,
            sock=self._sock)

    async def start_unix_server(self):
        return await asyncio.start_unix_server(self._serve_http,
            path=self._unixfile)

    async def _main_httpd_loop(self):
        self._server = await self.init_server(await self.start_server())
        self._unix_server = await self.start_unix_server()
        return await asyncio.gather(
            self._server.serve_forever(),
            self._unix_server.serve_forever())

    def _make_server_socket(self):
        _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _sock.bind((
            self.config.worker_listen_host,
            self.config.worker_listen_port))
        _sock.settimeout(self.config.worker_accept_timeout)
        _sock.listen(self.config.worker_listen_queue)
        _sock_desc = '%s:%d' % _sock.getsockname()[:2]
        return _sock_desc, _sock

    def _make_url(self, host_port):
        self._secret = str(b64encode(os.urandom(18), b'-_').strip(), 'utf-8')
        return 'http://%s/%s' % (host_port, self._secret)

    def _log_more(self, *ignored):
        pass  # FIXME

    def _to_json(self, data):
        return bytes(json.dumps(data) + '\n', 'utf-8')

    def _from_json(self, jd):
        return json.loads(jd if isinstance(jd, str) else str(jd, 'utf-8'))

    def _to_msgpack(self, data):
        return msgpack.packb(data)

    def _from_msgpack(self, d):
        return msgpack.unpackb(
            d if isinstance(d, (bytes, bytearray)) else bytes(d, 'latin-1'))

    async def _url_connect(self, url):
        proto, _, host_port, path = url.split('/', 3)

        if self._unixfile and self._peeraddr:
            try:
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
        except:
            self.exception('%s.sendmsg(..., fds=%s)', sock, fds)
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
                    self.MAGIC_SOCK, a.family, a.type, a.proto)
            return '%s-%s' % (self.MAGIC_FD, a.mode)
        return a

    def _fd_from_magic_arg(self, a, fds):
        if isinstance(a, str):
            if a.startswith(self.MAGIC_SOCK):
                family, typ, proto = (int(i) for i in a.split('-')[1:])
                try:
                    fd = os.fdopen(fds.pop(0))
                    return socket.fromfd(fd.fileno(), family, typ, proto)
                finally:
                    fd.close()
            if a.startswith(self.MAGIC_FD):
                _, mode = a.split('-')
                return os.fdopen(fds.pop(0), mode=mode)
        return a

    async def call(self, fn, *args, **kwargs):
        t0 = time.time()

        retries = kwargs.pop('reconnect', 2)
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
        payload = self._to_msgpack(kwargs or {})
        http_req = bytes("""\
POST %s HTTP/1.1
Connection: close
Content-Type: application/x-msgpack
Content-Length: %d

""" % (url_path, len(payload)), 'utf-8') + payload

        try:
            if fds:
                if not fds_ok:
                    raise ValueError('Cannot send file descriptors over TCP')
                await self._send_data_and_fds(writer, http_req, fds)
            else:
                writer.write(http_req)
                await writer.drain()

            chunked, resp_code, body, result = False, 500, b'', {}
            try:
                head, hdrs, body, rfds = await self._http11(reader, writer)
                resp_code = int(head.split(None, 2)[1])
                chunked = (hdrs.get('Transfer-Encoding') == 'chunked')
                if body and not chunked:
                    ctype = hdrs['Content-Type']
                    if ctype == 'application/x-msgpack':
                        result = self._from_msgpack(body)
                    elif ctype == 'application/json':
                        result = self._from_json(body)
                    else:
                        result = {'data': body}
            except IOError as e:
                result = {'error': 'Read failed: %s' % e}
            except Exception as e:
                if not body:
                    result = {'error': 'Empty response body'}
                else:
                    result = {'error': 'Failed to unpack %s: %s' % (body, e)}

            if resp_code == 200:
                if 'error' not in result:
                    if chunked:
                        return self._chunk_decoder(reader, hdrs, body, rfds)
                    return result
                resp_code = 500

            exc = RuntimeError
            if resp_code in (401, 403, 407):
                exc = PermissionError
            elif 300 <= resp_code < 400:
                exc = IOError
            elif 400 <= resp_code < 500:
                exc = ValueError

            raise exc('HTTP %d: %s' % (resp_code, result.get('error')))
        finally:
            rtime = 1000 * (time.time() - t0)
            (self.debug if (resp_code == 200) else self.error)(
                'CALL %s(%s)%s %s %.2fms',
                fn, str_args(args), fds, resp_code, rtime)

    async def ping(self):
        self._peeraddr = None
        pong = await self.call('ping', reconnect=0)
        return pong

    async def quitquitquit(self):
        return await self.call('quitquitquit', reconnect=0)

    async def public_api_varz(self, m, h, b):
        if m != 'GET':
            raise PermissionError('Nope')
        return None, {'stats': True}

    def _http11_chunk(self, buffer):
        try:
            ln, data = buffer.split(b'\r\n', 1)
            ln = int(ln, 16)
            if len(data) >= ln:
                return data[:ln], data[ln+2:]
        except (ValueError, IndexError):
            pass
        return None, buffer

    def _chunk_decoder(self, reader, hdrs, buffer, rfds):
        ctype = hdrs['Content-Type']
        if ctype == 'application/x-msgpack':
            decode = self._from_msgpack
        elif ctype == 'application/json':
            decode = self._from_json
        else:
            decode = lambda v: v

        async def decoded_chunk_generator():
            nonlocal reader, buffer
            if rfds:
                yield {'received_fds': rfds}
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
                else:
                    yield decode(chunk)

        return decoded_chunk_generator

    def _wrap_async_generator(self, api_method):
        # Use chunked encoding to render and yield multiple API results
        async def raw_method(writer, mt, enc, method, headers, body, *args):
            first = True
            try:
                async for m, r in api_method(method, headers, body, *args):
                    mimetype, resp = m, r
                    if resp is None and mimetype is None:
                        return

                    if mimetype and first:
                        mt = mimetype
                        enc = lambda v: v
                    if first:
                        writer.write(
                            self.HTTP_200_CHUNKED_OK % bytes(mt, 'utf-8'))

                    data = enc(resp)
                    writer.write(self.HTTP_CHUNK_BEG % len(data))
                    writer.write(data)
                    writer.write(self.HTTP_CHUNK_END)

                    await writer.drain()
                    first = False
            except (IOError, BrokenPipeError):
                writer.close()
                writer = None
            finally:
                if writer:
                    await writer.drain()
                    writer.close()

        return raw_method

    async def raw_ping(self, writer, mt, enc, method, headers, body):
        if not isinstance(body, dict):
            body = {}

        body['pong'] = True
        body['conn'] = str_addr(writer._transport._sock.getsockname())

        writer.write((self.HTTP_200_OK % bytes(mt, 'utf-8')) + enc(body))
        await writer.drain()
        writer.close()

    async def api_config(self, method, headers, body):
        return None, str(self.config)

    async def api_quitquitquit(self, method, headers, body):
        try:
            await self.shutdown()
        except:
            pass
        def _cleanup():
            for _cleanup_func in (
                    self._unix_server.close,
                    self._server.close):
                try:
                    _cleanup_func()
                except:
                    pass
            os._exit(0)
        asyncio.get_running_loop().call_soon(_cleanup)
        return None, "Goodbye forever"

    async def api_help(self, m, h, b, *args):
        def doc(obj):
            docstring = (obj.__doc__ or 'No Help Available').strip()
            return docstring.replace('\n    ', '\n') + '\n'

        if not args:
            return None, doc(self)

        api_command = getattr(self, 'api_' + args[0], None)
        if api_command:
            return None, doc(api_command)

        raw_command = getattr(self, 'raw_' + args[0], None)
        if api_command:
            return None, doc(raw_command)

        return None, doc(None)

    @classmethod
    def extract_kwargs(cls, args, allowed):
        is_arg = lambda a: isinstance(a, str) and a[:2] == '--'
        kwargs = dict(a[2:].split('=', 1) for a in args if is_arg(a))
        for k in kwargs:
            if k not in allowed:
                raise ValueError('Unrecognized option: --%s' % k)
        return [a for a in args if not is_arg(a)], kwargs

    @classmethod
    def Main(cls, args):
        if not args:
            print(cls.__doc__.strip().replace('\n    ', '\n'))
            sys.exit(1)

        async def async_main(config, args):
            # FIXME: Allow options or environment variables that tweak
            #        how we instanciate the class here.

            print_json = ('--json' in args)
            if print_json:
                args.remove('--json')

            self = cls(config)
            name = '%s/%s' % (config.app_name, self.name)
            command = args.pop(0)

            if command == 'help':
                print((await self.api_help(None, None, None, *args))[1])
                return

            if command == 'start':
                await self.connect(auto_start=True)
                sys.stderr.write(
                    '%s: Running at %s\n' % (name, self._url))
                if self._unixfile and os.path.exists(self._unixfile):
                    sys.stderr.write(
                        '%s: Running at %s\n' % (name, self._unixfile))
                return os._exit(0)

            if command == 'stop':
                try:
                    await self.connect(auto_start=False, retry=0)
                    await self.quitquitquit()
                    sys.stderr.write('%s: Stopped\n' % name)
                except cls.NotRunning:
                    sys.stderr.write('%s: Not running\n' % name)
                return

            if command == 'restart':
                await async_main(config, ['stop'])
                await async_main(config, ['start'])
                return

            def _print_result(result):
                if isinstance(result, dict) and '_format' in result:
                    fmt = result.pop('_format')
                else:
                    fmt = '%s'
                if print_json:
                    print(json.dumps(result))
                else:
                    print(fmt % result)

            try:
                await self.connect()
                result = await self.call(command, *args, _fromm='CLI')
                if inspect.isasyncgenfunction(result):
                    async for res in result():
                        _print_result(res)
                else:
                    _print_result(result)
            except cls.NotRunning:
                sys.stderr.write(
                    '%s: Not running: Start it first?\n' % self.name)
                sys.exit(1)
            except:
                self.exception('%s %s failed', self.name, command)
                logging.shutdown()
                sys.exit(2)

        try:
            config = cls.Configuration()
            task = async_main(config, config.configure(list(args), strict=False))
        except:
            traceback.print_exc()

        asyncio.run(task)
