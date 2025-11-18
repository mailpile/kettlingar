"""
Mix-in for RPCKittens that also serve a "normal" website.
"""
import datetime
import inspect
import os
import re

from kettlingar import RPCKitten, HttpResult, RequestInfo


# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments
# pylint: disable=too-many-instance-attributes


class Route:
    """
    This class represents a single route in our URL map; mapping paths
    from our URL space to API functions.

    For each mapping, a Jinja template can be specified to render the
    result, the route can be toggled public/private, we can specify which
    HTTP verbs to accept (default is `GET` only), we can specify whether
    to parse the query string (`?foo=bar&...`) for arguments and whether
    this route handles subpaths or only exact matches.

    The path rule can be either a plain string, a Django style string
    including variable placeholders, or a regular expression with named
    matches (names are mapped to API method keyword arguments).
    """
    # FIXME: Give some examples of a route map
    def __init__(self, path_rule, api_method,
            template=None,
            public=False,
            methods='',
            subpaths=False,
            qs=False):
        self.path_rule = path_rule
        self.api_method = api_method
        self.template = template
        self.public = public
        self.subpaths = subpaths
        self.qs = qs
        self.methods = set([m.upper() for m in methods] or ('GET',))
        self.simple = (not subpaths)

    CONVERSIONS = {
        'int': r'[0-9]+',
        'str': r'[^/]+',
        'slug': r'[a-zA-Z0-9_-]+'}

    def __repr__(self):
        return 'Route(%s, %s, template=%s, public=%s, qs=%s)' % (
            repr(self.path_rule).replace('re.compile(', 're('),
            self.api_method, self.template, self.public, self.qs)

    def compile(self):
        """Convert django-style path rules into compiled regexes."""
        if isinstance(self.path_rule, re.Pattern):
            return self

        parts = self.path_rule.split('/')
        for i, p in enumerate(parts):
            if p[:1] == '<' and p[-1:] == '>':
                p = p[1:-1]
                if ':' in p:
                    t, p = p.split(':')  # Deliberate: More than one throws!
                else:
                    t = 'str'
                parts[i] = '(?P<%s>%s)' % (p, self.CONVERSIONS[t])
                self.simple = False

        path_rule = '^' + '/'.join(parts)
        if self.subpaths:
            trailing = path_rule[-1:] == '/'
            mname = self.subpaths if isinstance(self.subpaths, str) else 'path'
            path_rule = path_rule.rstrip('/')
            path_rule += '(?P<%s>/.*)%s$' % (mname, '' if trailing else '?')
        else:
            path_rule += '$'

        if not self.simple:
            self.path_rule = re.compile(path_rule)

        return self

    def matches(self, request_info, path):
        """Check whether this rule matches a given path."""
        if request_info.method not in self.methods:
            return False

        if not (self.public or request_info.authed):
            return False

        if path == self.path_rule:
            return self, [], {}

        if isinstance(self.path_rule, re.Pattern):
            m = self.path_rule.match(path)
            if m:
                return self, [], m.groupdict()

        return False


class WebKitten:
    """
    A mix-in class for RPCKittens that want to also expose a normal public
    website (including static and jinja2 templated content) using the same
    server as serves the API.
    """
    STATIC_MIMETYPES = {
        'COPYING': 'text/plain',
        'README':  'text/plain',
        'Makefile':'text/plain',
        '.c':      'text/plain',
        '.cpp':    'text/plain',
        '.css':    'text/css',
        '.doc':    'application/msword',
        '.docx':   'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        '.eps':    'application/postscript',
        '.gif':    'image/gif',
        '.h':      'text/plain',
        '.hpp':    'text/plain',
        '.htm':    'text/html',
        '.html':   'text/html',
        '.ico':    'image/x-icon',
        '.java':   'text/plain',
        '.jpg':    'image/jpeg',
        '.jpeg':   'image/jpeg',
        '.js':     'text/javascript',
        '.json':   'application/json',
        '.md':     'text/plain',
        '.odp':    'application/vnd.oasis.opendocument.presentation',
        '.ods':    'application/vnd.oasis.opendocument.spreadsheet',
        '.odt':    'application/vnd.oasis.opendocument.text',
        '.otf':    'font/otf',
        '.pdf':    'application/pdf',
        '.ps':     'application/postscript',
        '.png':    'image/png',
        '.pl':     'text/plain',
        '.py':     'text/plain',
        '.toml':   'text/plain',
        '.ttc':    'font/collection',
        '.ttf':    'font/ttf',
        '.txt':    'text/plain',
        '.woff':   'font/woff',
        '.woff2':  'font/woff2',
        '.yml':    'text/plain',
        '.xls':    'application/vnd.ms-excel',
        '.xlsx':   'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.xpm':    'image/x-pixmap',
    }

    WEB_JINJA_LOADER_PACKAGE = 'kettlingar'
    WEB_PACKAGE_STATIC_DIR = 'resources'
    WEB_PACKAGE_JINJA_DIR = 'templates'
    WEB_PREFIX = '/'
    WEB_ROUTES_FALL_BACK_TO_RPCKITTEN = True
    WEB_ROUTES_IGNORE = ('/ping', '/quitquitquit')
    WEB_ROUTES = [
        Route('/static/', 'public_api_web_static', public=True, subpaths=True),
        Route('/metrics', 'public_api_metrics',    public=True, qs=True),
        Route('404',      'public_api_web_404',    public=True, subpaths=True)]

    class Configuration(RPCKitten.Configuration):
        """Configuration variables for the webkitten."""
        WEB_JINJA_DIR = None
        WEB_STATIC_DIR = None
        WEB_FOLLOW_SYMLINKS = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # Req'd for cooperative inheritance

        self.package = self.WEB_JINJA_LOADER_PACKAGE
        self.routes = None
        self.simple_routes = {}
        self._static_loader = None
        self._jinja = None

        # Make sure our prefix composes nicely
        self.prefix = (self.WEB_PREFIX or '').strip('/')
        if self.prefix:
            self.prefix = '/%s/' % self.prefix
        else:
            self.prefix = '/'

        # monkey patch in our route handling and setup, with fallback
        # pylint: disable=access-member-before-definition
        self.rpc_api_methods = self.get_api_methods
        self.get_api_methods = self.web_api_methods
        self.init_rpckitten = self.init_servers
        self.init_servers = self.init_webkitten

    def web_url(self, subpath_or_request_info='', public=None):
        """Generate a valid URL to communicte with this webkitten."""
        if isinstance(subpath_or_request_info, RequestInfo):
            request_info, subpath = subpath_or_request_info, None
        else:
            request_info, subpath = None, subpath_or_request_info

        if request_info:
            if public is None:
                public = not request_info.authed
            subpath = request_info.path
            if subpath.startswith(self.prefix):
                subpath = subpath[len(self.prefix):]
            else:
                base_url = self.web_url()
                if subpath.startswith(base_url):
                    return subpath

        base_url = self.api_url
        if public:
            base_url, _auth = base_url.rsplit('/', 1)
        if subpath:
            subpath = subpath.lstrip('/')
        return '%s%s%s' % (
            base_url,
            self.prefix,
            subpath)

    async def init_webkitten(self, servers):
        """Initialize and configure the web server."""
        import locale
        locale.setlocale(locale.LC_TIME, "C")

        # Compile our route map, create simple_routes shortcuts
        self.routes = [r.compile() for r in self.WEB_ROUTES]
        self.simple_routes = dict(
            (r.path_rule, r) for r in self.routes if r.simple)

        return await self.init_rpckitten(servers)

    def common_jinja_vars(self, _request_info):
        """Common Jinja variables. Subclasses override."""
        return {}

    def _get_fs_loader(self, path, binary=False):
        import posixpath
        from jinja2 import FileSystemLoader
        from jinja2.loaders import split_template_path
        from jinja2.exceptions import TemplateNotFound

        if not binary:
            return FileSystemLoader(path,
                followlinks=getattr(self.config, 'web_follow_symlinks', True),
                encoding='utf-8')

        class BinaryFileSystemLoader(FileSystemLoader):
            """A binary-mode FileSystemLoader"""
            def get_source(self, _environment, template):
                """Fetch the source, return bytes!"""
                pieces = split_template_path(template)
                for searchpath in self.searchpath:
                    fn = posixpath.join(searchpath, *pieces)
                    if os.path.isfile(fn):
                        with open(fn, 'rb') as fd:
                            return fd.read(), fn, lambda: False
                raise TemplateNotFound(template, 'Tried: %s' % self.searchpath)

        return BinaryFileSystemLoader(path,
            followlinks=getattr(self.config, 'web_follow_symlinks', True))


    def jinja(self, recreate=False):
        """
        Return our Jinja environment, creating it if necessary. Once created
        the environment is cached as `self._jinja` for reuse.
        """
        if recreate or self._jinja is None:
            from jinja2 import Environment, select_autoescape
            from jinja2 import PackageLoader, ChoiceLoader

            loaders = [PackageLoader('kettlingar', 'templates')]  # Hard coded!
            if self.package and self.WEB_PACKAGE_JINJA_DIR:
                loaders += [PackageLoader(
                    self.package,
                    self.WEB_PACKAGE_JINJA_DIR)]
            if getattr(self.config, 'web_jinja_dir', None):
                loaders += [self._get_fs_loader(self.config.web_jinja_dir)]

            loaders = list(reversed(loaders))
            self._jinja = Environment(
                #cache_size=0,  # FIXME: Set to -1 to just cache everything
                enable_async=True,
                loader=ChoiceLoader(loaders),
                autoescape=select_autoescape())

        return self._jinja

    def static_loader(self, recreate=False):
        """
        Return our static resource loader, creating it if necessary. Once
        created the loader is cached as `self._static_loader` for reuse.
        """
        if recreate or self._static_loader is None:
            from jinja2 import PackageLoader, ChoiceLoader
            loaders = []
            if self.package and self.WEB_PACKAGE_STATIC_DIR:
                loaders += [PackageLoader(
                    self.package,
                    self.WEB_PACKAGE_STATIC_DIR,
                    encoding='latin-1')]
            if getattr(self.config, 'web_static_dir', None):
                loaders += [self._get_fs_loader(
                    self.config.web_static_dir,
                    binary=True)]

            loaders = list(reversed(loaders))
            if len(loaders) > 1:
                self._static_loader = ChoiceLoader(loaders)
            elif loaders:
                self._static_loader = loaders[0]
            else:
                raise ValueError('No static resources configured/found')

        return self._static_loader

    def web_api_methods(self, request_info):
        """Default methods for web-kittens.

        Looks requests up in our route map before falling back to the
        RPCKitten default routing.
        """
        ignored = request_info.path in self.WEB_ROUTES_IGNORE

        def _get_method_and_args(remap='', default=None):
            route_args_kwargs = self._get_route(request_info, remap=remap)
            if not route_args_kwargs:
                return default, [], {}

            route, args, kwargs = route_args_kwargs

            # If this route wants to parse the query string, do so.
            if route.qs and '?' in request_info.path:
                _, qs = request_info.path.split('?', 1)
                for kv in qs.split('&'):
                    # FIXME: Handle plus-encoding and %XX codes? Use stdlib?
                    if '=' in kv:
                        k, v = kv.split('=', 1)
                        kwargs[k] = v
                    else:
                        args.append(kv)

            api_func = getattr(self, route.api_method)  # May raise!
            if api_func.__annotations__:
                self._apply_annotations(api_func, args, kwargs)

            if route.template:
                request_info.routed_as = (route, api_func, args, kwargs)
                return self._web_handle, [], {}

            return api_func, args, kwargs

        # Try our routing first...
        try:
            if not ignored and (
                    not self.prefix
                    or request_info.path.startswith(self.prefix)):
                api_func, args, kwargs = _get_method_and_args()
                if api_func:
                    return api_func, None, args, kwargs
        except (AttributeError, PermissionError):
            pass

        # Fall back to RPCKitten default logic...
        try:
            if ignored or self.WEB_ROUTES_FALL_BACK_TO_RPCKITTEN:
                return self.rpc_api_methods(request_info)
        except (AttributeError, PermissionError):
            pass

        # Still no luck, try to map as a 404 error...
        api_func, args, kwargs = _get_method_and_args(
            remap='404',
            default=self.public_api_web_404)
        if api_func:
            return api_func, None, args, kwargs

        # Finally, if all else fails, raise an exception.
        exc = (AttributeError if request_info.authed else PermissionError)
        raise exc(request_info.path)

    def _get_route(self, request_info, remap=''):
        path = remap + request_info.path[(len(self.prefix or '/') - 1):]
        if '?' in path:
            path, _ = path.split('?', 1)

        route = self.simple_routes.get(path)
        if route:
            return route.matches(request_info, path)

        for route in self.routes:
            route_args_kwargs = route.matches(request_info, path)
            if route_args_kwargs:
                return route_args_kwargs

        return None

    async def _web_handle(self, request_info):
        try:
            route, api_func, args, kwargs = request_info.routed_as

            template = route.template
            if isinstance(template, str):
                template = self.jinja().get_template(template)

            variables = self.common_jinja_vars(request_info)
            variables.update({
                'partial': bool(kwargs.pop('htmx', False)),
                'base_url': self.web_url(public=not request_info.authed),
                'page_url': self.web_url(request_info)})

            if inspect.isasyncgenfunction(api_func):
                variables['results'] = results = []
                async for result in api_func(request_info, *args, **kwargs):
                    if isinstance(result, HttpResult):
                        results.append(result.data)
                    else:
                        results.append(result)
            else:
                variables.update(await api_func(request_info, *args, **kwargs))

            return HttpResult('text/html', await template.render_async(variables))
        except:
            self.exception('Error handling %s' % request_info.path)
            raise

    async def api_web_routes(self, _request_info):
        """/web_routes

        Return a summary of the current URL routing table.
        """
        return {'routes': [str(r) for r in self.routes]}

    async def public_api_web_404(self, _request_info, *_args, **_kwargs):
        """Return an HTTP 404 Not Found result."""
        return HttpResult('text/plain', 'Not Found', http_code=404)

    async def public_api_web_static(self, _request_info,
            path:str=None,
            mimetype:str=None,
            chunksize:int=16*1024):
        """/static <path> [--mimetype=<T>]

        Load and return a static resource by HTTP path. If not specified,
        the MIME-type will be guessed from the filename.
        """
        resource = path or _request_info.path[(len(self.prefix) - 1):]
        if not resource or '/.' in resource:
            raise PermissionError('Invalid path: %s' % resource)

        mimetype = (mimetype or
            self.STATIC_MIMETYPES.get(os.path.basename(resource)) or
            self.STATIC_MIMETYPES.get(os.path.splitext(resource)[-1]) or
            'application/octet-stream')

        mimetype += '\nCache-Control: public, max-age=360000, immutable'
        mimetype += '\nLast-Modified: ' + self._today()

        first = True
        error = 'Not Found'
        try:
            data, _, _ = self.static_loader().get_source(self.jinja(), resource)
            data = bytes(data, 'utf-8') if isinstance(data, str) else data
            while data:
                chunk, data = data[:chunksize], data[chunksize:]
                if first:
                    yield HttpResult(mimetype, chunk)
                    first = False
                else:
                    yield chunk
            return
        except (KeyError, OSError, IOError) as e:
            error = '%s(%s)' % (type(e).__name__, e)

        yield {'error': error, 'finished': True}

    def _today(self):
        return datetime.datetime.now().strftime('%a, %d %b %Y 00:00:00 GMT')


class WebKittenWithHelp(WebKitten):
    """
    A mix-in class for RPC Kittens which serves up help for the API methods
    as a human readable website.

    FIXME: This doesn't work yet!
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.routes['GET']['/help'] = (
            False, 'help.jinja', self.web_help, 'Help')

    async def web_help(self, _request_info, *args, **kwargs):
        """Provide help."""
        return await self.api_help(_request_info, *args, **kwargs)


if __name__ == '__main__':
    from kettlingar.metrics import MetricsKitten
    from kettlingar.tlskitten import TLSKitten

    class MiniWebKitten(RPCKitten, WebKitten, TLSKitten, MetricsKitten):
        """Miniature webkitten that does barely anything."""

        class Configuration(WebKitten.Configuration, TLSKitten.Configuration):
            """Configuration!"""
            APP_NAME = 'webkitten'
            WORKER_NAME = 'httpd'
            WEB_STATIC_DIR = '.'

    MiniWebKitten.Main()
