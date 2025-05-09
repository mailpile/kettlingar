import datetime
import inspect
import os


class WebKitten:
    STATIC_MIMETYPES = {
        'COPYING': 'text/plain',
        'README':  'text/plain',
        '.css':    'text/css',
        '.ico':    'image/x-icon',
        '.jpg':    'image/jpeg',
        '.jpeg':   'image/jpeg',
        '.js':     'text/javascript',
        '.md':     'text/plain',
        '.png':    'image/png',
        '.txt':    'text/plain',
    }

    def __init__(self, *args, **kwargs):
        self.prefix = None
        self.routes = {
            'GET': {
                 # PATH: (public?, template, func, title),
            }}

        # Monkey patch in our route handling, with fallback
        self.rpc_default_methods = self.get_default_methods
        self.get_default_methods = self.web_default_methods

    async def init_webkitten(self, package, prefix, routes):
        import locale
        locale.setlocale(locale.LC_TIME, "C")

        self.package = package
        self.prefix = prefix
        self.static_loader = None
        self.jinja = None

        for method in routes:
            if method not in self.routes:
                self.routes[method] = {}
            self.routes[method].update(routes[method])

    def common_jinja_vars(self, request_info):
        """
        """
        return {}

    def web_default_methods(self, request_info):
        if request_info.path.startswith('/static'):
            return (self._web_static, None)

        if self.prefix and request_info.path.startswith(self.prefix):
            return (self._web_handle, None)

        return self.old_default_methods(request_info)

    def _jinja(self):
        if self.jinja is None:
            from jinja2 import Environment, select_autoescape
            from jinja2 import PackageLoader, ChoiceLoader
            self.jinja = Environment(
                cache_size=0,  # FIXME: Set to -1 to just cache everything
                enable_async=True,
                loader=ChoiceLoader([
                    PackageLoader(self.package, 'templates'),
                    PackageLoader('kettlingar', 'templates'),
                ]),
                autoescape=select_autoescape())
        return self.jinja

    def _static_loader(self):
        if self.static_loader is None:
            from jinja2 import PackageLoader
            self.static_loader = PackageLoader(self.package, 'resources')
        return self.static_loader

    async def _web_handle(self, req):
        try:
            parts = req.path.split('?', 1)
            path = parts.pop(0)
            qs = parts.pop(0) if parts else ''

            tft, args = self._route(req.method, path, req.authed)
            template, func, title = tft
            if isinstance(template, str):
                template = self._jinja().get_template(template)

            # FIXME: qs -> kwargs
            variables = self.common_jinja_vars(req)
            variables.update({
                'title': title,
                'partial': 'htmx=1' in qs,
                'url': self.api_url + self.prefix})

            if inspect.isasyncgenfunction(func):
                variables['results'] = results = []
                async for mimetype, result in func(req, *args):
                    results.append(result)
            else:
                variables.update((await func(req, *args))[1])

            return 'text/html', await template.render_async(variables)
        except Exception as e:
            self.exception('Error handling %s' % req.path)
            raise

    async def _web_static(self, request_info):
        resource = request_info.path[8:]
        if '/.' in request_info.path or not resource:
            raise PermissionError('Invalid path')

        mimetype = (
            self.STATIC_MIMETYPES.get(os.path.basename(resource)) or
            self.STATIC_MIMETYPES.get(os.path.splitext(resource)[-1]) or
            'application/octet-stream')

        mimetype += '\nCache-Control: public, max-age=360000, immutable'
        mimetype += '\nLast-Modified: ' + self._today()

        error = 'Not Found'
        try:
            data, _, _ = self._static_loader().get_source(self._jinja(), resource)
            while data:
                chunk, data = data[:16*1024], data[16*1024:]
                yield mimetype, bytes(chunk, 'utf-8')
            return
        except OSError as e:
            error = str(e)

        yield None, {'error': error, 'finished': True}

    def _today(self):
        return datetime.datetime.now().strftime('%a, %d %b %Y 00:00:00 GMT')

    def _route(self, method, path, authed):
        if not path.startswith(self.prefix):
            raise KeyError('Route not found: "%s %s"' % (method, path))
        path = path[len(self.prefix):] or '/'

        rpath, args = path, []
        while True:
            try:
                is_public, tpl, func, title = self.routes[method][rpath]
                if is_public or authed:
                    return ((tpl, func, title), args)
            except KeyError:
                pass
            try:
                rpath, arg = rpath.rsplit('/', 1)
                args[:0] = [arg]
            except ValueError:
                raise KeyError('Route not found: "%s %s"' % (method, path))


class WebKittenWithHelp(WebKitten):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.routes['GET']['/help'] = (
            False, 'help.jinja', self.web_help, 'Help')

    async def web_help(self, request_info, *args, **kwargs):
        mt, result = await self.api_help(request_info, *args, **kwargs)
        return mt, {'text': result}
