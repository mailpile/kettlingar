"""
Collect and expose Prometheus-compatible metrics within a running
RPCKitten microservice.
"""
import random

from kettlingar import HttpResult


class MetricsKitten:
    """
    Work in progress - This class will be a mix-in for RPC Kittens
    that adds metrics/stats.
    """
    METRICS_INFO = 'info'
    METRICS_COUNT = 'count'
    METRICS_GAUGE = 'gauge'
    METRICS_SAMPLE = 'sample'
    METRICS_TYPE_MAP = '_type_map'
    METRICS_MIN_SAMPLES = 5
    METRICS_MAX_SAMPLES = 25
    METRICS_PRUNE_OLDEST = 0
    METRICS_PRUNE_RANDOM = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # Req'd for cooperative inheritance

        self.stats_public = {self.METRICS_TYPE_MAP: {}}
        self.stats_private = {self.METRICS_TYPE_MAP: {}}

    def _prune_oldest_samples(self, vals, max_samples):
        max_samples = max_samples or self.METRICS_MAX_SAMPLES
        while len(vals) >= max_samples:
            vals.pop(0)

    def _prune_samples_at_random(self, vals, max_samples):
        max_samples = max_samples or self.METRICS_MAX_SAMPLES
        while len(vals) >= max_samples:
            vals.pop(random.randint(0, len(vals) - 1))

    def _get_stats(self, public):
        return self.stats_public if public else self.stats_private

    def _key_with_labels(self, key, labels):
        if not labels:
            return key
        return key + ',' + ','.join(
            '='.join((
                str(k).replace('=', '').replace(',', ''),
                ('"%s"' % v).replace(',', '')))
            for k, v in labels.items())

    def metrics_info(self, key, val, public=False, **labels):
        """
        Update an info metric (arbitrary text).
        """
        key = self._key_with_labels(key, labels)
        stats = self._get_stats(public)
        stats[key] = val
        stats[self.METRICS_TYPE_MAP][key] = self.METRICS_INFO

    def metrics_gauge(self, key, val, public=False, **labels):
        """
        Update a gauge metric (like a spedometor, a current numeric value).
        """
        key = self._key_with_labels(key, labels)
        stats = self._get_stats(public)
        stats[key] = val
        stats[self.METRICS_TYPE_MAP][key] = self.METRICS_GAUGE

    def metrics_count(self, key, cnt=1, public=False, **labels):
        """
        Update a counting metric, adding 1 (or more).
        """
        key = self._key_with_labels(key, labels)
        stats = self._get_stats(public)
        stats[key] = stats.get(key, 0) + cnt
        stats[self.METRICS_TYPE_MAP][key] = self.METRICS_COUNT

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-positional-arguments
    def metrics_sample(self, key, val,
            max_samples=None,
            prune=METRICS_PRUNE_RANDOM,
            public=False,
            **labels):
        """
        Add a value to a sample metric.
        """
        key = self._key_with_labels(key, labels)
        stats = self._get_stats(public)
        vals = stats[key] = stats.get(key, [])
        stats[self.METRICS_TYPE_MAP][key] = 'sample'

        if prune == self.METRICS_PRUNE_RANDOM:
            self._prune_samples_at_random(vals, max_samples)
        else:
            self._prune_oldest_samples(vals, max_samples)
        vals.append(val)

    def metrics_http_request(self, request_info, elapsed_us=None, sent=None):
        """
        Record metrics for an HTTP request.
        """
        sent = sent or request_info.sent

        for pfx in ('http', request_info.handler):
            if not pfx:
                break
            pub = (pfx == 'http')

            self.metrics_count(pfx, public=pub, code=request_info.code)

            via = '_unix' if request_info.via_unix_domain else '_tcp'
            self.metrics_count(pfx + via, public=False)

            if request_info.is_generator and (pfx == 'http'):
                pfx = 'http_gen'

            if sent:
                self.metrics_count(pfx + '_sent_bytes', sent, public=pub)
            if elapsed_us:
                self.metrics_sample(pfx + '_elapsed_us', elapsed_us, public=pub)

    def _format_openmetrics(self, varz):
        type_map = varz.pop(self.METRICS_TYPE_MAP)
        output = []
        emit = output.append
        for var in sorted(varz.keys()):
            if ',' in var:
                vl = var.split(',')
                vn = vl.pop(0)
                vl = '{%s}' % ', '.join(vl)
            else:
                vn, vl = var, ''

            val = varz[var]
            dtype = type_map.get(var, self.METRICS_GAUGE)

            if isinstance(val, str):
                if dtype == 'info' and not vn.endswith('_info'):
                    vn += '_info'

                emit('# TYPE %s %s' % (vn, dtype))
                vl = (vl[1:-1] + ', ') if vl else vl
                val = val.replace('"', "'")
                if ' ' in val:
                    v1, v2 = val.split(' ', 1)
                    emit('%s{%s%s="%s", details="%s"} 1' % (vn, vl, vn, v1, v2))
                else:
                    emit('%s{%s%s="%s"} 1' % (vn, vl, vn, val))

            elif isinstance(val, bool):
                emit('# TYPE %s %s' % (vn, dtype))
                emit('%s%s %s' % (vn, vl, val and 1 or 0))

            elif isinstance(val, list):
                if len(val) >= self.METRICS_MIN_SAMPLES:
                    slist = sorted([float(v) for v in val])
                    emit('%s_m50%s %.2f' % (vn, vl, slist[int(len(slist) * 0.5)]))
                    emit('%s_m90%s %.2f' % (vn, vl, slist[int(len(slist) * 0.9)]))
                    emit('%s_avg%s %.2f' % (vn, vl, sum(slist) / len(slist)))

            else:
                emit('# TYPE %s %s' % (vn, dtype))
                emit('%s%s %s' % (vn, vl, val))

        emit('# EOF\n')
        return '\n'.join(output)

    async def public_api_metrics(self, request_info, openmetrics:bool=False):
        """/metrics

        Return the current internal metrics. Authenticated requests will
        see private metrics, otherwise we only expose the public ones.
        """
        varz = {}
        if hasattr(self, 'start_time'):
            self.metrics_gauge('start_time', self.start_time,
                app=self.config.app_name,
                worker=self.config.worker_name)

        varz.update(self.stats_public)

        if request_info.authed:
            varz.update(self.stats_private)
            varz[self.METRICS_TYPE_MAP].update(
                self.stats_public[self.METRICS_TYPE_MAP])

        if not openmetrics:
            return varz

        return HttpResult(
            'application/openmetrics-text; version=1.0.0; charset=utf-8',
            self._format_openmetrics(varz))
