"""
Collect and expose Prometheus-compatible metrics within a running
RPCKitten microservice.
"""
import random


class RPCKittenVarz:
    """
    Work in progress - This class will be a mix-in for RPC Kittens
    that adds metrics/stats.
    """
    METRICS_COUNT = 'count'
    METRICS_GAUGE = 'gauge'
    METRICS_SAMPLE = 'sample'
    METRICS_TYPE_MAP = '_type_map'
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

    def metrics_gauge(self, key, val, public=False):
        """
        Update a gauge metric (like a spedometor, a current numeric value).
        """
        stats = self._get_stats(public)
        stats[key] = val
        stats[self.METRICS_TYPE_MAP][key] = self.METRICS_GAUGE

    def metrics_count(self, key, cnt=1, public=False):
        """
        Update a counting metric, adding 1 (or more).
        """
        stats = self._get_stats(public)
        stats[key] = stats.get(key, 0) + cnt
        stats[self.METRICS_TYPE_MAP][key] = self.METRICS_COUNT

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-positional-arguments
    def metrics_sample(self, key, val,
            max_samples=None,
            prune=METRICS_PRUNE_RANDOM,
            public=False):
        """
        Add a value to a sample metric.
        """
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

            self.metrics_count(pfx + '_%d' % request_info.code, public=pub)

            via = '_unix' if request_info.via_unix_domain else '_tcp'
            self.metrics_count(pfx + via, public=False)

            if request_info.is_generator and (pfx == 'http'):
                pfx = 'http_gen'

            if sent:
                self.metrics_count(pfx + '_sent_bytes', sent, public=pub)
            if elapsed_us:
                self.metrics_sample(pfx + '_elapsed_us', elapsed_us, public=pub)

    async def public_api_varz(self, request_info):
        """/varz

        Return the current internal metrics.
        """
        varz = {}
        if hasattr(self, 'start_time'):
            self.metrics_gauge('start_time', self.start_time)

        varz.update(self.stats_public)

        if request_info.authed:
            varz.update(self.stats_private)
            varz[self.METRICS_TYPE_MAP].update(
                self.stats_public[self.METRICS_TYPE_MAP])

        return varz
