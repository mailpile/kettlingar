import random

class RPCKittenVarz:
    """
    Work in progress - This class will be a mix-in for RPC Kittens
    that adds metrics/stats.
    """
    METRICS_COUNT = 'count'
    METRICS_GUAGE = 'guage'
    METRICS_SAMPLE = 'sample'
    METRICS_TYPE_MAP = '_type_map'
    METRICS_MAX_SAMPLES = 25
    METRICS_PRUNE_OLDEST = 0
    METRICS_PRUNE_RANDOM = 1

    def __init__(self, **kwargs):
        super().__init__(**kwargs)  # Required for cooperative inheritance
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

    def metrics_guage(self, key, val, public=False):
        """
        Update a guage metric (like a spedometor, a current numeric value).
        """
        stats = self._get_stats(public)
        stats[key] = stats.get(key, val)
        stats[self.METRICS_TYPE_MAP][key] = self.METRICS_GUAGE

    def metrics_count(self, key, cnt=1, public=False):
        """
        Update a counting metric, adding 1 (or more).
        """
        stats = self._get_stats(public)
        stats[key] = stats.get(key, 0) + cnt
        stats[self.METRICS_TYPE_MAP][key] = self.METRICS_COUNT

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
        for prefix in ('http', request_info.handler):
            self.metrics_count(prefix + '_%d' % request_info.code,
                public=(prefix == 'http'))

            via = '_unix' if request_info.via_unix_domain else '_tcp'
            self.metrics_count(prefix + via, public=False)

            if sent:
                self.metrics_count(prefix + '_sent_bytes', sent,
                    public=(prefix == 'http'))
            if elapsed_us:
                self.metrics_sample(prefix + '_elapsed_us', elapsed_us,
                    public=(prefix == 'http'))

    async def public_api_varz(self, request_info):
        """/varz

        Return the current internal metrics.
        """
        varz = {}
        if self.start_time:
            self.metrics_guage('start_time', self.start_time)
        varz.update(self.stats_public)

        if request_info.authed:
            varz.update(self.stats_private)
            varz[self.METRICS_TYPE_MAP].update(
                self.stats_public[self.METRICS_TYPE_MAP])
        return None, varz

