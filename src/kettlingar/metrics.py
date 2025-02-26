import random

class RPCKittenVarz:
    """
    Work in progress - This class will be a mix-in for RPC Kittens
    that adds metrics/stats.
    """
    METRICS_MAX_SAMPLES = 25
    METRICS_PRUNE_OLDEST = 0
    METRICS_PRUNE_RANDOM = 1

    def __init__(self, **kwargs):
        super().__init__(**kwargs)  # Required for cooperative inheritance

    def _prune_oldest_samples(self, vals, max_samples):
        max_samples = max_samples or self.METRICS_MAX_SAMPLES
        while len(vals) >= max_samples:
            vals.pop(0)

    def _prune_samples_at_random(self, vals, max_samples):
        max_samples = max_samples or self.METRICS_MAX_SAMPLES
        while len(vals) >= max_samples:
            vals.pop(random.randint(0, len(vals)))

    def _get_stats(self, public):
        sname = 'stats_%s' % ('public' if public else 'private')
        stats = getattr(self, sname, {})
        setattr(self, sname, stats)
        return stats

    def metrics_count(self, key, cnt=1, public=False):
        stats = self._get_stats(public)
        stats[key] = stats.get(key, 0) + cnt

    def metrics_sample(self, key, val,
            max_samples=None,
            prune=METRICS_PRUNE_RANDOM,
            public=False):
        stats = self._get_stats(public)
        vals = stats[key] = stats.get(key, [])

        if prune == self.METRICS_PRUNE_RANDOM:
            self._prune_samples_at_random(vals, max_samples)
        else:
            self._prune_oldest_samples(vals, max_samples)
        vals.append(val)

    def metrics_http_request(self, method, path, code, sent, elapsed, peer):
        self.metrics_count('http_%d' % code, public=True)
        if sent:
            self.metrics_count('http_sent_bytes', sent, public=True)
        if elapsed:
            self.metrics_sample('http_elapsed_us', elapsed, public=True)

    async def public_api_varz(self, request_info):
        """/varz

        Return the current internal metrics.
        """
        varz = {}
        if hasattr(self, 'stats_public'):
            varz.update(self.stats_public)
        if hasattr(self, 'stats_private') and request_info.authed:
            varz.update(self.stats_private)
        return None, varz

