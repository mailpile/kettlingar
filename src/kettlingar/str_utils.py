"""
String manipulation helper functions.
"""
import binascii


#pylint: disable=consider-using-f-string


def str_addr(addr):
    """Format internet addresses as strings"""
    if isinstance(addr, (tuple, list)):
        if (len(addr) == 3) and isinstance(addr[2], int):
            return '%s/[%s]:%s' % tuple(addr[:3])
        return '[%s]:%s' % tuple(addr[:2])
    return str(addr)


def str_route_map_key(route_map_key):
    #pylint: disable=missing-function-docstring
    # FIXME: This may not belong here!
    return str(binascii.b2a_base64(route_map_key), 'utf-8').strip()


def str_args(args):
    """
    Render a list of arguments as a string, for logging purposes.
    """

    def _trunc(a, l):
        return (a[:l-2] + '..') if (len(a) > l) else a

    def _fmt(a):
        # pylint: disable=too-many-return-statements
        if isinstance(a, str):
            return "'%s'" % _trunc(a, 16)
        if isinstance(a, int):
            return '%d' % a
        if isinstance(a, float):
            return '%.3f' % a
        if isinstance(a, (bytes, bytearray)):
            rv = str(binascii.b2a_base64(a), 'utf-8').strip()
            if len(rv) > 30:
                rv = '%s..%d' % (rv[:25], len(a))
            return rv
        if isinstance(a, dict):
            return '<dict(%d)>' % len(a)
        if isinstance(a, list):
            return '<list(%d)>' % len(a)

        return '<%s>' % a.__class__.__name__

    return ', '.join(_fmt(a) for a in args)
