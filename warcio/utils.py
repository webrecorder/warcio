import six
from contextlib import contextmanager

try:
    import collections.abc as collections_abc  # only works on python 3.3+
except ImportError:  #pragma: no cover
    import collections as collections_abc

BUFF_SIZE = 16384


# #===========================================================================
def to_native_str(value, encoding='utf-8'):
    if isinstance(value, str):
        return value

    if six.PY3 and isinstance(value, six.binary_type):  #pragma: no cover
        return value.decode(encoding)
    elif six.PY2 and isinstance(value, six.text_type):  #pragma: no cover
        return value.encode(encoding)


# #===========================================================================
@contextmanager
def open_or_default(filename, mod, default_fh):
    if filename and isinstance(filename, str):
        res = open(filename, mod)
        yield res
        res.close()
    elif filename:
        yield filename
    else:
        yield default_fh


# #===========================================================================
def headers_to_str_headers(headers):
    '''
    Converts dict or tuple-based headers of bytes or str to
    tuple-based headers of str, which is the python norm (pep 3333)
    '''
    ret = []

    if isinstance(headers, collections_abc.Mapping):
        h = headers.items()
    else:
        h = headers

    if six.PY2:  #pragma: no cover
        return h

    for tup in h:
        k, v = tup
        if isinstance(k, six.binary_type):
            k = k.decode('iso-8859-1')
        if isinstance(v, six.binary_type):
            v = v.decode('iso-8859-1')
        ret.append((k, v))
    return ret
