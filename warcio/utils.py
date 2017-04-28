import six
from contextlib import contextmanager

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
def open_or_default(filename, mod, default_fh):  #pragma: no cover
    if filename:
        res = open(filename, mod)
        yield res
        res.close()
    else:
        yield default_fh
