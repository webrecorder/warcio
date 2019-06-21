import six
import os
from contextlib import contextmanager
import base64
import hashlib

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
    else:
        return value


# #===========================================================================
@contextmanager
def open_or_default(filename, mod, default_fh):
    if filename == '-' or filename == b'-':
        yield default_fh
    elif filename and isinstance(filename, str):
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


# ============================================================================
class Digester(object):
    def __init__(self, type_='sha1'):
        self.type_ = type_
        self.digester = hashlib.new(type_)

    def update(self, buff):
        self.digester.update(buff)

    def __str__(self):
        return self.type_ + ':' + to_native_str(base64.b32encode(self.digester.digest()))


#=============================================================================
sys_open = open

def open(filename, mode='r', **kwargs):  #pragma: no cover
    """
    open() which supports exclusive mode 'x' in python < 3.3
    """
    if six.PY3 or 'x' not in mode:
        return sys_open(filename, mode, **kwargs)

    flags = os.O_EXCL | os.O_CREAT | os.O_WRONLY
    if 'b' in mode and hasattr(os, 'O_BINARY'):
        flags |= os.O_BINARY

    fd = os.open(filename, flags)
    mode = mode.replace('x', 'w')
    return os.fdopen(fd, mode, 0x664)



