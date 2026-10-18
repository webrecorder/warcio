import builtins
from contextlib import contextmanager
import base64
import hashlib

try:
    import collections.abc as collections_abc  # only works on python 3.3+
except ImportError:  #pragma: no cover
    import collections as collections_abc

try:
    from fsspec import open as _fsspec_open
    HAS_FSSPEC = True
except ImportError:
    HAS_FSSPEC = False


BUFF_SIZE = 16384


# #===========================================================================
def to_native_str(value, encoding='utf-8'):
    if isinstance(value, bytes):  # pragma: no cover
        return value.decode(encoding)
    return value


# #===========================================================================
@contextmanager
def fsspec_open(filename, mod, default_fh=None, **kwargs):
    """
    Open a file using fsspec if available, otherwise use built-in open.
    """
    if filename == '-' or filename == b'-':
        yield default_fh
    elif filename and isinstance(filename, str):
        if HAS_FSSPEC:
            with _fsspec_open(filename, mode=mod, **kwargs) as f:
                yield f
        else:
            builtin_kwargs = {k: v for k, v in kwargs.items() 
                            if k in ['buffering', 'encoding', 'errors', 'newline']}
            with open(filename, mode=mod, **builtin_kwargs) as f:
                yield f

    elif filename:
        yield filename
    else:
        yield default_fh


# #===========================================================================
@contextmanager
def open_or_default(filename, mod, default_fh):
    """
    Alias for fsspec_open, which replaced this method, for backwards
    compatibility
    """
    with fsspec_open(filename, mod, default_fh) as f:
        yield f


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

    for k, v in h:
        if isinstance(k, bytes):
            k = k.decode('iso-8859-1')
        if isinstance(v, bytes):
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
open = builtins.open  # Compatibility for code that uses warcio.utils.open
