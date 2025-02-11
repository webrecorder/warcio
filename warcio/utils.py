import os
from contextlib import contextmanager
import base64
import hashlib

import collections.abc

BUFF_SIZE = 16384


# #===========================================================================
def to_native_str(value, encoding='utf-8'):
    if isinstance(value, str):
        return value

    if isinstance(value, bytes):
        return value.decode(encoding)
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

    if isinstance(headers, collections.abc.Mapping):
        h = headers.items()
    else:
        h = headers

    for tup in h:
        k, v = tup
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
