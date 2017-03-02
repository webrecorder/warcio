import six


BUFF_SIZE = 16384


# #===========================================================================
def to_native_str(value, encoding='iso-8859-1', func=lambda x: x):
    if isinstance(value, str):
        return value

    if six.PY3 and isinstance(value, six.binary_type):  #pragma: no cover
        return func(value.decode(encoding))
    elif six.PY2 and isinstance(value, six.text_type):  #pragma: no cover
        return func(value.encode(encoding))


