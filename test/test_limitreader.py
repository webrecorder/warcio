from warcio.limitreader import LimitReader
from contextlib import closing

from io import BytesIO
import pytest

class TestLimitReader(object):
    def test_limit_reader_1(self):
        assert b'abcdefghji' == LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 10).read(26)

    def test_limit_reader_2(self):
        assert b'abcdefgh' == LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 8).readline(26)

    def test_limit_reader_3(self):
        reader = LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 8)
        new_reader = LimitReader.wrap_stream(reader, 4)
        assert reader == new_reader
        assert b'abcd' == new_reader.readline(26)
        #assert b'abcd' == LimitReader.wrap_stream(LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 8), 4).readline(26)

    def test_limit_reader_multiple_read(self):
        reader = LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 10)
        string = None
        for x in [2, 2, 20]:
            string = reader.read(x)

        assert b'efghji' == string

    def test_limit_reader_zero(self):
        assert b'' == LimitReader(BytesIO(b'a'), 0).readline(0)

    def test_limit_reader_invalid_wrap(self):
        b = BytesIO(b'some data')
        assert LimitReader.wrap_stream(b, 'abc') == b

    def test_limit_reader_close(self):
        reader = LimitReader(BytesIO(b'abcdefg'), 3)
        with closing(reader):
            assert b'abc' == reader.read(10)
            assert reader.tell() == 3

    @pytest.mark.parametrize('method', ['read', 'readline'])
    @pytest.mark.parametrize('args', [(), (None,), (-1,), (-2,)])
    def test_unbounded_read_preserves_limit(self, method, args):
        stream = BytesIO(b'bodyNEXT_RECORD\n')
        reader = LimitReader(stream, 4)
        assert reader.read(1) == b'b'

        read = getattr(reader, method)
        assert read(*args) == b'ody'
        assert reader.tell() == 4
        assert reader.limit == 0
        assert read(*args) == b''
        assert stream.read() == b'NEXT_RECORD\n'

    @pytest.mark.parametrize('method', ['read', 'readline'])
    def test_zero_and_positive_read_sizes(self, method):
        stream = BytesIO(b'bodyNEXT_RECORD\n')
        reader = LimitReader(stream, 4)
        read = getattr(reader, method)

        assert read(0) == b''
        assert stream.tell() == 0
        assert reader.tell() == 0
        assert read(1) == b'b'
        assert read(10) == b'ody'
        assert read(1) == b''
        assert stream.read() == b'NEXT_RECORD\n'

    def test_negative_readline_stops_at_newline(self):
        stream = BytesIO(b'one\ntwoNEXT_RECORD\n')
        reader = LimitReader(stream, 7)

        assert reader.readline(-1) == b'one\n'
        assert reader.tell() == 4
        assert reader.readline(-2) == b'two'
        assert reader.readline(-1) == b''
        assert stream.read() == b'NEXT_RECORD\n'
