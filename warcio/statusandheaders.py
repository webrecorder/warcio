"""
Representation and parsing of HTTP-style status + headers
"""

from six.moves import range
from six import iteritems
from warcio.utils import to_native_str, headers_to_str_headers
import uuid

from six.moves.urllib.parse import quote
import re


#=================================================================
class StatusAndHeaders(object):
    ENCODE_HEADER_RX = re.compile(r'[=]["\']?([^;"]+)["\']?(?=[;]?)')
    """
    Representation of parsed http-style status line and headers
    Status Line if first line of request/response
    Headers is a list of (name, value) tuples
    An optional protocol which appears on first line may be specified
    If is_http_request is true, split http verb (instead of protocol) from start of statusline
    """
    def __init__(self, statusline, headers, protocol='', total_len=0, is_http_request=False):
        if is_http_request:
            protocol, statusline = statusline.split(' ', 1)

        self.statusline = statusline
        self.headers = headers_to_str_headers(headers)
        self.protocol = protocol
        self.total_len = total_len
        self.headers_buff = None

    def get_header(self, name, default_value=None):
        """
        return header (name, value)
        if found
        """
        name_lower = name.lower()
        for value in self.headers:
            if value[0].lower() == name_lower:
                return value[1]

        return default_value

    def add_header(self, name, value):
        self.headers.append((name, value))

    def replace_header(self, name, value):
        """
        replace header with new value or add new header
        return old header value, if any
        """
        name_lower = name.lower()
        for index in range(len(self.headers) - 1, -1, -1):
            curr_name, curr_value = self.headers[index]
            if curr_name.lower() == name_lower:
                self.headers[index] = (curr_name, value)
                return curr_value

        self.headers.append((name, value))
        return None

    def remove_header(self, name):
        """
        Remove header (case-insensitive)
        return True if header removed, False otherwise
        """
        name_lower = name.lower()
        for index in range(len(self.headers) - 1, -1, -1):
            if self.headers[index][0].lower() == name_lower:
                del self.headers[index]
                return True

        return False

    def get_statuscode(self):
        """
        Return the statuscode part of the status response line
        (Assumes no protocol in the statusline)
        """
        code = self.statusline.split(' ', 1)[0]
        return code

    def validate_statusline(self, valid_statusline):
        """
        Check that the statusline is valid, eg. starts with a numeric
        code. If not, replace with passed in valid_statusline
        """
        code = self.get_statuscode()
        try:
            code = int(code)
            assert(code > 0)
            return True
        except(ValueError, AssertionError):
            self.statusline = valid_statusline
            return False

    def add_range(self, start, part_len, total_len):
        """
        Add range headers indicating that this a partial response
        """
        content_range = 'bytes {0}-{1}/{2}'.format(start,
                                                   start + part_len - 1,
                                                   total_len)

        self.statusline = '206 Partial Content'
        self.replace_header('Content-Range', content_range)
        self.replace_header('Content-Length', str(part_len))
        self.replace_header('Accept-Ranges', 'bytes')
        return self

    def compute_headers_buffer(self, header_filter=None):
        """
        Set buffer representing headers
        """
        # HTTP headers %-encoded as ascii (see to_ascii_bytes for more info)
        self.headers_buff = self.to_ascii_bytes(header_filter)

    def __repr__(self):
        return "StatusAndHeaders(protocol = '{0}', statusline = '{1}', \
headers = {2})".format(self.protocol, self.statusline, self.headers)

    def __ne__(self, other):
        return not (self == other)

    def __eq__(self, other):
        if not other:
            return False

        return (self.statusline == other.statusline and
                self.headers == other.headers and
                self.protocol == other.protocol)

    def __str__(self, exclude_list=None):
        return self.to_str(exclude_list)

    def __bool__(self):
        return bool(self.statusline or self.headers)

    __nonzero__ = __bool__

    def to_str(self, filter_func=None):
        string = self.protocol

        if string and self.statusline:
            string += ' '

        if self.statusline:
            string += self.statusline

        if string:
            string += '\r\n'

        for h in self.headers:
            if filter_func:
                h = filter_func(h)
                if not h:
                    continue

            string += ': '.join(h) + '\r\n'

        return string

    def to_bytes(self, filter_func=None, encoding='utf-8'):
        return self.to_str(filter_func).encode(encoding) + b'\r\n'

    def to_ascii_bytes(self, filter_func=None):
        """ Attempt to encode the headers block as ascii
            If encoding fails, call percent_encode_non_ascii_headers()
            to encode any headers per RFCs
        """
        try:
            string = self.to_str(filter_func)
            string = string.encode('ascii')
        except (UnicodeEncodeError, UnicodeDecodeError):
            self.percent_encode_non_ascii_headers()
            string = self.to_str(filter_func)
            string = string.encode('ascii')

        return string + b'\r\n'

    def percent_encode_non_ascii_headers(self, encoding='UTF-8'):
        """ Encode any headers that are not plain ascii
            as UTF-8 as per:
            https://tools.ietf.org/html/rfc8187#section-3.2.3
            https://tools.ietf.org/html/rfc5987#section-3.2.2
        """
        def do_encode(m):
            return "*={0}''".format(encoding) + quote(to_native_str(m.group(1)))

        for index in range(len(self.headers) - 1, -1, -1):
            curr_name, curr_value = self.headers[index]
            try:
                # test if header is ascii encodable, no action needed
                curr_value.encode('ascii')
            except:
                # if single value header, (eg. no ';'), %-encode entire header
                if ';' not in curr_value:
                    new_value = quote(curr_value)

                else:
                # %-encode value in ; name="value"
                    new_value = self.ENCODE_HEADER_RX.sub(do_encode, curr_value)
                    if new_value == curr_value:
                        new_value = quote(curr_value)

                self.headers[index] = (curr_name, new_value)

    # act like a (case-insensitive) dictionary of headers, much like other
    # python http headers apis including http.client.HTTPMessage
    # and requests.structures.CaseInsensitiveDict
    get = get_header
    __getitem__ = get_header
    __setitem__ = replace_header
    __delitem__ = remove_header
    def __contains__(self, key):
        return bool(self[key])

#=================================================================
def _strip_count(string, total_read):
    length = len(string)
    return string.rstrip(), total_read + length


#=================================================================
class StatusAndHeadersParser(object):
    """
    Parser which consumes a stream support readline() to read
    status and headers and return a StatusAndHeaders object
    """
    def __init__(self, statuslist, verify=True):
        self.statuslist = statuslist
        self.verify = verify

    def parse(self, stream, full_statusline=None):
        """
        parse stream for status line and headers
        return a StatusAndHeaders object

        support continuation headers starting with space or tab
        """

        # status line w newlines intact
        if full_statusline is None:
            full_statusline = stream.readline()

        full_statusline = self.decode_header(full_statusline)

        statusline, total_read = _strip_count(full_statusline, 0)

        headers = []

        # at end of stream
        if total_read == 0:
            raise EOFError()
        elif not statusline:
            return StatusAndHeaders(statusline=statusline,
                                    headers=headers,
                                    protocol='',
                                    total_len=total_read)

        # validate only if verify is set
        if self.verify:
            protocol_status = self.split_prefix(statusline, self.statuslist)

            if not protocol_status:
                msg = 'Expected Status Line starting with {0} - Found: {1}'
                msg = msg.format(self.statuslist, statusline)
                raise StatusAndHeadersParserException(msg, full_statusline)
        else:
            protocol_status = statusline.split(' ', 1)

        line, total_read = _strip_count(self.decode_header(stream.readline()), total_read)
        while line:
            result = line.split(':', 1)
            if len(result) == 2:
                name = result[0].rstrip(' \t')
                value = result[1].lstrip()
            else:
                name = result[0]
                value = None

            next_line, total_read = _strip_count(self.decode_header(stream.readline()),
                                                 total_read)

            # append continuation lines, if any
            while next_line and next_line.startswith((' ', '\t')):
                if value is not None:
                    value += next_line
                next_line, total_read = _strip_count(self.decode_header(stream.readline()),
                                                     total_read)

            if value is not None:
                header = (name, value)
                headers.append(header)

            line = next_line

        if len(protocol_status) > 1:
            statusline = protocol_status[1].strip()
        else:
            statusline = ''

        return StatusAndHeaders(statusline=statusline,
                                headers=headers,
                                protocol=protocol_status[0],
                                total_len=total_read)

    @staticmethod
    def split_prefix(key, prefixs):
        """
        split key string into prefix and remainder
        for first matching prefix from a list
        """
        key_upper = key.upper()
        for prefix in prefixs:
            if key_upper.startswith(prefix):
                plen = len(prefix)
                return (key_upper[:plen], key[plen:])

    @staticmethod
    def make_warc_id(id_=None):
        if not id_:
            id_ = uuid.uuid4()
        return '<urn:uuid:{0}>'.format(id_)


    @staticmethod
    def decode_header(line):
        try:
            # attempt to decode as utf-8 first
            return to_native_str(line, 'utf-8')
        except:
            # if fails, default to ISO-8859-1
            return to_native_str(line, 'iso-8859-1')


#=================================================================
class StatusAndHeadersParserException(Exception):
    """
    status + headers parsing exception
    """
    def __init__(self, msg, statusline):
        super(StatusAndHeadersParserException, self).__init__(msg)
        self.statusline = statusline
