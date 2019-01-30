from __future__ import print_function

import re
import sys
import six
from collections import defaultdict

from warcio.archiveiterator import WARCIterator
from warcio.utils import to_native_str, Digester
from warcio.exceptions import ArchiveLoadFailed


class Commentary(object):
    def __init__(self, record_id=None, rec_type=None):
        self._record_id = record_id
        self._rec_type = rec_type
        self.errors = []
        self.recommendations = []
        self._comments = []

    def record_id(self):
        return self._record_id

    def rec_type(self):
        return self._rec_type

    def error(self, *args):
        self.errors.append(args)

    def recommendation(self, *args):
        self.recommendations.append(args)

    def comment(self, *args):
        self._comments.append(args)

    def has_comments(self):
        if self.errors or self.recommendations or self._comments:
            return True

    def comments(self):
        # XXX str() all of these, in case an int or other thing slips in?
        for e in self.errors:
            yield 'error: ' + ' '.join(e)
        for r in self.recommendations:
            yield 'recommendation: ' + ' '.join(r)
        for c in self._comments:
            yield 'comment: ' + ' '.join(c)


class WrapRecord(object):
    def __init__(self, obj):
        self.obj = obj
        self._content = None

    def __getattr__(self, name):
        if name == 'content':
            if self._content is None:
                self._content = self.obj.content_stream().read()
            return self._content
        if name == 'stream_for_digest_check':
            def _doit():
                while True:
                    piece = self.obj.content_stream().read(1024*1024)
                    if len(piece) == 0:
                        break
            return _doit
        return getattr(self.__dict__['obj'], name)


def canon_content_type(s):
    # wget omits the space after the ;, let that pass
    return s.lower().replace(';msgtype=', '; msgtype=')


def validate_warc_fields(record, commentary):
    # warc-fields = *named-field CRLF
    # named-field = field-name ":" [ field-value ]
    # field-value = *( field-content | LWS )  # LWS signals continuations
    # field-name = token  # token_re

    content = record.content

    if six.PY2:  # pragma: no cover
        try:
            content.decode('utf-8', errors='strict')
            text = content  # already a str
        except UnicodeDecodeError as e:
            err = str(e)
            err = err.replace('utf8', 'utf-8')  # sigh
            commentary.error('warc-fields contains invalid utf-8: '+err)
            text = content.decode('utf-8', errors='replace')
    else:  # pragma: no cover
        try:
            text = to_native_str(content, 'utf-8', errors='strict')
        except UnicodeDecodeError as e:
            commentary.error('warc-fields contains invalid utf-8: '+str(e))
            text = to_native_str(content, 'utf-8', errors='replace')

    first_line = True
    lines = []
    for line in text.splitlines(True):
        if not line.endswith('\r\n'):
            commentary.comment('warc-fields lines must end with \\r\\n:', line.rstrip())
            line = line.rstrip('\r\n')
        else:
            line = line[:-2]

        if line.startswith(' ') or line.startswith('\t'):
            if first_line:
                commentary.comment('The first line of warc-fields cannot start with whitespace')
            else:
                lines[-1] += ' ' + line[1:]
        elif line == '':
            # are blank lines prohibited?
            pass
        else:
            # check for field-name :
            if ':' not in line:
                commentary.comment('Missing colon in warc-fields line:', line)
            else:
                field_name = line.split(':', 1)[0]
                if not re.search(token_re, field_name):
                    commentary.comment('Invalid warc-fields name:', field_name)
                else:
                    lines.append(line)
        first_line = False

    if not lines:
        commentary.comment('warc-fields block present but empty')
        return

    # check known fields


def validate_warcinfo(record, commentary, pending):
    content_type = record.rec_headers.get_header('Content-Type', 'none')
    if content_type.lower() != 'application/warc-fields':
        # https://github.com/iipc/warc-specifications/issues/33 -- SHALL BE or recommended?
        commentary.recommendation('warcinfo Content-Type recommended to be application/warc-fields:', content_type)
    else:
        #   format: warc-fields
        #   allowable fields include but not limited to DMCI plus the following
        #   operator, software, robots, hostname, ip, http-header-user-agent, http-header-from
        #     if operator present, recommended name or name and email address
        #     comment if http-user-agent here and in the request or metadata record?
        #     comment if http-header-from here and in the request?
        validate_warc_fields(record, commentary)

    # whole-file tests:
    # recommended that all files start with warcinfo
    # elsewise allowable for warcinfo to appear anywhere


def validate_response(record, commentary, pending):
    target_uri = record.rec_headers.get_header('WARC-Target-URI', 'none').lower()

    if target_uri.startswith('http:') or target_uri.startswith('https:'):
        content_type = record.rec_headers.get_header('Content-Type', 'none')
        if canon_content_type(content_type) not in {'application/http; msgtype=response', 'application/http'}:
            commentary.error('responses for http/https should have Content-Type of application/http; msgtype=response or application/http:', content_type)

        if record.rec_headers.get_header('WARC-IP-Address') is None:
            commentary.error('WARC-IP-Address should be used for http and https responses')

        if not record.http_headers:
            commentary.error('http/https responses should have http headers')
            return

        http_content_length = record.http_headers.get_header('Content-Length')
        if http_content_length is None:
            return

        if not http_content_length.isdigit():
            commentary.comment('http content length header is not an integer', str(http_content_length))
            return

        # We want to verify http_content_length, which is the size of the compressed payload
        # Trying to catch that commoncrawl nutch bug that prefixed /r/n to the payload without changing http content-length

        # this blecherous hack is because we need the length of the (possibly compressed) raw stream
        # without reading any of it (so that it can be read elsewhere to check the payload digest)

        # XXX fix me before shipping :-D

        if hasattr(record, 'raw_stream'):
            if hasattr(record.raw_stream, 'stream'):
                if hasattr(record.raw_stream.stream, 'limit'):
                    if int(http_content_length) != record.raw_stream.stream.limit:
                        commentary.comment('Actual http payload length is different from http header Content-Length:',
                                           str(record.raw_stream.stream.limit), http_content_length)


def validate_resource(record, commentary, pending):
    target_uri = record.rec_headers.get_header('WARC-Target-URI', '').lower()

    if target_uri.startswith('dns:'):
        content_type = record.rec_headers.get_header('Content-Type', 'none')
        if content_type.lower() != 'text/dns':
            commentary.error('resource records for dns shall have Content-Type of text/dns:', content_type)
        else:
            # rfc 2540 and rfc 1035
            #validate_text_dns()
            pass

    # should never have http headers
    #   heuristic of looking for an http status line? and then a blank line?!


def validate_request(record, commentary, pending):
    target_uri = record.rec_headers.get_header('WARC-Target-URI', 'none').lower()

    if target_uri.startswith('http:') or target_uri.startswith('https:'):
        content_type = record.rec_headers.get_header('Content-Type')

        if canon_content_type(content_type) not in {'application/http; msgtype=request', 'application/http'}:
            commentary.error('requests for http/https should have Content-Type of application/http; msgtype=request or application/http:', content_type)

        if record.rec_headers.get_header('WARC-IP-Address') is None:
            commentary.error('WARC-IP-Address should be used for http and https requests')

        # error: http and https schemes should have http request headers

        # WARC-Concurrent-To field or fields may be used, comment if present but target record is not


def validate_metadata(record, commentary, pending):
    content_type = record.rec_headers.get_header('Content-Type', 'none')
    if content_type.lower() == 'application/warc-fields':
        # https://github.com/iipc/warc-specifications/issues/33 SHALL be or not?
        #
        # dublin core plus via, hopsFromSeed, fetchTimeMs -- w1.1 section 6
        # via: uri -- example in Warc 1.1 section 10.5 does not have <> around it
        # hopsFromSeed: string
        # fetchTimeMs: time in milliseconds, so it's an integer?
        validate_warc_fields(record, commentary)


def validate_revisit(record, commentary, pending):
    warc_profile = record.rec_headers.get_header('WARC-Profile', 'none')

    if warc_profile.endswith('/revisit/identical-payload-digest') or warc_profile.endswith('/revisit/uri-agnostic-identical-payload-digest'):
        config = {
            'required': ['WARC-Payload-Digest'],
            'recommended': ['WARC-Refers-To'],
        }
        if '/1.1/' in warc_profile:
            config['recommended'].extend(('WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date'))

        validate_fields_against_rec_type('revisit', config, record.rec_headers, commentary, allow_all=True)
        # may have record block;
        #  if not, shall have Content-Length: 0,
        #  if yes, should be like a response record, truncated FOR LENGTH ONLY if desired
        #  recommended that server response headers be preserved "in this manner"
        #   I suppose that means headers are required if there is any content?!

    elif warc_profile.endswith('/revisit/server-not-modified'):
        config = {
            'recommended': ['WARC-Refers-To', 'WARC-Refers-To-Date'],
            'prohibited': ['WARC-Payload-Digest'],
        }
        validate_fields_against_rec_type('revisit', config, record.rec_headers, commentary, allow_all=True)
        #   may have content body;
        #     if not, shall have Content-Length: 0,
        #     if yes, should be like a response record, truncated if desired
        #   WARC-Refers-To-Date should be the same as WARC-Date in the original record if present
    else:
        commentary.comment('no revisit details validation done due to unknown profile:', warc_profile)


def validate_conversion(record, commentary, pending):
    # where practical, have a warc-refers-to field -- not quite a recommendation, perhaps make it a comment?
    # suggests there should be a corresponding metadata record -- which may have a WARC-Refers-To
    pass


def validate_continuation(record, commentary, pending):
    commentary.comment('warcio test continuation code has not been tested, expect bugs')

    segment_number = record.rec_headers.get_header('WARC-Segment-Number', 'none')
    if segment_number.isdigit() and int(segment_number) < 2:
        commentary.error('continuation record must have WARC-Segment-Number > 1:', segment_number)

    # last segment: required WARC-Segment-Total-Length, optional WARC-Truncated


def validate_unbracketed_uri(field, value, record, version, commentary, pending):
    # uri per RFC 3986
    # should use a registered scheme
    # %XX encoding, normalize to upper case
    # schemes are case-insensitive and normalize to lower
    if value.startswith('<') or value.endswith('>'):
        # wget 1.19 bug caused by WARC 1.0 spec error
        commentary.error('uri must not be within <>:', field, value)
    if ':' not in value:
        commentary.error('invalid uri, no scheme:', field, value)
    if re.search(r'\s', value):
        commentary.error('invalid uri, contains whitespace:', field, value)
    scheme = value.split(':', 1)[0]
    if not re.search(r'\A[A-Za-z][A-Za-z0-9+\-\.]*\Z', scheme):
        commentary.error('invalid uri scheme, bad character:', field, value)
    # https://www.iana.org/assignments/uri-schemes/uri-schemes.xhtml


def validate_warc_type(field, value, record, version, commentary, pending):
    if not value.islower():
        # I am unclear if this is allowed? standard is silent
        commentary.comment('WARC-Type is not lower-case:', field, value)
    if value.lower() not in record_types:
        # standard says readers should ignore unknown warc-types
        commentary.comment('unknown WARC-Type:', field, value)


def validate_bracketed_uri(field, value, record, version, commentary, pending):
    # < uri >
    if not (value.startswith('<') and value.endswith('>')):
        commentary.error('uri must be within <>:', field, value)
        return
    validate_unbracketed_uri(field, value[1:-1], record, version, commentary, pending)


def validate_record_id(field, value, record, version, commentary, pending):
    validate_bracketed_uri(field, value, record, version, commentary, pending)


def validate_timestamp(field, value, record, version, commentary, pending):
    ISO_RE = r'\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:.\d{1,9})?Z\Z'

    if not re.match(ISO_RE, value):
        commentary.error('Invalid timestamp:', field, value)

    use_ms = False if version <= '1.0' else True
    if not use_ms:
        if '.' in value:
            # specification infelicity: would be nice to have 'advice to implementers' here
            commentary.error('WARC versions <= 1.0 may not have timestamps with fractional seconds:', field, value)


def validate_content_length(field, value, record, version, commentary, pending):
    if not value.isdigit():
        commentary.error('must be an integer:', field, value)


token_re = r'\A[!"#$%&\'()*+\-\.0-9A-Z\^_`a-z|~]+\Z'
digest_re = r'\A[A-Za-z0-9/+\-_=]+\Z'


def validate_content_type(field, value, record, version, commentary, pending):
    if '/' not in value:
        commentary.error('must contain a /:', field, value)
    splits = value.split('/', 1)
    ctype = splits[0]
    if len(splits) > 1:
        rest = splits[1]
    else:
        rest = ''
    if not re.search(token_re, ctype):
        commentary.error('invalid type:', field, value)
    if ';' in rest:
        subtype, rest = rest.split(';', 1)
    else:
        subtype = rest
    if not re.search(token_re, subtype):
        commentary.error('invalid subtype:', field, value)

    # at this point there can be multiple parameters,
    # some of which could have quoted string values with ; in them


def validate_digest(field, value, record, version, commentary, pending):
    if ':' not in value:
        commentary.error('missing algorithm:', field, value)
    splits = value.split(':', 1)
    algorithm = splits[0]
    if len(splits) > 1:
        digest = splits[1]
    else:
        digest = 'none'
    if not re.search(token_re, algorithm):
        commentary.error('invalid algorithm:', field, value)
    else:
        try:
            Digester(algorithm)
        except ValueError:
            commentary.comment('unknown digest algorithm:', field, value)
    if not re.search(token_re, digest):
        # https://github.com/iipc/warc-specifications/issues/48
        # commentary.comment('spec incorrectly says this is an invalid digest', field, value)
        pass
    if not re.search(digest_re, digest):
        # suggested in https://github.com/iipc/warc-specifications/issues/48
        commentary.comment('Invalid-looking digest value:', field, value)


def validate_ip(field, value, record, version, commentary, pending):
    try:
        import ipaddress
        if six.PY2:  # pragma: no cover
            value = unicode(value)
        ipaddress.ip_address(value)
    except ValueError:
        commentary.error('invalid ip:', field, value)
    except (ImportError, NameError):  # pragma: no cover
        commentary.comment('did not check ip address format, install ipaddress module from pypi if you care')


def validate_truncated(field, value, record, version, commentary, pending):
    if value.lower() not in {'length', 'time', 'disconnect', 'unspecified'}:
        commentary.comment('unknown value, perhaps an extension:', field, value)


def validate_warcinfo_id(field, value, record, version, commentary, pending):
    validate_bracketed_uri(field, value, record, version, commentary, pending)


def validate_filename(field, value, record, version, commentary, pending):
    # text or quoted-string
    # comment for dangerous utf-8 in filename?
    pass


profiles = {
    '0.17': ['http://netpreserve.org/warc/0.17/revisit/identical-payload-digest',
             'http://netpreserve.org/warc/0.17/revisit/server-not-modified'],
    '0.18': ['http://netpreserve.org/warc/0.18/revisit/identical-payload-digest',
             'http://netpreserve.org/warc/0.18/revisit/server-not-modified'],
    '1.0': ['http://netpreserve.org/warc/1.0/revisit/identical-payload-digest',
            'http://netpreserve.org/warc/1.0/revisit/server-not-modified',
            'http://netpreserve.org/warc/1.0/revisit/uri-agnostic-identical-payload-digest'],
    '1.1': ['http://netpreserve.org/warc/1.1/revisit/identical-payload-digest',
            'http://netpreserve.org/warc/1.1/revisit/server-not-modified'],
}
profiles_rev = dict([(filename, version) for version, filenames in profiles.items() for filename in filenames])


def validate_profile(field, value, record, version, commentary, pending):
    if version not in profiles:
        return

    if value in profiles_rev:
        if profiles_rev[value] != version:
            commentary.comment('WARC-Profile value is for a different version:', version, value)
    else:
        commentary.comment('unknown value, perhaps an extension:', field, value)

    if '/revisit/uri-agnostic-identical-payload-digest' in value:
        commentary.comment('This Heretrix extension never made it into the standard:', field, value)


def validate_segment_number(field, value, record, version, commentary, pending):
    if not value.isdigit():
        commentary.error('must be an integer:', field, value)
        return
    iv = int(value)
    if iv == 0:
        commentary.error('must be 1 or greater:', field, value)

    rec_type = record.rec_headers.get_header('WARC-Type', 'none')
    if rec_type != 'continuation':
        if iv != 1:
            commentary.error('non-continuation records must always have WARC-Segment-Number: 1:', field, value)
        origin_id = record.rec_headers.get_header('WARC-Segment-Origin-ID')
        if origin_id is None:
            commentary.error('segmented records must have both WARC-Segment-Number and WARC-Segment-Origin-ID')
    if rec_type in {'warcinfo', 'request', 'metadata', 'revisit'}:
        commentary.recommendation('do not segment WARC-Type', rec_type)


def validate_segment_total_length(field, value, record, version, commentary, pending):
    if not value.isdigit():
        commentary.error('must be an integer:', field, value)


def validate_refers_to_filename(field, value, record, version, commentary, pending):
    commentary.comment('This Heretrix extension never made it into the standard:', field, value)


def validate_refers_to_file_offset(field, value, record, version, commentary, pending):
    commentary.comment('This Heretrix extension never made it into the standard:', field, value)


warc_fields = {
    'WARC-Type': {
        'validate': validate_warc_type,
    },
    'WARC-Record-ID': {
        'validate': validate_record_id,
    },
    'WARC-Date': {
        'validate': validate_timestamp,
    },
    'Content-Length': {
        'validate': validate_content_length,
    },
    'Content-Type': {
        'validate': validate_content_type,
    },
    'WARC-Concurrent-To': {
        'validate': validate_bracketed_uri,
    },
    'WARC-Block-Digest': {
        'validate': validate_digest,
    },
    'WARC-Payload-Digest': {
        'validate': validate_digest,
    },
    'WARC-IP-Address': {
        'validate': validate_ip,
    },
    'WARC-Refers-To': {
        'validate': validate_bracketed_uri,
    },
    'WARC-Target-URI': {
        'validate': validate_unbracketed_uri,
    },
    'WARC-Truncated': {
        'validate': validate_truncated,
    },
    'WARC-Warcinfo-ID': {
        'validate': validate_warcinfo_id,
    },
    'WARC-Filename': {
        'validate': validate_filename,
    },
    'WARC-Profile': {
        'validate': validate_profile,
    },
    'WARC-Identified-Payload-Type': {
        'validate': validate_content_type,
    },
    'WARC-Segment-Origin-ID': {
        'validate': validate_bracketed_uri,
    },
    'WARC-Segment-Number': {
        'validate': validate_segment_number,
    },
    'WARC-Segment-Total-Length': {
        'validate': validate_segment_total_length,
    },
    'WARC-Refers-To-Target-URI': {
        'validate': validate_unbracketed_uri,
        'minver': '1.1',
    },
    'WARC-Refers-To-Date': {
        'validate': validate_timestamp,
        'minver': '1.1',
    },
    'WARC-Refers-To-Filename': {
        'validate': validate_refers_to_filename,
    },
    'WARC-Refers-To-File-Offset': {
        'validate': validate_refers_to_file_offset,
    },
}
warc_fields = dict([(k.lower(), v) for k, v in warc_fields.items()])

record_types = {
    'warcinfo': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type', 'Content-Type'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-Filename', 'WARC-Truncated'],
        'prohibited': ['WARC-Refers-To', 'WARC-Profile', 'WARC-Identified-Payload-Type'],
        'ignored': ['WARC-Segment-Number', 'WARC-Segment-Origin-ID'],
        'validate': validate_warcinfo,
    },
    'response': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'Content-Type', 'WARC-Target-URI'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-IP-Address', 'WARC-Truncated',
                     'WARC-Concurrent-To', 'WARC-Warcinfo-ID', 'WARC-IP-Address', 'WARC-Segment-Number', 'WARC-Segment-Origin-ID'],
        'prohibited': ['WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'validate': validate_response,
    },
    'resource': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type', 'WARC-Target-URI', 'Content-Type'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-IP-Address', 'WARC-Truncated',
                     'WARC-Concurrent-To', 'WARC-Warcinfo-ID', 'WARC-Identified-Payload-Type', 'WARC-Segment-Number', 'WARC-Segment-Origin-ID'],
        'prohibited': ['WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'validate': validate_resource,
    },
    'request': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'Content-Type', 'WARC-Target-URI'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-IP-Address', 'WARC-Truncated',
                     'WARC-Concurrent-To', 'WARC-Warcinfo-ID', 'WARC-IP-Address'],
        'prohibited': ['WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'ignored': ['WARC-Segment-Number', 'WARC-Segment-Origin-ID'],
        'validate': validate_request,
    },
    'metadata': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'Content-Type'],
        'optional': ['WARC-Block-Digest', 'WARC-IP-Address', 'WARC-Truncated',
                     'WARC-Concurrent-To', 'WARC-Refers-To', 'WARC-Target-URI', 'WARC-Warcinfo-ID'],
        'prohibited': ['WARC-Payload-Digest', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'ignored': ['WARC-Segment-Number', 'WARC-Segment-Origin-ID'],
        'validate': validate_metadata,
    },
    'revisit': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'Content-Type', 'WARC-Target-URI', 'WARC-Profile'],
        'optional': ['WARC-Block-Digest', 'WARC-Truncated', 'WARC-IP-Address', 'WARC-Warcinfo-ID',  # normal optionals
                     'WARC-Payload-Digest', 'WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date'],  # these are for profiles
        'prohibited': ['WARC-Filename'],
        'ignored': ['WARC-Segment-Number', 'WARC-Segment-Origin-ID'],
        'validate': validate_revisit,
    },
    'conversion': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type', 'WARC-Target-URI'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-Truncated', 'WARC-Refers-To', 'WARC-Warcinfo-ID', 'WARC-Segment-Number', 'WARC-Segment-Origin-ID'],
        'prohibited': ['WARC-Concurrent-To', 'WARC-IP-Address', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'validate': validate_conversion,
    },
    'continuation': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'WARC-Segment-Number', 'WARC-Segment-Origin-ID', 'WARC-Target-URI'],
        'optional': ['WARC-Segment-Total-Length', 'WARC-Truncated'],
        'prohibited': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-Warcinfo-ID', 'WARC-IP-Address', 'WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'validate': validate_continuation,
    },
}


def make_header_set(config, kinds):
    ret = set()
    for kind in kinds:
        ret = ret.union(set([x.lower() for x in config.get(kind, [])]))
    return ret


def validate_fields_against_rec_type(rec_type, config, rec_headers, commentary, allow_all=False):
    for req in sorted(config.get('required', [])):
        if not rec_headers.get_header(req):
            commentary.error('missing required header:', req)
    for rec in sorted(config.get('recommended', [])):
        if not rec_headers.get_header(rec):
            commentary.recommendation('missing recommended header:', rec)
    allowed = make_header_set(config, ('required', 'optional', 'recommended', 'ignored'))
    prohibited = make_header_set(config, ('prohibited',))

    for field, value in rec_headers.headers:  # XXX not exported
        fl = field.lower()
        if fl in prohibited:
            commentary.error('field not allowed in record type:', rec_type, field)
        elif allow_all or fl in allowed:
            pass
        elif fl in warc_fields:  # pragma: no cover (this is a tester.py configuration omission)
            commentary.comment('Known field, but not expected for this record type:', rec_type, field)
        else:
            # an 'unknown field' comment has already been issued in validate_record
            pass


def validate_record_against_rec_type(config, record, commentary, pending):
    if 'validate' in config:
        config['validate'](record, commentary, pending)


def validate_record(record):
    version = record.rec_headers.protocol.split('/', 1)[1]  # XXX not exported

    record_id = record.rec_headers.get_header('WARC-Record-ID')
    rec_type = record.rec_headers.get_header('WARC-Type')
    commentary = Commentary(record_id=record_id, rec_type=rec_type)
    pending = None

    seen_fields = set()
    for field, value in record.rec_headers.headers:  # XXX not exported
        field_l = field.lower()
        if field_l != 'warc-concurrent-to' and field_l in seen_fields:
            commentary.error('duplicate field seen:', field, value)
        seen_fields.add(field_l)
        if field_l not in warc_fields:
            commentary.comment('unknown field, no validation performed:', field, value)
            continue
        config = warc_fields[field_l]
        if 'minver' in config:
            if version < config['minver']:
                commentary.comment('field was introduced after this warc version:', version, field, value)
        if 'validate' in config:
            config['validate'](field, value, record, version, commentary, pending)

    if rec_type not in record_types:
        # we print a comment for this elsewhere
        pass
    else:
        validate_fields_against_rec_type(rec_type, record_types[rec_type], record.rec_headers, commentary)
        validate_record_against_rec_type(record_types[rec_type], record, commentary, pending)

    return commentary


def save_global_info(record, warcfile, commentary, all_records, concurrent_to):
    record_id = record.rec_headers.get_header('WARC-Record-ID')
    if record_id is None:
        return

    for field, value in record.rec_headers.headers:  # XXX not exported
        if field.lower() == 'warc-concurrent-to':
            if record_id is not None and value is not None:
                concurrent_to[record_id].append(value)
                concurrent_to[value].append(record_id)

    save = {'warcfile': warcfile}

    saved_fields = (
        'WARC-Type', 'WARC-Warcinfo-ID', 'WARC-Date'
        'WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Payload-Digest', 'WARC-Target-URI',
        'WARC-Segment-Number', 'WARC-Segment-Origin-ID', 'WARC-Segment-Total-Length', 'WARC-Truncated'
    )
    saved_fields = set([x.lower() for x in saved_fields])

    for field, value in record.rec_headers.headers:  # XXX not exported
        field_l = field.lower()
        if field_l in saved_fields and value is not None:
            save[field_l] = value
        if field_l == 'warc-concurrent-to':
            if 'warc-concurrent-to' not in save:
                save['warc-concurrent-to'] = []
            save['warc-concurrent-to'].append(value)

    if record_id in all_records:
        commentary.error('Duplicate WARC-Record-ID:', record_id, 'found in files', warcfile, all_records[record_id]['warcfile'])
    else:
        all_records[record_id] = save


def check_global(all_records, concurrent_to):
    check_global_warcinfo(all_records)
    check_global_concurrent_to(all_records, concurrent_to)
    check_global_refers_to(all_records)
    check_global_segment(all_records)


def _print_global(header, commentary):
    if commentary.has_comments():
        print(header)
        for c in commentary.comments():
            print(' ', c)


def check_global_warcinfo(all_records):
    commentary = Commentary()
    for record_id, fields in all_records.items():
        if 'warc-warcinfo-id' in fields:
            wanted_id = fields['warc-warcinfo-id']
            if wanted_id not in all_records or all_records[wanted_id]['warc-type'] != 'warcinfo':
                commentary.comment('WARC-Warcinfo-ID not found:', record_id, 'WARC-Warcinfo-ID', wanted_id)

    _print_global('global warcinfo checks', commentary)


def check_global_concurrent_to(all_records, concurrent_to):
    commentary = Commentary()
    for record_id, fields in all_records.items():
        if 'warc-concurrent-to' in fields:
            whole_set = set(fields['warc-concurrent-to'])
            del fields['warc-concurrent-to']
            while True:
                current_set = list(whole_set)
                for c in current_set:
                    if c in all_records and 'warc-concurrent-to' in all_records[c]:
                        whole_set.update(set(all_records[c]['warc-concurrent-to']))
                        del all_records[c]['warc-concurrent-to']
                if len(whole_set) == len(current_set):
                    break
            warc_date = fields.get('warc-date')
            for wanted_id in sorted(whole_set):
                if wanted_id not in all_records:
                    commentary.comment('WARC-Concurrent-To not found:', record_id, 'WARC-Concurrent-To', wanted_id)
                else:
                    new_date = all_records[wanted_id].get('warc-date')
                    if warc_date != new_date:
                        commentary.comment('WARC-Concurrent-To set has conflicting dates:',
                                           record_id, warc_date, wanted_id, new_date)

    _print_global('global Concurrent-To checks', commentary)


def _revisit_compare(record_id, fields, source_field, wanted_id, all_records, target_field, commentary):
    if source_field.lower() not in fields:
        return

    if target_field.lower() not in all_records[wanted_id]:
        commentary.comment('revisit target lacks field:', wanted_id, target_field)
        return

    source_value = fields[source_field.lower()]
    target_value = all_records[wanted_id][target_field.lower()]
    if source_value != target_value:
        commentary.comment('revisit and revisit target disagree:',
                           record_id, source_field, source_value,
                           wanted_id, target_field, target_value)


def check_global_refers_to(all_records):
    commentary = Commentary()
    for record_id, fields in all_records.items():
        if 'warc-refers-to' not in fields:
            continue

        wanted_id = fields['warc-refers-to']
        if wanted_id not in all_records:
            commentary.comment('WARC-Refers-To target not found:', record_id, 'Warc-Refers-To', wanted_id)
            continue

        rec_type = fields.get('warc-type')
        if rec_type != 'revisit':
            continue

        _revisit_compare(record_id, fields, 'WARC-Refers-To-Target-URI',
                         wanted_id, all_records, 'WARC-Target-URI', commentary)
        _revisit_compare(record_id, fields, 'WARC-Refers-To-Date',
                         wanted_id, all_records, 'WARC-Date', commentary)
        _revisit_compare(record_id, fields, 'WARC-Payload-Digest',
                         wanted_id, all_records, 'WARC-Payload-Digest', commentary)

    _print_global('global Refers-To checks', commentary)


def check_global_segment(all_records):
    # warc-segment-origin-id :: exists, is warc-segment-number 1
    #   all segments exist, and the last one has WARC-Segment-Total-Length
    #   and only the last one has WARC-Truncated, if any

    # Segmentation shall not be used if a record can be stored in an existing warc file
    # The origin segment shall be placed in a new warc file preceded only by a warcinfo record (if any)

    pass


def _process_one(warcfile, all_records, concurrent_to):
    if warcfile.endswith('.arc') or warcfile.endswith('.arc.gz'):
        return
    with open(warcfile, 'rb') as stream:
        for record in WARCIterator(stream, check_digests=True, fixup_bugs=False):

            record = WrapRecord(record)
            digest_present = (record.rec_headers.get_header('WARC-Payload-Digest') or
                              record.rec_headers.get_header('WARC-Block-Digest'))

            commentary = validate_record(record)
            save_global_info(record, warcfile, commentary, all_records, concurrent_to)

            record.stream_for_digest_check()

            if commentary.has_comments() or record.digest_checker.passed is False:
                print(' ', 'WARC-Record-ID', commentary.record_id())
                print('   ', 'WARC-Type', commentary.rec_type())

                if record.digest_checker.passed is True:
                    print('    digest pass')
                elif record.digest_checker.passed is None:
                    if digest_present:  # pragma: no cover
                        # WARC record missing Content-Length: header, which is verboten
                        print('    digest present but not checked')
                    else:
                        print('    digest not present')
                for p in record.digest_checker.problems:
                    print('   ', p)

                if commentary.has_comments():
                    for c in commentary.comments():
                        print('   ', c)


class Tester(object):
    def __init__(self, cmd):
        self.inputs = cmd.inputs
        self.exit_value = 0
        self.all_records = defaultdict(dict)
        self.concurrent_to = defaultdict(list)

    def process_all(self):
        for warcfile in self.inputs:
            print(warcfile)
            try:
                self.process_one(warcfile)
            except ArchiveLoadFailed as e:
                print('  saw exception ArchiveLoadFailed: '+str(e).rstrip(), file=sys.stderr)
                print('  skipping rest of file', file=sys.stderr)

        check_global(self.all_records, self.concurrent_to)

        return self.exit_value

    def process_one(self, warcfile):
        _process_one(warcfile, self.all_records, self.concurrent_to)
