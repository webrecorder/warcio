from __future__ import print_function

import re
import sys
import six

from warcio.archiveiterator import WARCIterator
from warcio.utils import to_native_str, Digester
from warcio.exceptions import ArchiveLoadFailed


class Commentary:
    def __init__(self, record_id, rec_type):
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
        return getattr(self.__dict__['obj'], name)


def canon_content_type(s):
    return s.lower().replace('; ', ';')


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
                commentary.comment('Missing field-name : in warc-fields line:', line)
            else:
                field_name = line.split(':', 1)[0]
                if not re.search(token_re, field_name):
                    commentary.comment('invalid warc-fields name:', field_name)
                else:
                    lines.append(line)
        first_line = False

    if not lines:
        commentary.comment('warc-fields body present but empty')
        return

    # check known fields


def validate_warcinfo(record, commentary, pending):
    content_type = record.rec_headers.get_header('Content-Type', 'none')
    if content_type.lower() != 'application/warc-fields':
        commentary.recommendation('warcinfo Content-Type of application/warc-fields, saw', content_type)
    else:
        #   format: warc-fields
        #   allowable fields include but not limited to DMCI plus the following
        #   operator, software, robots, hostname, ip, http-header-user-agent, http-header-from
        #     if operator present, recommended name or name and email address
        #     comment if http-user-agent here and in the request or metadata record?
        #     comment if http-header-from here and in the request?
        validate_warc_fields(record, commentary)

    # whole-file tests:
    # optional that warcinfo be first in file, still deserves a comment
    # allowable for warcinfo to appear anywhere


def validate_response(record, commentary, pending):
    target_uri = record.rec_headers.get_header('WARC-Target-URI', 'none').lower()

    if target_uri.startswith('http:') or target_uri.startswith('https:'):
        content_type = record.rec_headers.get_header('Content-Type', 'none')
        if canon_content_type(content_type) not in {'application/http;msgtype=response', 'application/http'}:
            commentary.error('responses for http/https should have Content-Type of application/http; msgtype=response or application/http, saw', content_type)

        if record.rec_headers.get_header('WARC-IP-Address') is None:
            commentary.error('WARC-IP-Address should be used for http and https responses')

        # error: http and https schemes should have http response headers
        #   test by attempting to parse them?

        # comment: verify http content-length, if present -- commoncrawl nutch bug


def validate_resource(record, commentary, pending):
    target_uri = record.rec_headers.get_header('WARC-Target-URI', '').lower()

    if target_uri.startswith('dns:'):
        content_type = record.rec_headers.get_header('Content-Type', 'none')
        if content_type.lower() != 'text/dns':
            commentary.error('recource records for dns: shall have Content-Type of text/dns, saw', content_type)
        else:
            # rfc 2540 and rfc 1035
            #validate_text_dns()
            pass

    # should never have http headers


def validate_request(record, commentary, pending):
    target_uri = record.rec_headers.get_header('WARC-Target-URI', 'none').lower()

    if target_uri.startswith('http:') or target_uri.startswith('https:'):
        content_type = record.rec_headers.get_header('Content-Type')

        if canon_content_type(content_type) not in {'application/http;msgtype=request', 'application/http'}:
            commentary.error('requests for http/https should have Content-Type of application/http; msgtype=request or application/http, saw', content_type)

        if record.rec_headers.get_header('WARC-IP-Address') is None:
            commentary.error('WARC-IP-Address should be used for http and https requests')

        # error: http and https schemes should have http request headers

        # WARC-Concurrent-To field or fields may be used, comment if present but target record is not


def validate_metadata(record, commentary, pending):
    content_type = record.rec_headers.get_header('Content-Type', 'none')
    if content_type.lower() == 'application/warc-fields':
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
            'recommended': ['WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date'],
        }
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
        commentary.comment('no revisit details validation done due to unknown profile')


def validate_conversion(record, commentary, pending):
    # where practical, have a warc-refers-to field -- not quite a recommendation, perhaps make it a comment?
    # suggests there should be a corresponding metadata record -- which may have a WARC-Refers-To
    pass


def validate_continuation(record, commentary, pending):
    commentary.comment('warcio test continuation code has not been tested, expect bugs')

    segment_number = record.rec_headers.get_header('WARC-Segment-Number', 'none')
    if segment_number.isdigit() and int(segment_number) < 2:
        commentary.error('continuation record must have WARC-Segment-Number > 1, saw', segment_number)

    # last segment: required WARC-Segment-Total-Length, optional WARC-Truncated


def validate_actual_uri(field, value, record, version, commentary, pending):
    # uri per RFC 3986
    # should use a registered scheme
    # %XX encoding, normalize to upper case
    # schemes are case-insensitive and normalize to lower
    if value.startswith('<') or value.endswith('>'):
        # wget 1.19 bug caused by WARC 1.0 spec error
        commentary.error('uri must not be within <>', field, value)
    if ':' not in value:
        commentary.error('invalid uri, no scheme', field, value)
    if re.search(r'\s', value):
        commentary.error('invalid uri, contains whitespace', field, value)
    scheme = value.split(':', 1)[0]
    if not re.search(r'\A[A-Za-z][A-Za-z0-9+\-\.]*\Z', scheme):
        commentary.error('invalid uri scheme, bad character', field, value)
    # https://www.iana.org/assignments/uri-schemes/uri-schemes.xhtml


def validate_warc_type(field, value, record, version, commentary, pending):
    if not value.islower():
        # I am unclear if this is allowed? standard is silent
        commentary.comment('WARC-Type is not lower-case', field, value)
    if value.lower() not in record_types:
        # standard says readers should ignore unknown warc-types
        commentary.comment('unknown WARC-Type', field, value)


def validate_uri(field, value, record, version, commentary, pending):
    # < uri >
    if not (value.startswith('<') and value.endswith('>')):
        commentary.error('uri must be within <>', field, value)
        return
    validate_actual_uri(field, value[1:-1], record, version, commentary, pending)


def validate_record_id(field, value, record, version, commentary, pending):
    validate_uri(field, value, record, version, commentary, pending)
    # TODO: should be "globally unique for its period of intended use"


def validate_timestamp(field, value, record, version, commentary, pending):
    use_ms = False if version == '1.0' else True
    if not use_ms:
        if '.' in value:
            # XXX specification infelicity: would be nice to have 'advice to implementers' here
            commentary.error('WARC 1.0 may not have fractional seconds', field, value)
    else:
        if '.' in value:
            start, end = value.split('.', 1)
            if not re.search(r'\A[0-9]{1,9}Z\Z', end):
                commentary.error('fractional seconds must have 1-9 digits', field, value)

    # XXX the above is pretty incomplete for dash, colon, trailing Z, etc

    # TODO: "multiple records written as part of a single capture event shall use the same WARC-Date"
    # how? follow WARC-Concurrent-To pointer(s) from request to response(s)


def validate_content_length(field, value, record, version, commentary, pending):
    if not value.isdigit():
        commentary.error('must be an integer', field, value)


token_re = r'\A[!"#$%&\'()*+\-\.0-9A-Z\^_`a-z|~]+\Z'
digest_re = r'\A[A-Za-z0-9/+\-_=]+\Z'


def validate_content_type(field, value, record, version, commentary, pending):
    if '/' not in value:
        commentary.error('must contain a /', field, value)
    splits = value.split('/', 1)
    ctype = splits[0]
    if len(splits) > 1:
        rest = splits[1]
    else:
        rest = ''
    if not re.search(token_re, ctype):
        commentary.error('invalid type', field, value)
    if ';' in rest:
        subtype, rest = rest.split(';', 1)
    else:
        subtype = rest
    if not re.search(token_re, subtype):
        commentary.error('invalid subtype', field, value)

    # at this point there can be multiple parameters,
    # some of which could have quoted string values with ; in them

    # TODO: more checking


def validate_digest(field, value, record, version, commentary, pending):
    if ':' not in value:
        commentary.error('missing algorithm', field, value)
    splits = value.split(':', 1)
    algorithm = splits[0]
    if len(splits) > 1:
        digest = splits[1]
    else:
        digest = 'none'
    if not re.search(token_re, algorithm):
        commentary.error('invalid algorithm', field, value)
    else:
        try:
            Digester(algorithm)
        except ValueError:
            commentary.comment('unknown digest algorithm', field, value)
    if not re.search(token_re, digest):
        # https://github.com/iipc/warc-specifications/issues/48
        # commentary.comment('spec incorrectly says this is an invalid digest', field, value)
        pass
    if not re.search(digest_re, digest):
        commentary.comment('Invalid-looking digest value', field, value)


def validate_ip(field, value, record, version, commentary, pending):
    try:
        import ipaddress
        if six.PY2:  # pragma: no cover
            value = unicode(value)
        ipaddress.ip_address(value)
    except ValueError:
        commentary.error('invalid ip', field, value)
    except (ImportError, NameError):  # pragma: no cover
        commentary.comment('did not check ip address format, install ipaddress module from pypi if you care')


def validate_truncated(field, value, record, version, commentary, pending):
    if value.lower() not in {'length', 'time', 'disconnect', 'unspecified'}:
        commentary.comment('extension seen', field, value)


def validate_warcinfo_id(field, value, record, version, commentary, pending):
    validate_uri(field, value, record, version, commentary, pending)
    # TODO: should point at a warcinfo record


def validate_filename(field, value, record, version, commentary, pending):
    # TODO: text or quoted-string
    pass


profiles = {
    # XXX WARC/0.17 and WARC/0.18
    '1.0': ['http://netpreserve.org/warc/1.0/revisit/identical-payload-digest',
            'http://netpreserve.org/warc/1.0/revisit/server-not-modified',
            # the following removed from iipc/webarchive-commons in may 2017; common in the wild TODO comment or not?
            # https://github.com/iipc/webarchive-commons/commits/988bec707c27a01333becfc3bd502af4441ea1e1/src/main/java/org/archive/format/warc/WARCConstants.java
            'http://netpreserve.org/warc/1.0/revisit/uri-agnostic-identical-payload-digest'],
    '1.1': ['http://netpreserve.org/warc/1.1/revisit/identical-payload-digest',
            'http://netpreserve.org/warc/1.1/revisit/server-not-modified'],
}


def validate_profile(field, value, record, version, commentary, pending):
    if version not in profiles:
        commentary.comment('no profile check because unknown warc version', field, value)
        return
    if value not in profiles[version]:
        commentary.comment('extension seen', field, value)


def validate_segment_number(field, value, record, version, commentary, pending):
    if not value.isdigit():
        commentary.error('must be an integer', field, value)
        return
    iv = int(value)
    if iv == 0:
        commentary.error('must be 1 or greater', field, value)

    rec_type = record.rec_headers.get_header('WARC-Type', 'none')
    if rec_type != 'continuation':
        if iv != 1:
            commentary.error('non-continuation records must always have WARC-Segment-Number = 1', field, value)
    if rec_type in {'warcinfo', 'request', 'metadata', 'revisit'}:
        commentary.recommendation('do not segment WARC-Type', rec_type)


def validate_segment_total_length(field, value, record, version, commentary, pending):
    if not value.isdigit():
        commentary.error('must be an integer', field, value)


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
        'validate': validate_uri,
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
        'validate': validate_uri,
    },
    'WARC-Target-URI': {
        'validate': validate_actual_uri,
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
        'validate': validate_uri,
    },
    'WARC-Segment-Number': {
        'validate': validate_segment_number,
    },
    'WARC-Segment-Total-Length': {
        'validate': validate_segment_total_length,
    },
    'WARC-Refers-To-Target-URI': {
        'validate': validate_actual_uri,
        'minver': '1.1',
    },
    'WARC-Refers-To-Date': {
        'validate': validate_timestamp,
        'minver': '1.1',
    },
}
warc_fields = dict([(k.lower(), v) for k, v in warc_fields.items()])

record_types = {
    'warcinfo': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type', 'Content-Type'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-Filename', 'WARC-Truncated'],
        'prohibited': ['WARC-Refers-To', 'WARC-Profile', 'WARC-Identified-Payload-Type'],
        'validate': validate_warcinfo,
    },
    'response': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'Content-Type', 'WARC-Target-URI'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-IP-Address', 'WARC-Truncated',
                     'WARC-Concurrent-To', 'WARC-Warcinfo-ID', 'WARC-IP-Address'],
        'prohibited': ['WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'validate': validate_response,
    },
    'resource': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type', 'WARC-Target-URI', 'Content-Type'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-IP-Address', 'WARC-Truncated',
                     'WARC-Concurrent-To', 'WARC-Warcinfo-ID', 'WARC-Identified-Payload-Type'],
        'prohibited': ['WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'validate': validate_resource,
    },
    'request': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'Content-Type', 'WARC-Target-URI'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-IP-Address', 'WARC-Truncated',
                     'WARC-Concurrent-To', 'WARC-Warcinfo-ID', 'WARC-IP-Address'],
        'prohibited': ['WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'validate': validate_request,
    },
    'metadata': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'Content-Type'],
        'optional': ['WARC-Block-Digest', 'WARC-IP-Address', 'WARC-Truncated',
                     'WARC-Concurrent-To', 'WARC-Refers-To', 'WARC-Target-URI', 'WARC-Warcinfo-ID'],
        'prohibited': ['WARC-Payload-Digest', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'validate': validate_metadata,
    },
    'revisit': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'Content-Type', 'WARC-Target-URI', 'WARC-Profile'],
        'optional': ['WARC-Block-Digest', 'WARC-Truncated', 'WARC-IP-Address', 'WARC-Warcinfo-ID',  # normal optionals
                     'WARC-Payload-Digest', 'WARC-Refers-To', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date'],  # these are for profiles
        'prohibited': ['WARC-Filename'],
        'validate': validate_revisit,
    },
    'conversion': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type', 'WARC-Target-URI'],
        'optional': ['WARC-Block-Digest', 'WARC-Payload-Digest', 'WARC-Truncated', 'WARC-Refers-To', 'WARC-Warcinfo-ID'],
        'prohibited': ['WARC-Concurrent-To', 'WARC-IP-Address', 'WARC-Refers-To-Target-URI', 'WARC-Refers-To-Date', 'WARC-Filename', 'WARC-Profile'],
        'validate': validate_conversion,
    },
    'continuation': {
        'required': ['WARC-Record-ID', 'Content-Length', 'WARC-Date', 'WARC-Type',
                     'WARC-Segment-Origin-ID', 'WARC-Segment-Number', 'WARC-Target-URI'],
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
            commentary.error('missing required header', req)
    for rec in sorted(config.get('recommended', [])):
        if not rec_headers.get_header(rec):
            commentary.recommendation('missing recommended header', rec)
    allowed = make_header_set(config, ('required', 'optional', 'recommended'))
    prohibited = make_header_set(config, ('prohibited',))

    for field, value in rec_headers.headers:
        fl = field.lower()
        if fl in prohibited:
            commentary.error('field not allowed in record_type', field, rec_type)
        elif allow_all or fl in allowed:
            pass
        elif fl in warc_fields:
            commentary.comment('no configuration seen for', field, rec_type)
        else:
            # an 'unknown field' comment has already been issued in validate_record
            pass


def validate_record_against_rec_type(config, record, commentary, pending):
    if 'validate' in config:
        config['validate'](record, commentary, pending)


def validate_record(record):
    version = record.rec_headers.protocol.split('/', 1)[1]  # XXX not exported?

    record_id = record.rec_headers.get_header('WARC-Record-ID')
    rec_type = record.rec_headers.get_header('WARC-Type')
    commentary = Commentary(record_id, rec_type)
    pending = None

    seen_fields = set()
    for field, value in record.rec_headers.headers:
        field_case = field
        field = field.lower()
        if field != 'warc-concurrent-to' and field in seen_fields:
            commentary.error('duplicate field seen', field, value)
        seen_fields.add(field)
        if field not in warc_fields:
            commentary.comment('unknown field, no validation performed', field_case, value)
            continue
        config = warc_fields[field]
        if 'minver' in config:
            if version < config['minver']:
                # unknown fields are extensions, so this is a comment and not an error
                commentary.comment('field was introduced after this warc version', field_case, value, version)
        if 'validate' in config:
            config['validate'](field, value, record, version, commentary, pending)

    if rec_type not in record_types:
        pass  # we print a comment for this elsewhere
    else:
        validate_fields_against_rec_type(rec_type, record_types[rec_type], record.rec_headers, commentary)
        validate_record_against_rec_type(record_types[rec_type], record, commentary, pending)

    return commentary


def _process_one(warc):
    if warc.endswith('.arc') or warc.endswith('.arc.gz'):
        return
    with open(warc, 'rb') as stream:
        for record in WARCIterator(stream, check_digests=True, fixup_bugs=False):

            record = WrapRecord(record)
            digest_present = (record.rec_headers.get_header('WARC-Payload-Digest') or
                              record.rec_headers.get_header('WARC-Block-Digest'))

            commentary = validate_record(record)

            record.content  # make sure digests are checked
            # XXX might need to read and digest the raw stream to check digests for chunked encoding?
            # XXX chunked lacks Content-Length and presumably the digest needs to be computed on the non-chunked bytes

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

    def process_all(self):
        for warc in self.inputs:
            print(warc)
            try:
                self.process_one(warc)
            except ArchiveLoadFailed as e:
                print('  saw exception ArchiveLoadFailed: '+str(e).rstrip(), file=sys.stderr)
                print('  skipping rest of file', file=sys.stderr)
        return self.exit_value

    def process_one(self, filename):
        _process_one(filename)
