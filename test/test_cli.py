from warcio.cli import main

from . import get_test_file

from contextlib import contextmanager
from io import BytesIO

from warcio.exceptions import ArchiveLoadFailed

import pytest
import sys
import tempfile
import os


def test_index(capsys):
    files = ['example.warc.gz', 'example.warc', 'example.arc.gz', 'example.arc']
    files = [get_test_file(filename) for filename in files]

    args = ['index', '-f', 'length,offset,warc-type,warc-target-uri,warc-filename,http:content-type']
    args.extend(files)

    expected = """\
{"length": "353", "offset": "0", "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"length": "431", "offset": "353", "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"length": "1228", "offset": "784", "warc-type": "response", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "609", "offset": "2012", "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"length": "586", "offset": "2621", "warc-type": "revisit", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "609", "offset": "3207", "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"length": "484", "offset": "0", "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"length": "705", "offset": "488", "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"length": "1365", "offset": "1197", "warc-type": "response", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "800", "offset": "2566", "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"length": "942", "offset": "3370", "warc-type": "revisit", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "800", "offset": "4316", "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"length": "171", "offset": "0", "warc-type": "warcinfo", "warc-filename": "live-web-example.arc.gz"}
{"length": "856", "offset": "171", "warc-type": "response", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "150", "offset": "0", "warc-type": "warcinfo", "warc-filename": "live-web-example.arc.gz"}
{"length": "1656", "offset": "151", "warc-type": "response", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
"""
    res = main(args=args)
    assert capsys.readouterr().out == expected


def test_index_2(capsys):
    files = ['example.warc.gz']
    files = [get_test_file(filename) for filename in files]

    args = ['index', '-f', 'offset,length,http:status,warc-type,filename']
    args.extend(files)

    expected = """\
{"offset": "0", "length": "353", "warc-type": "warcinfo", "filename": "example.warc.gz"}
{"offset": "353", "length": "431", "warc-type": "warcinfo", "filename": "example.warc.gz"}
{"offset": "784", "length": "1228", "http:status": "200", "warc-type": "response", "filename": "example.warc.gz"}
{"offset": "2012", "length": "609", "warc-type": "request", "filename": "example.warc.gz"}
{"offset": "2621", "length": "586", "http:status": "200", "warc-type": "revisit", "filename": "example.warc.gz"}
{"offset": "3207", "length": "609", "warc-type": "request", "filename": "example.warc.gz"}
"""
    res = main(args=args)
    assert capsys.readouterr().out == expected


def check_helper(args, capsys, expected_exit_value):
    exit_value = None
    try:
        main(args=args)
    except SystemExit as e:
        exit_value = e.code
    finally:
        assert exit_value == expected_exit_value

    return capsys.readouterr().out


def test_check_valid(capsys):
    filenames = [get_test_file('example.warc'), get_test_file('example.warc.gz')]

    args = ['check'] + filenames
    expected = ''
    assert check_helper(args, capsys, 0) == expected

    args = ['check', '-v'] + filenames
    value = check_helper(args, capsys, 0)
    assert value.count('digest pass') == 4
    assert value.count('WARC-Record-ID') == 12


def test_check_invalid(capsys):
    filenames = [get_test_file('example-digest.warc')]

    args = ['check'] + filenames
    value = check_helper(args, capsys, 1)
    assert value.count('payload digest failed') == 1
    assert value.count('WARC-Record-ID') == 1

    args = ['check', '-v'] + filenames
    value = check_helper(args, capsys, 1)
    assert value.count('payload digest failed') == 1
    assert value.count('digest pass') == 3
    assert value.count('WARC-Record-ID') == 4

    files = ['example-bad-non-chunked.warc.gz', 'example-digest.warc']
    filenames = [get_test_file(filename) for filename in files]
    args = ['check'] + filenames
    value = check_helper(args, capsys, 1)
    assert value.count('ArchiveLoadFailed') == 1
    assert value.count('payload digest failed') == 1
    assert value.count('WARC-Record-ID') == 1


def test_recompress_non_chunked(capsys):
    with named_temp() as temp:
        test_file = get_test_file('example-bad-non-chunked.warc.gz')

        with pytest.raises(ArchiveLoadFailed):
            main(args=['index', test_file, '-f', 'warc-type'])

        assert capsys.readouterr().out

        # recompress!
        main(args=['recompress', test_file, temp.name])
        assert 'Compression Errors Found and Fixed!' in capsys.readouterr().out

        expected = """\
{"warc-type": "warcinfo"}
{"warc-type": "warcinfo"}
{"warc-type": "response"}
{"warc-type": "request"}
{"warc-type": "revisit"}
{"warc-type": "request"}
"""

        main(args=['index', temp.name, '-f', 'warc-type'])
        assert capsys.readouterr().out == expected


def test_recompress_wrong_chunks(capsys):
    with named_temp() as temp:
        test_file = get_test_file('example-wrong-chunks.warc.gz')

        with pytest.raises(ArchiveLoadFailed):
            main(args=['index', test_file, '-f', 'warc-type'])


        expected = """\
{"offset": "0", "warc-type": "response", "warc-target-uri": "http://example.com/"}
{"offset": "1061", "warc-type": "request", "warc-target-uri": "http://example.com/"}
"""

        # recompress!
        main(args=['recompress', '-v', test_file, temp.name])

        out = capsys.readouterr().out
        assert '2 records read' in out
        assert 'Compression Errors Found and Fixed!' in out
        assert 'No Errors Found!' not in out

        assert expected in out


def test_recompress_arc2warc(capsys):
    with named_temp() as temp:
        test_file = get_test_file('example.arc.gz')

        # recompress!
        main(args=['recompress', test_file, temp.name])

        assert "No Errors" in capsys.readouterr().out


        expected = """\
{"warc-type": "warcinfo", "warc-block-digest": "sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ"}
{"warc-type": "response", "warc-block-digest": "sha1:PEWDX5GTH66WU74WBPGFECIYBMPMP3FP", "warc-payload-digest": "sha1:B2LTWWPUOYAH7UIPQ7ZUPQ4VMBSVC36A"}
"""

        main(args=['index', temp.name, '-f', 'warc-type,warc-block-digest,warc-payload-digest'])
        assert capsys.readouterr().out == expected


def test_recompress_arc2warc_verbose(capsys):
    with named_temp() as temp:
        test_file = get_test_file('example.arc.gz')

        # recompress!
        main(args=['recompress', '-v', test_file, temp.name])

        out = capsys.readouterr().out
        assert '{"offset": "0", "warc-type": "warcinfo"}' in out
        assert '"warc-target-uri": "http://example.com/"' in out

        assert 'No Errors Found!' in out
        assert '2 records read' in out


def test_recompress_bad_file():
    with named_temp() as temp:
        temp.write(b'abcdefg-not-a-warc\n')
        temp.seek(0)
        with named_temp() as temp2:
            with pytest.raises(SystemExit):
                main(args=['recompress', temp.name, temp2.name])

def test_recompress_bad_file_verbose():
    with named_temp() as temp:
        temp.write(b'abcdefg-not-a-warc\n')
        temp.seek(0)
        with named_temp() as temp2:
            with pytest.raises(SystemExit):
                main(args=['recompress', '--verbose', temp.name, temp2.name])


def test_extract_warcinfo(capsys):
    res = main(args=['extract', get_test_file('example.warc.gz'), '0'])
    assert capsys.readouterr().out == 'WARC/1.0\r\nWARC-Date: 2017-03-06T04:03:53Z\r\nWARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\nWARC-Filename: temp-20170306040353.warc.gz\r\nWARC-Type: warcinfo\r\nContent-Type: application/warc-fields\r\nContent-Length: 249\r\n\r\nsoftware: Webrecorder Platform v3.7\r\nformat: WARC File Format 1.0\r\ncreator: temp-MJFXHZ4S\r\nisPartOf: Temporary%20Collection\r\njson-metadata: {"title": "Temporary Collection", "size": 2865, "created_at": 1488772924, "type": "collection", "desc": ""}\r\n'

    res = main(args=['extract', '--headers', get_test_file('example.warc.gz'), '0'])
    assert capsys.readouterr().out == 'WARC/1.0\r\nWARC-Date: 2017-03-06T04:03:53Z\r\nWARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>\r\nWARC-Filename: temp-20170306040353.warc.gz\r\nWARC-Type: warcinfo\r\nContent-Type: application/warc-fields\r\nContent-Length: 249\r\n\r\n'

    res = main(args=['extract', '--payload', get_test_file('example.warc.gz'), '0'])
    assert capsys.readouterr().out == 'software: Webrecorder Platform v3.7\r\nformat: WARC File Format 1.0\r\ncreator: temp-MJFXHZ4S\r\nisPartOf: Temporary%20Collection\r\njson-metadata: {"title": "Temporary Collection", "size": 2865, "created_at": 1488772924, "type": "collection", "desc": ""}\r\n'

def test_extract_warc_response(capsysbinary):
    res = main(args=['extract', get_test_file('example.warc.gz'), '784'])
    assert capsysbinary.readouterr().out == b'WARC/1.0\r\nWARC-Target-URI: http://example.com/\r\nWARC-Date: 2017-03-06T04:02:06Z\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:a9c51e3e-0221-11e7-bf66-0242ac120005>\r\nWARC-IP-Address: 93.184.216.34\r\nWARC-Block-Digest: sha1:DR5MBP7OD3OPA7RFKWJUD4CTNUQUGFC5\r\nWARC-Payload-Digest: sha1:G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 975\r\n\r\nHTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nAccept-Ranges: bytes\r\nCache-Control: max-age=604800\r\nContent-Type: text/html\r\nDate: Mon, 06 Mar 2017 04:02:06 GMT\r\nEtag: "359670651+gzip"\r\nExpires: Mon, 13 Mar 2017 04:02:06 GMT\r\nLast-Modified: Fri, 09 Aug 2013 23:54:35 GMT\r\nServer: ECS (iad/182A)\r\nVary: Accept-Encoding\r\nX-Cache: HIT\r\nContent-Length: 606\r\nConnection: close\r\n\r\n\x1f\x8b\x08\x00;\x81\x05R\x00\x03\x8dTA\xaf\xd30\x0c\xbe\xefW\x98r\x01i]\xf7\x80\x07S\xd7V @\xe2\x02\x1c\xe0\xc21k\xdc\xd5Z\x93\x94$\xed6\xa1\xf7\xdfq\xdb\xbd\xae\xe5\xed@+\xb5\x8e\x1d\x7f\xfel\xc7I\x9eI\x93\xfbs\x8dPzUe\x8b\xe4\xf1\x87Bf\x0b\xe0\'\xf1\xe4+\xcc>\x9f\x84\xaa+\x84OF\t\xd2I4h\x17\xc3\x16\x85^@^\n\xeb\xd0\xa7A\xe3\x8bp\x13@\x94M\x8c\xa5\xf7u\x88\xbf\x1bj\xd3\xe0\xa3\xd1\x1e\xb5\x0f\xbb\xb0\x01\xe4\xc3*\r<\x9e|\xd4\x85\xdf\x8eP\xb7\x90\xb4P\x98\x06-\xe1\xb16\xd6O\xfc\x8f$}\x99Jl)\xc7\xb0_,\x814y\x12U\xe8rQazw\x85r\xfe\xcc\xc9t\x0c.\x81s\xe7\x82\xc1\xb63\xf2\x0c\x7fz\xb1_\x8a\xfc\xb0\xb7\xa6\xd12\xccMel\x0c\xcf\x8b5\xbf\xaf\xb6\xe3\x16%\xec\x9et\x0c\xeb\xab\xaa\x16R\x92\xde\xcft\x053\r\x0b\xa1\xa8:\xc7\x10|\xafQ\xc3\x0f\xa1]\xb0\x84\xe0\x0bV-z\xca\x05|\xc3\x06Y3*\x96\xf0\xc1r\x06Kp\xbc5th\xa9\xb8"\xf6\xc2C\xff\x95\xd4NH\xf7\xe9\xc7\xf0v\xbd\xaeOOy\xde\xa3\x02\xd1xs\x83\xee\xfd\xcc\xe1V\xee\xc5$\xfe\xceX\x896\xb4BR\xe3b\xb8C\xb5\x9dP\x12qE\xfa\xb0\xe4\x7fK\x8e<\xca\t\xc1G\xb8\xd7\x9b7\x9b\xcd\x04\xb1\xebE(17Vx2\xccU\x1b\x8dS\xd0\xf7\n%\tx\xa1\xc4)\xbcd\xf9\xae\xcb\xf2\xe5\xb4e\xf3\x0e\xfeO&\x0f\xa34/\xe4\xa4\x98\xf3\x8a\xcd\xfa~\xc3\xf6Oi\xd6s\xebX\xef\xb1dW\x12\xc37\x89\xfa#\x9au\xf2"\x89\x86y\\$]j<\x9eL\xf2r\x90\xcb\xbb\'\xa3\xc9\xaa\xc1Vg?Kr {=\xb0\x84\xce\x8b]E\xae\xe4^x\x03;\x84\xc6\xb1X\x18\x0bTU\x8d\xf3]\xd5[\x04\x1c\x10\x1d\xcf\x0f{\xe7\x8d\xe2\x01s+\xf8e\x1a\xce\xf9\xdc9\x81g\xe4\xe1\xe0]\xd0\xf5\xd5\xebH\xbe4\x8d\x87\xda\x12#\xe7\x86KA\xba\xef\'\xf0Z\xb8\x03\xa7\xde\x07\xad\xd1*r\x8e\r\xab$\xaaG\xd6\t\xdf\x17\x16\x8b4\xe8n\x8d8\x8a\x8e\xc7\xe3\x8a\x84\x16+c\xf7\xd1\x10\xcfE\x97hA\xf6\xd5X\xe4\xf0\x8c\xa7\xfa\x18\xab\x15\x83\x89\xac\x07L\xa2\xbeRIt\xa9[4\\o\x7f\x01\x08\x95\xaa\x8b\xf6\x04\x00\x00'

    res = main(args=['extract', '--headers', get_test_file('example.warc.gz'), '784'])
    assert capsysbinary.readouterr().out == b'WARC/1.0\r\nWARC-Target-URI: http://example.com/\r\nWARC-Date: 2017-03-06T04:02:06Z\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:a9c51e3e-0221-11e7-bf66-0242ac120005>\r\nWARC-IP-Address: 93.184.216.34\r\nWARC-Block-Digest: sha1:DR5MBP7OD3OPA7RFKWJUD4CTNUQUGFC5\r\nWARC-Payload-Digest: sha1:G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 975\r\n\r\nHTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nAccept-Ranges: bytes\r\nCache-Control: max-age=604800\r\nContent-Type: text/html\r\nDate: Mon, 06 Mar 2017 04:02:06 GMT\r\nEtag: "359670651+gzip"\r\nExpires: Mon, 13 Mar 2017 04:02:06 GMT\r\nLast-Modified: Fri, 09 Aug 2013 23:54:35 GMT\r\nServer: ECS (iad/182A)\r\nVary: Accept-Encoding\r\nX-Cache: HIT\r\nContent-Length: 606\r\nConnection: close\r\n\r\n'

    res = main(args=['extract', '--payload', get_test_file('example.warc.gz'), '784'])
    assert capsysbinary.readouterr().out == b'<!doctype html>\n<html>\n<head>\n    <title>Example Domain</title>\n\n    <meta charset="utf-8" />\n    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />\n    <meta name="viewport" content="width=device-width, initial-scale=1" />\n    <style type="text/css">\n    body {\n        background-color: #f0f0f2;\n        margin: 0;\n        padding: 0;\n        font-family: "Open Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;\n        \n    }\n    div {\n        width: 600px;\n        margin: 5em auto;\n        padding: 50px;\n        background-color: #fff;\n        border-radius: 1em;\n    }\n    a:link, a:visited {\n        color: #38488f;\n        text-decoration: none;\n    }\n    @media (max-width: 700px) {\n        body {\n            background-color: #fff;\n        }\n        div {\n            width: auto;\n            margin: 0 auto;\n            border-radius: 0;\n            padding: 1em;\n        }\n    }\n    </style>    \n</head>\n\n<body>\n<div>\n    <h1>Example Domain</h1>\n    <p>This domain is established to be used for illustrative examples in documents. You may use this\n    domain in examples without prior coordination or asking for permission.</p>\n    <p><a href="http://www.iana.org/domains/example">More information...</a></p>\n</div>\n</body>\n</html>\n'

# @pytest.mark.xfail
# warcio doesn't support ARC output yet, and @xfail tests have some bad
# interaction with capture_stdout(capsys), thus the failing tests are commented out
def test_extract_arc(capsysbinary):
    res = main(args=['extract', '--payload', get_test_file('example.arc'), '151'])
    assert capsysbinary.readouterr().out == b'<!doctype html>\n<html>\n<head>\n    <title>Example Domain</title>\n\n    <meta charset="utf-8" />\n    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />\n    <meta name="viewport" content="width=device-width, initial-scale=1" />\n    <style type="text/css">\n    body {\n        background-color: #f0f0f2;\n        margin: 0;\n        padding: 0;\n        font-family: "Open Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;\n        \n    }\n    div {\n        width: 600px;\n        margin: 5em auto;\n        padding: 50px;\n        background-color: #fff;\n        border-radius: 1em;\n    }\n    a:link, a:visited {\n        color: #38488f;\n        text-decoration: none;\n    }\n    @media (max-width: 700px) {\n        body {\n            background-color: #fff;\n        }\n        div {\n            width: auto;\n            margin: 0 auto;\n            border-radius: 0;\n            padding: 1em;\n        }\n    }\n    </style>    \n</head>\n\n<body>\n<div>\n    <h1>Example Domain</h1>\n    <p>This domain is established to be used for illustrative examples in documents. You may use this\n    domain in examples without prior coordination or asking for permission.</p>\n    <p><a href="http://www.iana.org/domains/example">More information...</a></p>\n</div>\n</body>\n</html>\n'

    # with capture_stdout(capsys) as buff:
    #     res = main(args=['extract', '--headers', get_test_file('example.arc'), '151'])
    #     assert buff.getvalue() == b'http://example.com/ 93.184.216.119 20140216050221 text/html 1591\nHTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nCache-Control: max-age=604800\r\nContent-Type: text/html\r\nDate: Sun, 16 Feb 2014 05:02:20 GMT\r\nEtag: "359670651"\r\nExpires: Sun, 23 Feb 2014 05:02:20 GMT\r\nLast-Modified: Fri, 09 Aug 2013 23:54:35 GMT\r\nServer: ECS (sjc/4FCE)\r\nX-Cache: HIT\r\nx-ec-custom-error: 1\r\nContent-Length: 1270\r\n\r\n'

    # with capture_stdout(capsys) as buff:
    #     res = main(args=['extract', get_test_file('example.arc'), '151'])
    #     assert buff.getvalue() == b'http://example.com/ 93.184.216.119 20140216050221 text/html 1591\nHTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nCache-Control: max-age=604800\r\nContent-Type: text/html\r\nDate: Sun, 16 Feb 2014 05:02:20 GMT\r\nEtag: "359670651"\r\nExpires: Sun, 23 Feb 2014 05:02:20 GMT\r\nLast-Modified: Fri, 09 Aug 2013 23:54:35 GMT\r\nServer: ECS (sjc/4FCE)\r\nX-Cache: HIT\r\nx-ec-custom-error: 1\r\nContent-Length: 1270\r\n\r\n<!doctype html>\n<html>\n<head>\n    <title>Example Domain</title>\n\n    <meta charset="utf-8" />\n    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />\n    <meta name="viewport" content="width=device-width, initial-scale=1" />\n    <style type="text/css">\n    body {\n        background-color: #f0f0f2;\n        margin: 0;\n        padding: 0;\n        font-family: "Open Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;\n        \n    }\n    div {\n        width: 600px;\n        margin: 5em auto;\n        padding: 50px;\n        background-color: #fff;\n        border-radius: 1em;\n    }\n    a:link, a:visited {\n        color: #38488f;\n        text-decoration: none;\n    }\n    @media (max-width: 700px) {\n        body {\n            background-color: #fff;\n        }\n        div {\n            width: auto;\n            margin: 0 auto;\n            border-radius: 0;\n            padding: 1em;\n        }\n    }\n    </style>    \n</head>\n\n<body>\n<div>\n    <h1>Example Domain</h1>\n    <p>This domain is established to be used for illustrative examples in documents. You may use this\n    domain in examples without prior coordination or asking for permission.</p>\n    <p><a href="http://www.iana.org/domains/example">More information...</a></p>\n</div>\n</body>\n</html>\n'

# due to NamedTemporaryFile issue on Windows
# see: https://bugs.python.org/issue14243#msg157925
@contextmanager
def named_temp():
    f = tempfile.NamedTemporaryFile(delete=False)
    try:
        yield f

    finally:
        try:
            os.unlink(f.name)
        except OSError:
            pass

