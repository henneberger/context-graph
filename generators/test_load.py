import json
import random
import unittest
import threading
import shutil
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from load import check_response, json_payload, parse_args as parse_load_args, video_command, stream_video, auth_headers, entity_id

def parse_args(argv):
    return parse_load_args(argv + ["--token", "test-jwt", "--workspace", "demo", "--entity", "alpha", "--related-to", "shared"])


class LoadTests(unittest.TestCase):
    def test_error_status_and_stream_errors_are_failures(self):
        for status, body, streaming in [(503, '{}', False), (200, '{"status":"error"}', True),
                                         (200, '{"status":"streaming"}', True), (202, '', False)]:
            with self.subTest(body=body), self.assertRaises(RuntimeError):
                check_response(status, body, streaming)
        self.assertEqual('complete', check_response(200, '{"status":"streaming"}\n{"status":"complete"}\n', True)['status'])

    def test_payload_supports_unknown_fields_and_invalid_schema(self):
        args = parse_args(['json', '--schema-drift', '--invalid-ratio', '1', '--out-of-order-seconds', '10'])
        event = json_payload(args, 21, random.Random(1))
        self.assertEqual('intentionally-invalid', event['value'])
        self.assertIn('extra', event)
        self.assertIn('relatedTo', event)
        self.assertTrue(event['eventTime'].endswith('Z'))
        json.dumps(event)

    def test_authentication_and_entity_selection_are_explicit(self):
        with self.assertRaises(SystemExit):
            parse_load_args(['json'])
        args = parse_load_args(['json','--token','signed-token','--workspace','demo','--entity','alpha','--entity','shared'])
        self.assertEqual('shared', entity_id(args, 1))
        self.assertEqual({'Authorization':'Bearer signed-token','X-Workspace-Id':'demo'}, auth_headers(args))

    def test_rejects_invalid_options(self):
        for options in [['--rate', '0'], ['--concurrency', '-1'], ['--invalid-ratio', '2']]:
            with self.subTest(options=options), self.assertRaises(SystemExit):
                parse_args(['json'] + options)

    @unittest.skipUnless(shutil.which('ffmpeg'), 'FFmpeg is required')
    def test_real_video_upload_drains_early_response(self):
        received = []
        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *args):
                pass
            def do_POST(self):
                received.append(dict(self.headers))
                self.send_response(200)
                self.send_header('Transfer-Encoding', 'chunked')
                self.end_headers()
                def record(value):
                    data = (json.dumps(value) + '\n').encode()
                    self.wfile.write(f'{len(data):x}\r\n'.encode() + data + b'\r\n')
                    self.wfile.flush()
                record({'status': 'streaming'})
                count = 0
                while True:
                    size = int(self.rfile.readline().strip(), 16)
                    if not size:
                        self.rfile.readline()
                        break
                    count += len(self.rfile.read(size))
                    self.rfile.read(2)
                received.append(count)
                record({'status': 'complete', 'segments': 1})
                self.wfile.write(b'0\r\n\r\n')
                self.wfile.flush()
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            args = parse_args(['video', '--url', f'http://127.0.0.1:{server.server_port}', '--stream-seconds', '0.3', '--no-realtime', '--width', '160', '--height', '120'])
            self.assertEqual('complete', stream_video(args, 0)['status'])
            self.assertEqual(received[0]["Authorization"], "Bearer test-jwt")
            self.assertEqual(received[0]["X-Workspace-Id"], "demo")
            self.assertEqual(received[0]["X-Entity-Id"], "alpha")
            self.assertGreater(received[1], 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_video_has_realtime_gop_and_pipe_output(self):
        command = video_command(parse_args(['video', '--gop', '101', '--keyframe-seconds', '3']))
        self.assertIn('-re', command)
        self.assertEqual('101', command[command.index('-g') + 1])
        self.assertEqual('pipe:1', command[-1])
        self.assertNotIn('-re', video_command(parse_args(['video', '--no-realtime'])))


if __name__ == '__main__':
    unittest.main()
