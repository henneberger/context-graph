"""TLS SpiceDB permission-check proxy; the public client credential cannot mutate policy."""
import hmac
import json
import os
from pathlib import Path
import ssl
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(os.environ.get('CHECK_PROXY_SECRETS', '/run/check-proxy'))
CLIENT_TOKEN = (ROOT / 'client-token').read_text().strip()
BACKEND_TOKEN = (ROOT / 'backend-token').read_text().strip()
BACKEND = os.environ.get('SPICEDB_ENDPOINT', 'https://spicedb:8443').rstrip('/')
if not CLIENT_TOKEN or not BACKEND_TOKEN or hmac.compare_digest(CLIENT_TOKEN, BACKEND_TOKEN):
    raise RuntimeError('Distinct nonempty client and backend credentials are mandatory')
if not BACKEND.startswith('https://'):
    raise RuntimeError('Backend TLS is mandatory')
TLS = ssl.create_default_context(cafile=ROOT / 'ca.crt')
SLOTS = threading.BoundedSemaphore(64)


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *args):
        pass
    def reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True
    def do_GET(self):
        self.reply(200 if self.path == '/health/live' else 404, {'status': 'ok'} if self.path == '/health/live' else {'error': 'not_found'})
    def do_POST(self):
        if self.path != '/v1/permissions/check':
            self.reply(404, {'error': 'not_found'}); return
        auth = self.headers.get('Authorization', '')
        if not hmac.compare_digest(auth, 'Bearer ' + CLIENT_TOKEN):
            self.reply(401, {'error': 'unauthorized'}); return
        if not SLOTS.acquire(blocking=False):
            self.reply(503, {'error': 'authorization_unavailable'}); return
        try:
            self.connection.settimeout(6)
            if self.headers.get('Transfer-Encoding'):
                self.reply(400, {'error': 'invalid_request'}); return
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 65536:
                self.reply(400, {'error': 'invalid_request'}); return
            body = json.loads(self.rfile.read(size))
            if not isinstance(body, dict) or set(body) - {'consistency', 'resource', 'permission', 'subject', 'context', 'with_tracing'}:
                self.reply(400, {'error': 'invalid_request'}); return
            # Enforce consistency independently of caller behavior.
            body['consistency'] = {'fully_consistent': True}
            request = urllib.request.Request(BACKEND + '/v1/permissions/check', data=json.dumps(body).encode(),
                headers={'Authorization': 'Bearer ' + BACKEND_TOKEN, 'Content-Type': 'application/json'})
            with urllib.request.urlopen(request, context=TLS, timeout=4) as response:
                data = response.read(65537)
                if response.status != 200 or len(data) > 65536:
                    raise RuntimeError()
                parsed = json.loads(data)
                if parsed.get('permissionship') not in {'PERMISSIONSHIP_HAS_PERMISSION', 'PERMISSIONSHIP_NO_PERMISSION', 'PERMISSIONSHIP_CONDITIONAL_PERMISSION'}:
                    raise RuntimeError()
                self.reply(200, {'permissionship': parsed['permissionship']})
        except (ValueError, json.JSONDecodeError):
            self.reply(400, {'error': 'invalid_request'})
        except Exception:
            self.reply(503, {'error': 'authorization_unavailable'})
        finally:
            SLOTS.release()


if __name__ == '__main__':
    server = ThreadingHTTPServer(('0.0.0.0', 8443), Handler)
    server.daemon_threads = True
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_2
    tls.load_cert_chain(ROOT / 'server.crt', ROOT / 'server.key')
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    server.serve_forever()
