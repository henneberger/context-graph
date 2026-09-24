"""Local development identity issuer. Production APIs accept a separately managed OIDC provider."""
import base64, hashlib, hmac, json, os, ssl, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

ROOT=Path(os.environ.get('IDENTITY_SECRETS','/run/identity'))
ISSUER=os.environ.get('OIDC_ISSUER','https://identity:8443')
AUDIENCE=os.environ.get('OIDC_AUDIENCE','context-graph')
KEY=serialization.load_pem_private_key((ROOT/'signing.key').read_bytes(),password=None)
USERS=json.loads((ROOT/'users.json').read_text())
JWKS=json.loads((ROOT/'jwks.json').read_text())

def encode(value):return base64.urlsafe_b64encode(value).rstrip(b'=').decode()
def token(subject,ttl=300):
    now=int(time.time());header=encode(json.dumps({'alg':'RS256','typ':'JWT','kid':JWKS['keys'][0]['kid']},separators=(',',':')).encode())
    claims=encode(json.dumps({'iss':ISSUER,'aud':AUDIENCE,'sub':subject,'iat':now,'nbf':now,'exp':now+ttl},separators=(',',':')).encode())
    message=f'{header}.{claims}';return message+'.'+encode(KEY.sign(message.encode(),padding.PKCS1v15(),hashes.SHA256()))

class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):pass # Never log credentials, tokens or request bodies.
    def reply(self,status,value):
        data=json.dumps(value).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def do_GET(self):
        if self.path=='/.well-known/jwks.json':self.reply(200,JWKS)
        elif self.path=='/.well-known/openid-configuration':self.reply(200,{'issuer':ISSUER,'jwks_uri':ISSUER+'/.well-known/jwks.json','token_endpoint':ISSUER+'/token','id_token_signing_alg_values_supported':['RS256']})
        elif self.path=='/health/live':self.reply(200,{'status':'ok'})
        else:self.reply(404,{'error':'not_found'})
    def do_POST(self):
        if self.path!='/token':self.reply(404,{'error':'not_found'});return
        try:
            length=int(self.headers.get('Content-Length','0'))
            if not 0<length<=4096:raise ValueError()
            body=json.loads(self.rfile.read(length));name=body['username'];password=body['password']
            if not isinstance(name,str) or not isinstance(password,str) or len(password)>256:raise ValueError()
            record=USERS.get(name)
            # Perform equal work for missing users; expose no account existence signal.
            salt=bytes.fromhex(record['salt']) if record else b'\0'*16
            actual=hashlib.scrypt(password.encode(),salt=salt,n=16384,r=8,p=1).hex()
            if not record or not hmac.compare_digest(actual,record['hash']):self.reply(401,{'error':'invalid_credentials'});return
            ttl=body.get('ttl_seconds',300)
            if type(ttl) is not int or not 1<=ttl<=300:raise ValueError()
            self.reply(200,{'access_token':token(record['subject'],ttl),'token_type':'Bearer','expires_in':ttl})
        except (ValueError,KeyError,TypeError):self.reply(400,{'error':'invalid_request'})

if __name__=='__main__':
    from http_metrics import instrument
    instrument(Handler)
    server=ThreadingHTTPServer(('0.0.0.0',8443),Handler)
    server.daemon_threads=True
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(ROOT/'server.crt',ROOT/'server.key');server.socket=context.wrap_socket(server.socket,server_side=True)
    server.serve_forever()
