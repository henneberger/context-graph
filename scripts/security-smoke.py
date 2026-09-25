#!/usr/bin/env python3
"""Real OIDC + SpiceDB + secure Kafka/Flink/Iceberg/GraphQL authorization smoke.
Uses private local fixture credentials; never prints credentials or JWTs.
"""
import argparse
import asyncio
import base64
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


load = module('cg_load', ROOT / 'generators/load.py')
ws_checks = module('cg_ws_security', ROOT / 'scripts/query-security-smoke.py')


class Smoke:
    def __init__(self, args):
        self.args = args
        self.credentials = json.loads(Path(args.credentials).read_text())
        self.spice_token = Path(args.spice_token).read_text().strip()
        self.tls = ssl.create_default_context(cafile=args.ca)
        self.tokens = {}
        self.evidence = {}
        self.metric = 'security-smoke-' + uuid.uuid4().hex[:10]

    def request(self, path, body=None, user=None, workspace='demo', token=None, headers=None, origin=None, method=None):
        hdr = dict(headers or {})
        if path == '/ingest/events' and isinstance(body,dict):
            hdr.setdefault('X-Resource-Key',body.get('entityId','alpha'))
        if user or token:
            hdr.update({'Authorization': 'Bearer ' + (token or self.token(user)), 'X-Workspace-Id': workspace})
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
            hdr['Content-Type'] = 'application/json'
        req = urllib.request.Request((origin or self.args.url).rstrip('/') + path, data=body, headers=hdr, method=method)
        try:
            response = urllib.request.urlopen(req, context=self.tls, timeout=25)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read(4 * 1024 * 1024)
            return response.status, data, response.headers

    def token(self, user):
        cached = self.tokens.get(user)
        if cached and cached[1] > time.time() + 60:
            return cached[0]
        code, body, _ = self.request(self.args.token_path, {'username': user, 'password': self.credentials[user]})
        if code != 200:
            raise AssertionError('Identity login failed for fixture user')
        result = json.loads(body)
        token = result['access_token']
        claims = json.loads(base64.urlsafe_b64decode(token.split('.')[1] + '==='))
        self.tokens[user] = (token, claims['exp'])
        return token

    def short_token(self,user,ttl):
        code,body,_=self.request('/token',{'username':user,'password':self.credentials[user],'ttl_seconds':ttl},origin=self.args.identity_url)
        if code!=200: raise AssertionError('Short-lived fixture token request failed')
        return json.loads(body)['access_token']

    def expect(self, label, response, codes):
        if response[0] not in codes:
            raise AssertionError(f'{label}: expected HTTP {codes}, received {response[0]}')
        self.evidence[label] = True
        return response

    def query(self, user, query, variables=None):
        code, body, _ = self.request('/graphql', {'query': query, 'variables': variables or {}}, user=user)
        if code != 200:
            raise AssertionError(f'Authorized GraphQL request failed with HTTP {code}')
        result = json.loads(body)
        if result.get('errors'):
            raise AssertionError('Authorized GraphQL request returned errors')
        return result['data']

    def relationship(self, entity, relation, subject, operation):
        resource = hashlib.sha256(('demo\0' + entity).encode()).hexdigest()
        update = {'operation': operation, 'relationship': {'resource': {'object_type': 'entity', 'object_id': resource},
                  'relation': relation, 'subject': {'object': {'object_type': 'user', 'object_id': subject}}}}
        code, _, _ = self.request('/v1/relationships/write', {'updates': [update]}, origin=self.args.spicedb_url,
                                  headers={'Authorization': 'Bearer ' + self.spice_token})
        if code != 200:
            raise AssertionError(f'SpiceDB fixture mutation failed with HTTP {code}')

    def publish(self, entity, value, metric=None, event_time=None, related=None):
        payload = {'entityId': entity, 'eventTime': event_time or dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')}
        if value is not None:
            payload.update(value=value, metric=metric or self.metric)
        if related:
            payload['relatedTo'] = related
        response = self.request('/ingest/events', payload, user='producer', headers={'X-Resource-Key':entity})
        if response[0] != 202:
            raise AssertionError(f'Authorized producer ingest failed with HTTP {response[0]}')
        return json.loads(response[1])

    def negative_checks(self):
        body = {'entityId': 'alpha', 'value': 1}
        self.expect('anonymousWriteDenied', self.request('/ingest/events', body), {401})
        self.expect('anonymousQueryDenied', self.request('/graphql', {'query': '{schemas{name}}'}), {401})
        token = self.token('producer')
        parts = token.split('.')
        parts[2] = ('A' if parts[2][0] != 'A' else 'B') + parts[2][1:]
        self.expect('forgedSignatureWriteDenied', self.request('/ingest/events', body, token='.'.join(parts)), {401})
        self.expect('forgedSignatureQueryDenied', self.request('/graphql', {'query': '{schemas{name}}'}, token='.'.join(parts)), {401})
        self.expect('readerCannotWrite', self.request('/ingest/events', body, user='alice'), {403})
        self.expect('disjointEntityWriteDenied', self.request('/ingest/events', {'entityId': 'beta', 'value': 1}, user='alice'), {403})
        self.expect('crossWorkspaceWriteDenied', self.request('/ingest/events', body, user='outsider'), {403})
        self.expect('crossWorkspaceQueryDenied', self.request('/graphql', {'query': '{schemas{name}}'}, user='outsider'), {403})
        self.expect('missingWorkspaceRelationshipDenied', self.request('/ingest/events', {'entityId': 'unprovisioned-' + uuid.uuid4().hex, 'value': 1}, user='producer'), {403})
        self.expect('invalidSchemaDenied', self.request('/ingest/events', {**body, 'value': 'not-a-number'}, user='producer'), {400})
        print('PASS verified identity, workspace boundary, explicit write grants and schema rejection', flush=True)

    def image_checks(self):
        content = load.image_bytes(argparse.Namespace(file=None, width=160, height=120))
        code, body, _ = self.expect('authorizedImageWrite', self.request('/ingest/images', content, user='producer', headers={'X-Entity-Id': 'alpha', 'Content-Type': 'image/png'}), {202})
        event = json.loads(body)
        labels = event['security']
        assert event['schemaVersion'] == 2 and labels['workspaceId'] == 'demo' and labels['subjectId'] == 'producer'
        assert labels['resourceId'] == hashlib.sha256(b'demo\0alpha').hexdigest()
        self.evidence['serverDerivedSecurityLabels'] = True
        uri = event['payload']['uri']
        allowed = self.expect('authorizedMediaRead', self.request(uri, user='alice'), {200})
        assert allowed[1] == content
        assert allowed[2].get('Cache-Control', '').replace(' ', '') == 'private,no-store'
        self.expect('anonymousMediaDenied', self.request(uri), {401})
        self.expect('disjointMediaDenied', self.request(uri, user='bob'), {403})
        self.expect('crossWorkspaceMediaDenied', self.request(uri, user='outsider'), {403})
        ranged = self.expect('authorizedMediaRange', self.request(uri, user='alice', headers={'Range': 'bytes=0-7'}), {206})
        assert ranged[1] == content[:8]
        self.expect('unauthorizedMediaRangeDenied', self.request(uri, user='bob', headers={'Range': 'bytes=0-7'}), {403})
        session = self.expect('verifiedMediaCookieExchange', self.request('/auth/session', b'', user='alice', method='POST'), {200})
        cookie = session[2].get('Set-Cookie', '')
        assert 'HttpOnly' in cookie and 'SameSite=Strict' in cookie and 'Path=/media/' in cookie
        cookie_header = {'Cookie': cookie.split(';')[0]}
        self.expect('verifiedMediaCookieRead', self.request(uri, headers=cookie_header), {200})
        self.expect('cookieCannotWrite', self.request('/ingest/events', {'entityId': 'alpha', 'value': 1}, headers=cookie_header), {401})
        try:
            self.relationship('alpha', 'reader', 'alice', 'OPERATION_DELETE')
            self.expect('revokedMediaBearerDenied', self.request(uri, user='alice'), {403})
            self.expect('revokedMediaCookieDenied', self.request(uri, headers=cookie_header), {403})
        finally:
            self.relationship('alpha', 'reader', 'alice', 'OPERATION_TOUCH')
        short_token=self.short_token('alice',2)
        expired_session=self.expect('shortLivedMediaSession',self.request('/auth/session',b'',token=short_token,method='POST'),{200})
        expired_cookie={'Cookie':expired_session[2]['Set-Cookie'].split(';')[0]}
        time.sleep(3)
        self.expect('expiredMediaSessionDenied',self.request(uri,headers=expired_cookie),{401})
        self.expect('expiredJwtQueryDenied',self.request('/graphql',{'query':'{schemas{name}}'},token=short_token),{401})
        logout = self.expect('mediaLogout', self.request('/auth/logout', b'', method='POST'), {204})
        assert 'Max-Age=0' in logout[2].get('Set-Cookie', '') and 'Path=/media/' in logout[2]['Set-Cookie']
        print('PASS protected media, byte ranges, signed cookies and immediate read revocation', flush=True)

    async def video_checks(self):
        token=await asyncio.to_thread(self.token,'producer')
        args=load.parse_args(['video','--url',self.args.url,'--workspace','demo','--entity','alpha','--token',token,
                              '--width','160','--height','120','--stream-seconds','5','--gop','97','--no-realtime'])
        result=await asyncio.to_thread(load.stream_video,args,0)
        playlist_response=self.expect('authorizedVideoPlaylist',await asyncio.to_thread(self.request,result['playlistUri'],user='alice'),{200})
        self.expect('disjointVideoPlaylistDenied',await asyncio.to_thread(self.request,result['playlistUri'],user='bob'),{403})
        playlist=playlist_response[1].decode(); assert '#EXT-X-INDEPENDENT-SEGMENTS' in playlist
        chunks=[line for line in playlist.splitlines() if line and not line.startswith('#')]; assert len(chunks)>=2
        with tempfile.TemporaryDirectory(prefix='cg-secure-video-') as folder:
            for index,name in enumerate(chunks):
                uri=result['playlistUri'].rsplit('/',1)[0]+'/'+name
                data=self.expect('authorizedVideoChunk',await asyncio.to_thread(self.request,uri,user='alice'),{200})[1]
                self.expect('disjointVideoChunkDenied',await asyncio.to_thread(self.request,uri,user='bob'),{403})
                path=Path(folder)/f'{index}.ts'; path.write_bytes(data)
                probe=await asyncio.to_thread(subprocess.run,['ffprobe','-v','error','-select_streams','v:0','-show_entries','frame=key_frame','-of','csv=p=0',str(path)],capture_output=True,text=True,timeout=20,check=True)
                assert probe.stdout.startswith('1')
                decode=await asyncio.to_thread(subprocess.run,['ffmpeg','-v','error','-i',str(path),'-f','null','-'],capture_output=True,timeout=20)
                assert decode.returncode==0 and not decode.stderr
        self.evidence['authorizedVideoKeyframeChunks']=len(chunks)
        # A decoder must never copy another entity's local media into a newly
        # authorized stream, or fetch an uploaded playlist's network references.
        args.entity=['beta']
        beta=await asyncio.to_thread(load.stream_video,args,0)
        args.entity=['alpha']
        beta_playlist=self.expect('producerBetaVideoPlaylist',await asyncio.to_thread(self.request,beta['playlistUri'],user='producer'),{200})[1].decode()
        beta_chunk=next(line for line in beta_playlist.splitlines() if line and not line.startswith('#'))
        local_uri='file:///data/media/'+beta['streamId']+'/'+beta_chunk
        try:
            await asyncio.to_thread(self.relationship,'alpha','writer','alice','OPERATION_TOUCH')
            for label,reference in [('localFileReferenceDenied',local_uri),('networkReferenceDenied','https://identity:8443/.well-known/jwks.json')]:
                attack=('#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\n'+reference+'\n#EXT-X-ENDLIST\n').encode()
                response=await asyncio.to_thread(self.request,'/ingest/video',attack,user='alice',headers={'X-Entity-Id':'alpha','Content-Type':'video/mp2t'})
                try:
                    load.check_response(response[0],response[1].decode(),streaming=True)
                except RuntimeError:
                    records=[json.loads(line) for line in response[1].decode().splitlines() if line.strip()]
                    assert records[-1].get('errorQueued') is True, 'Attack was not rejected at the authenticated media processing boundary'
                    self.evidence[label]=True
                else:
                    raise AssertionError('Uploaded playlist reference was accepted')
                self.expect('imagePlaylistReferenceDenied',await asyncio.to_thread(self.request,'/ingest/images',attack,user='alice',headers={'X-Entity-Id':'alpha','Content-Type':'image/png'}),{400})
        finally:
            await asyncio.to_thread(self.relationship,'alpha','writer','alice','OPERATION_DELETE')

        self.expect('unauthorizedVideoUploadDenied',await asyncio.to_thread(self.request,'/ingest/video',b'not-media',user='bob',headers={'X-Entity-Id':'alpha','Content-Type':'video/mp2t'}),{403})
        args.no_realtime=False; args.stream_seconds=20
        upload=asyncio.create_task(asyncio.to_thread(load.stream_video,args,0))
        await asyncio.sleep(4)
        try:
            await asyncio.to_thread(self.relationship,'alpha','writer','producer','OPERATION_DELETE')
            try:
                await asyncio.wait_for(upload,15)
                raise AssertionError('Revoked live upload unexpectedly completed')
            except (RuntimeError,BrokenPipeError,ConnectionResetError):
                self.evidence['activeVideoUploadRevocation']=True
        finally:
            await asyncio.to_thread(self.relationship,'alpha','writer','producer','OPERATION_TOUCH')
        print('PASS protected video playlists/chunks, keyframes, and active upload revocation',flush=True)

    def restrictions(self):
        # shared is workspace-visible with no explicit Alice/Bob reader grants.
        for user in ('alice', 'bob'):
            rows = self.query(user, '{records(limit:1000){entity_id}}')['records']
            assert any(row['entity_id'] == 'shared' for row in rows)
        try:
            self.relationship('shared', 'restricted', '*', 'OPERATION_TOUCH')
            for user in ('alice', 'bob'):
                rows = self.query(user, '{records(limit:1000){entity_id}}')['records']
                assert all(row['entity_id'] != 'shared' for row in rows)
            rows = self.query('admin', '{records(limit:1000){entity_id}}')['records']
            assert any(row['entity_id'] == 'shared' for row in rows)
        finally:
            self.relationship('shared', 'restricted', '*', 'OPERATION_DELETE')
        self.evidence['workspaceWideDefaultAndRestrictionToggle'] = True

    async def run(self):
        started = time.monotonic()
        await asyncio.to_thread(self.negative_checks)
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')
        for entity, values in [('alpha', [10, 20, 30]), ('beta', [100, 200, 300]), ('shared', [5])]:
            for value in values:
                await asyncio.to_thread(self.publish, entity, value, self.metric, timestamp, 'shared' if entity != 'shared' else None)
        await asyncio.to_thread(self.publish, 'alpha', None, None, timestamp, 'beta')
        await asyncio.to_thread(self.image_checks)
        await self.video_checks()
        expiry_token=await asyncio.to_thread(self.short_token,'alice',20)
        expiry_task=asyncio.create_task(ws_checks.run_expiry(self.args.url,expiry_token,'demo',45))
        historical = '''query($metric:String!){metrics(limit:1000){entity_id metric sample_count sum_value} metricTotals(metric:$metric){metric sample_count sum_value} records(limit:1000){entity_id} schemas{name}}'''
        expected = {'alice': (65, {'alpha', 'shared'}, 'beta'), 'bob': (605, {'beta', 'shared'}, 'alpha'), 'admin': (665, {'alpha', 'beta', 'shared'}, None)}
        deadline = time.monotonic() + self.args.deadline
        last = 'Waiting for secure checkpoint and query schema refresh'
        while time.monotonic() < deadline:
            try:
                await asyncio.to_thread(self.publish, 'shared', 0, 'security-watermark')
                for user, (total, allowed, denied) in expected.items():
                    result = await asyncio.to_thread(self.query, user, historical, {'metric': self.metric})
                    rows = [row for row in result['metrics'] if row['metric'] == self.metric]
                    assert {row['entity_id'] for row in rows} == allowed, f'{user}: allowed metric entities not committed'
                    assert result['metricTotals'] and result['metricTotals'][0]['sum_value'] == total, f'{user}: aggregate included missing or forbidden contributors'
                    assert all(row['entity_id'] != denied for row in result['records']), f'{user}: forbidden resource row'
                    assert all(row['name'].startswith('context_secure.') for row in result['schemas'])
                break
            except Exception as error:
                last = str(error)
                await asyncio.sleep(3)
        else:
            raise AssertionError('Secure historical checks deadline exceeded: ' + last)
        self.evidence['authorizationBeforeSqlAggregation'] = {'alice': 65, 'bob': 605, 'admin': 665}
        self.evidence['registeredSchemas'] = True
        for user, forbidden in [('alice', 'beta'), ('bob', 'alpha')]:
            raw = await asyncio.to_thread(self.query, user, '{records(limit:1000){entity_id}}')
            rows = raw['records']
            assert rows and all(row['entity_id'] != forbidden for row in rows)
        self.evidence['configuredFieldsRespectScope'] = True
        await asyncio.to_thread(self.restrictions)
        print('PASS Iceberg queries, authorized SQL aggregates, schema rows, configured fields and workspace visibility policy', flush=True)

        async def publish_phase(phase):
            await asyncio.to_thread(self.publish, 'alpha', phase, 'security-ws-' + self.metric)
            # Wait past the event-time window while keeping another partition active.
            for _ in range(8):
                await asyncio.sleep(2)
                await asyncio.to_thread(self.publish, 'shared', 0, 'security-watermark')

        async def revoke():
            await asyncio.to_thread(self.relationship, 'alpha', 'reader', 'alice', 'OPERATION_DELETE')

        async def restore():
            await asyncio.to_thread(self.relationship, 'alpha', 'reader', 'alice', 'OPERATION_TOUCH')

        self.tokens.pop('alice', None)
        self.tokens.pop('bob', None)
        alice_token = await asyncio.to_thread(self.token, 'alice')
        bob_token = await asyncio.to_thread(self.token, 'bob')
        self.evidence['liveSubscriptions'] = await ws_checks.run_revocation(self.args.url, alice_token, bob_token, 'demo', 'alpha', publish_phase, revoke, restore)
        self.evidence['liveTokenExpiry']=await expiry_task
        self.evidence['elapsedSeconds'] = round(time.monotonic() - started, 2)
        self.evidence['status'] = 'passed'
        self.evidence['metric'] = self.metric
        Path(self.args.output).write_text(json.dumps(self.evidence, indent=2))
        print('PASS active GraphQL subscription revocation; evidence ' + self.args.output, flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://localhost:18088')
    parser.add_argument('--token-path', default='/auth/token')
    parser.add_argument('--identity-url',default='https://localhost:18444',help='TLS development issuer operator forward for short-lived expiry fixtures')
    parser.add_argument('--spicedb-url', default='https://localhost:18443')
    parser.add_argument('--credentials', default='.runtime/security/credentials.json')
    parser.add_argument('--spice-token', default='.runtime/security/spicedb-token')
    parser.add_argument('--ca', default='.runtime/security/ca.crt')
    parser.add_argument('--deadline', type=int, default=240)
    parser.add_argument('--output', default='.runtime/security-smoke.json')
    args = parser.parse_args()
    if args.url.startswith('http://') and os.environ.get('SECURITY_ALLOW_HTTP') != 'true':
        parser.error('Local HTTP smoke requires explicit SECURITY_ALLOW_HTTP=true')
    os.environ['SECURITY_SMOKE_CA'] = args.ca
    try:
        asyncio.run(Smoke(args).run())
    except Exception as error:
        raise SystemExit('FAIL security smoke: ' + str(error))
