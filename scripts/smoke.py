#!/usr/bin/env python3
"""Full ingestion -> Kafka -> Flink -> Iceberg -> GraphQL/media smoke.
Requires running services and FFmpeg/ffprobe; websocket checks need websockets>=14.
"""
import argparse
import asyncio
import contextlib
import datetime as dt
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('cg_load', ROOT / 'generators/load.py')
load = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(load)


def graphql(origin, query, variables=None):
    request = urllib.request.Request(origin.rstrip('/') + '/graphql',
        data=json.dumps({'query': query, 'variables': variables or {}}).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=20) as response:
        result = json.load(response)
    if result.get('errors'):
        raise RuntimeError('GraphQL errors: ' + json.dumps(result['errors']))
    return result['data']


def fetch(origin, uri):
    with urllib.request.urlopen(origin.rstrip('/') + uri, timeout=15) as response:
        return response.read()


def object_payload(value):
    return json.loads(value) if isinstance(value, str) else value


def verify_video(origin, result):
    playlist = fetch(origin, result['playlistUri']).decode()
    if '#EXT-X-INDEPENDENT-SEGMENTS' not in playlist or '#EXT-X-ENDLIST' not in playlist:
        raise AssertionError('Video playlist is not independently decodable and finalized: ' + playlist)
    names = [line for line in playlist.splitlines() if line and not line.startswith('#')]
    if len(names) < 2:
        raise AssertionError(f'Expected multiple video segments, got {names}')
    with tempfile.TemporaryDirectory(prefix='cg-smoke-') as directory:
        for index, name in enumerate(names):
            chunk = fetch(origin, result['playlistUri'].rsplit('/', 1)[0] + '/' + name)
            local = Path(directory) / f'{index}.ts'
            local.write_bytes(chunk)
            probe = subprocess.run(['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'frame=key_frame',
                                    '-of', 'csv=p=0', str(local)], capture_output=True, text=True, timeout=20, check=True)
            if not probe.stdout.startswith('1'):
                raise AssertionError(f'Segment {name} does not start on a keyframe: {probe.stdout[:100]}')
            decode = subprocess.run(['ffmpeg', '-v', 'error', '-i', str(local), '-f', 'null', '-'], capture_output=True, timeout=20)
            if decode.returncode or decode.stderr:
                raise AssertionError(f'Segment {name} cannot decode independently: {decode.stderr.decode()[:500]}')
    return len(names)


async def run(args):
    started = time.monotonic()
    deadline = started + args.deadline
    entity = 'smoke-' + uuid.uuid4().hex[:12]
    target = entity + '-room'
    live = {'metrics': [], 'video': []}
    ws = None
    receiver = None
    receiver_error = []
    for origin in [args.ingestion, args.query]:
        await asyncio.to_thread(fetch, origin, '/health/ready')
    if not args.skip_subscriptions:
        try:
            from websockets.asyncio.client import connect
        except ImportError as exc:
            raise RuntimeError('Install websockets>=14 or explicitly use --skip-subscriptions') from exc
        url = args.query.replace('https://', 'wss://').replace('http://', 'ws://').rstrip('/') + '/graphql'
        ws = await connect(url, subprotocols=['graphql-transport-ws'])
        await ws.send(json.dumps({'type': 'connection_init'}))
        ack = json.loads(await asyncio.wait_for(ws.recv(), 10))
        if ack.get('type') != 'connection_ack':
            raise AssertionError(f'Invalid websocket acknowledgement: {ack}')
        for operation, query, selected in [
            ('metrics', 'subscription($entity:String){metricUpdated(entityId:$entity){entity_id metric sample_count avg_value}}', entity),
            ('video', 'subscription($entity:String){videoChunk(entityId:$entity){entityId payload{streamId sequence uri playlistUri keyframeAligned}}}', 'camera-0000')]:
            await ws.send(json.dumps({'id': operation, 'type': 'subscribe', 'payload': {'query': query, 'variables': {'entity': selected}}}))

        async def receive():
            try:
                async for raw in ws:
                    message = json.loads(raw)
                    if message.get('type') == 'ping':
                        await ws.send(json.dumps({'type': 'pong', 'payload': message.get('payload')}))
                    elif message.get('type') == 'error' or message.get('payload', {}).get('errors'):
                        raise AssertionError(f'Subscription error: {message}')
                    elif message.get('type') == 'next':
                        live[message['id']].append(message['payload']['data'])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                receiver_error.append(str(exc))
        receiver = asyncio.create_task(receive())
        await asyncio.sleep(0.5)
    try:
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')
        for value in [10, 20, 30]:
            payload = {'entityId': entity, 'eventTime': timestamp, 'metric': 'smoke', 'value': value, 'relatedTo': target,
                       'unknownNested': {'preserved': True}}
            await asyncio.to_thread(load.upload, args.ingestion, '/ingest/events', json.dumps(payload).encode(), 'application/json')
        print('PASS JSON ingestion acknowledged', flush=True)
        image = await asyncio.to_thread(load.image_bytes, load.parse_args(['images', '--width', '160', '--height', '120']))
        image_event = await asyncio.to_thread(load.upload, args.ingestion, '/ingest/images', image, 'image/png', entity)
        if await asyncio.to_thread(fetch, args.ingestion, image_event['payload']['uri']) != image:
            raise AssertionError('Stored image differs from uploaded bytes')
        print('PASS image metadata and stored bytes', flush=True)
        video_args = load.parse_args(['video', '--url', args.ingestion, '--stream-seconds', '5', '--width', '160', '--height', '120', '--gop', '97', '--no-realtime'])
        video = await asyncio.to_thread(load.stream_video, video_args, 0)
        count = await asyncio.to_thread(verify_video, args.ingestion, video)
        print(f'PASS video: {count} independently decodable keyframe chunks', flush=True)
        # Advance event-time past the 10-second window plus configured watermark delay.
        await asyncio.sleep(max(0, 15 - (time.monotonic() - started)))
        for index in range(20):
            marker = {'entityId': entity + '-advance-' + str(index), 'eventTime': dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z'), 'metric': 'advance', 'value': 1}
            await asyncio.to_thread(load.upload, args.ingestion, '/ingest/events', json.dumps(marker).encode(), 'application/json')
        query = '''query($entity:String,$camera:String){
          metrics(entityId:$entity){entity_id metric sample_count avg_value sum_value}
          nodes(limit:10000){node_id}
          edges(limit:10000){source_id target_id}
          images:media(entityId:$entity){event_id kind payload}
          videos:media(entityId:$camera){event_id kind payload}
        }'''
        last = {}
        advance_sequence = 0
        while time.monotonic() < deadline:
            try:
                # Keep event time advancing even when a restore consumes the earlier
                # marker batch at once and every source partition becomes idle.
                marker = {'entityId': entity + '-tick-' + str(advance_sequence % 20),
                          'eventTime': dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z'),
                          'metric': 'advance', 'value': 1}
                advance_sequence += 1
                await asyncio.to_thread(load.upload, args.ingestion, '/ingest/events', json.dumps(marker).encode(), 'application/json')
                data = await asyncio.to_thread(graphql, args.query, query, {'entity': entity, 'camera': 'camera-0000'})
                last = {
                    'metrics': any(row['metric'] == 'smoke' and row['sample_count'] == 3 and abs(row['avg_value']-20) < 1e-8 and abs(row['sum_value']-60) < 1e-8 for row in data['metrics']),
                    'node': any(row['node_id'] == entity for row in data['nodes']),
                    'edge': any(row['source_id'] == entity and row['target_id'] == target for row in data['edges']),
                    'image': any(row['event_id'] == image_event['eventId'] for row in data['images']),
                    'video': any(object_payload(row['payload']).get('streamId') == video['streamId'] for row in data['videos']),
                    'liveMetric': args.skip_subscriptions or any(row['metricUpdated']['entity_id'] == entity for row in live['metrics']),
                    'liveVideo': args.skip_subscriptions or any(row['videoChunk']['payload']['streamId'] == video['streamId'] for row in live['video'])}
                if receiver_error:
                    raise AssertionError(receiver_error)
                if all(last.values()):
                    print('PASS Kafka -> Flink -> Iceberg -> GraphQL metrics, nodes, edges and media; live subscriptions=' + str(not args.skip_subscriptions), flush=True)
                    print(json.dumps({'entity': entity, 'streamId': video['streamId'], 'checks': last, 'elapsedSeconds': round(time.monotonic()-started, 2)}))
                    return
            except Exception as exc:
                last = {'queryError': str(exc), 'subscriptionErrors': receiver_error}
            print('WAIT committed snapshots: ' + json.dumps(last), flush=True)
            await asyncio.sleep(min(3, max(0, deadline-time.monotonic())))
        raise TimeoutError('Pipeline smoke deadline exceeded. Check processor health, checkpoint commits, watermarks and query catalog refresh. Last checks: ' + json.dumps(last))
    finally:
        if receiver:
            receiver.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await receiver
        if ws:
            await ws.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ingestion', default='http://localhost:8080')
    parser.add_argument('--query', default='http://localhost:8081')
    parser.add_argument('--deadline', type=float, default=240)
    parser.add_argument('--skip-subscriptions', action='store_true', help='Explicitly skip live checks; historical checks still required')
    args = parser.parse_args()
    if args.deadline < 30:
        parser.error('--deadline must be at least 30 seconds')
    if not shutil.which('ffmpeg') or not shutil.which('ffprobe'):
        parser.error('ffmpeg and ffprobe must be installed')
    try:
        asyncio.run(asyncio.wait_for(run(args), timeout=args.deadline + 10))
    except Exception as exc:
        raise SystemExit('FAIL: ' + str(exc))
