#!/usr/bin/env python3
"""Real Kafka -> graphql-transport-ws routing/cancellation smoke.
Install scripts/query-smoke-requirements.txt in a venv; query and Kafka must run.
This publishes test metrics directly, independently of Flink and ingestion.
"""
import argparse
import asyncio
import json
import time
import subprocess
import uuid
from datetime import datetime, timezone
from urllib.request import urlopen
from kafka import KafkaProducer
from websockets.asyncio.client import connect


def metric(entity, value):
    end = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    return dict(entity_id=entity, metric='query-smoke', window_start=end,
                window_end=end, sample_count=1, sum_value=value, avg_value=value,
                min_value=value, max_value=value)


async def run(args):
    producer = None if args.kube_context else KafkaProducer(bootstrap_servers=args.kafka,
                            value_serializer=lambda value: json.dumps(value).encode(),
                            acks='all', request_timeout_ms=10000, max_block_ms=10000)
    try:
        health = args.url.replace('ws://', 'http://').replace('wss://', 'https://').rsplit('/graphql', 1)[0] + '/health/ready'
        with urlopen(health, timeout=5) as response:
            assert response.status == 200, 'Query service not ready'
        prefix = 'query-smoke-' + uuid.uuid4().hex
        a, b = prefix + '-a', prefix + '-b'
        async with connect(args.url, subprotocols=['graphql-transport-ws'], max_size=1048576) as ws:
            assert ws.subprotocol == 'graphql-transport-ws'
            await ws.send(json.dumps(dict(type='connection_init')))
            assert json.loads(await asyncio.wait_for(ws.recv(), 5))['type'] == 'connection_ack'

            async def subscribe(operation, entity):
                await ws.send(json.dumps(dict(id=operation, type='subscribe', payload=dict(
                    query='subscription($entity:String){metricUpdated(entityId:$entity){entity_id metric avg_value}}',
                    variables=dict(entity=entity)))))

            async def publish(entity, value):
                if producer is not None:
                    await asyncio.to_thread(lambda: producer.send('cg.metrics', key=entity.encode(), value=metric(entity, value)).get(timeout=10))
                else:
                    command = ['kubectl', '--context', args.kube_context, '-n', args.namespace,
                        'exec', '-i', 'kafka-0', '--', '/opt/kafka/bin/kafka-console-producer.sh',
                        '--bootstrap-server', 'kafka:9092', '--topic', 'cg.metrics',
                        '--property', 'parse.key=true', '--property', 'key.separator=\t',
                        '--producer-property', 'acks=all']
                    await asyncio.to_thread(subprocess.run, command, input=entity+'\t'+json.dumps(metric(entity,value))+'\n',
                        text=True, check=True, capture_output=True, timeout=30)

            await subscribe('a', a)
            await subscribe('b', b)
            # Protocol has no per-operation subscription ACK; allow installation to settle.
            await asyncio.sleep(0.5)
            await publish(a, 11)
            await publish(b, 22)
            received = {}
            deadline = time.monotonic() + 15
            while len(received) < 2:
                message = json.loads(await asyncio.wait_for(ws.recv(), max(.1, deadline-time.monotonic())))
                assert message['type'] == 'next', message
                data = message['payload']['data']['metricUpdated']
                assert data['entity_id'] == {'a': a, 'b': b}[message['id']], message
                assert data['avg_value'] == {'a': 11, 'b': 22}[message['id']], message
                received[message['id']] = data

            await ws.send(json.dumps(dict(id='a', type='complete')))
            await ws.send(json.dumps(dict(type='ping', payload=dict(fence='cancel'))))
            message = json.loads(await asyncio.wait_for(ws.recv(), 5))
            assert message == dict(type='pong', payload=dict(fence='cancel')), message
            await publish(a, 33)
            await publish(b, 44)
            message = json.loads(await asyncio.wait_for(ws.recv(), 10))
            assert message['id'] == 'b' and message['payload']['data']['metricUpdated']['avg_value'] == 44, message
            try:
                unexpected = await asyncio.wait_for(ws.recv(), 1)
                raise AssertionError('Cancelled subscription received data: ' + unexpected)
            except asyncio.TimeoutError:
                pass
            # Reuse an operation ID after complete, now for a different entity.
            await subscribe('a', b)
            await asyncio.sleep(.3)
            await publish(b, 55)
            ids = set()
            while len(ids) < 2:
                message = json.loads(await asyncio.wait_for(ws.recv(), 10))
                assert message['payload']['data']['metricUpdated']['entity_id'] == b, message
                assert message['payload']['data']['metricUpdated']['avg_value'] == 55, message
                ids.add(message['id'])
            assert ids == {'a', 'b'}
            for operation in ids:
                await ws.send(json.dumps(dict(id=operation, type='complete')))
        print('PASS: Kafka -> GraphQL WS, two entity routes, ping/pong, cancellation, operation ID reuse')
    finally:
        if producer is not None:
            producer.close(timeout=5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='ws://localhost:8081/graphql')
    parser.add_argument('--kafka', default='localhost:19092')
    parser.add_argument('--kube-context', help='Publish through kafka-0 in this explicit Kubernetes context')
    parser.add_argument('--namespace', default='context-graph')
    asyncio.run(run(parser.parse_args()))
