#!/usr/bin/env python3
"""Verified JWT + real SpiceDB GraphQL websocket revocation checks.

Import run_revocation from the full pipeline smoke. Callbacks perform real data
publication and SpiceDB writes. The helper never grants permissions itself.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import ssl
from urllib.parse import urlsplit, urlunsplit
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


def websocket_url(base_url):
    parsed = urlsplit(base_url)
    scheme = {'https': 'wss', 'http': 'ws', 'wss': 'wss', 'ws': 'ws'}[parsed.scheme]
    path = parsed.path.rstrip('/')
    if not path.endswith('/graphql'):
        path += '/graphql'
    return urlunsplit((scheme, parsed.netloc, path, '', ''))


def transport_options(url):
    if url.startswith('wss://'):
        ca = os.environ.get('SECURITY_SMOKE_CA', '.runtime/security/ca.crt')
        return {'ssl': ssl.create_default_context(cafile=ca)}
    if os.environ.get('SECURITY_ALLOW_HTTP') != 'true':
        raise ValueError('HTTP websocket smoke requires explicit SECURITY_ALLOW_HTTP=true')
    return {}


async def receive(ws, timeout=75):
    message = json.loads(await asyncio.wait_for(ws.recv(), timeout))
    if message.get('type') == 'ping':
        await ws.send(json.dumps({'type': 'pong', 'payload': message.get('payload')}))
        return await receive(ws, timeout)
    return message


async def initialize(ws, token, workspace):
    await ws.send(json.dumps({'type': 'connection_init', 'payload': {
        'authorization': token if token.startswith('Bearer ') else 'Bearer ' + token,
        'workspaceId': workspace}}))
    message = await receive(ws, 15)
    assert message.get('type') == 'connection_ack', 'Verified connection was not acknowledged'


async def subscribe(ws, operation, entity):
    await ws.send(json.dumps({'id': operation, 'type': 'subscribe', 'payload': {
        'query': 'subscription($entity:String){metricUpdated(entityId:$entity){entity_id metric avg_value window_end}}',
        'variables': {'entity': entity}}}))


async def run_revocation(base_url, alice_token, bob_token, workspace, restricted_entity,
                         publish_callback, revoke_callback, restore_callback):
    """Require visibility, disjoint-user denial, then active-stream revocation.

    publish_callback(phase) must produce a fresh committed metrics event for the
    tested entity (including event-time advancement/checkpoints when using Flink).
    revoke_callback() removes Alice's explicit read grant from a restricted entity;
    restore_callback() restores it, even if an assertion fails. No credentials are
    included in returned evidence or error messages.
    """
    url = websocket_url(base_url)
    options = dict(subprotocols=['graphql-transport-ws'], max_size=1048576,
                   open_timeout=15, **transport_options(url))
    revoked = False
    try:
        async with connect(url, **options) as alice, connect(url, **options) as bob:
            await initialize(alice, alice_token, workspace)
            await initialize(bob, bob_token, workspace)
            await subscribe(alice, 'allowed', restricted_entity)
            await subscribe(bob, 'denied', restricted_entity)
            denied = await receive(bob, 15)
            assert denied.get('id') == 'denied' and denied.get('type') == 'error', 'Disjoint user subscription was not denied'
            await publish_callback(1)
            first = await receive(alice)
            assert first.get('type') == 'next' and first.get('id') == 'allowed', 'Authorized subscriber received no data'
            data = first.get('payload', {}).get('data', {}).get('metricUpdated')
            assert data and data.get('entity_id') == restricted_entity, 'Authorized event routed incorrectly'
            # Drain frames authorized before revocation; delivered bytes cannot be retracted.
            while True:
                try:
                    await asyncio.wait_for(alice.recv(), .1)
                except asyncio.TimeoutError:
                    break
            revoked = True
            await revoke_callback()
            await publish_callback(2)
            try:
                message = await receive(alice)
                assert message.get('type') == 'error' and message.get('id') == 'allowed', 'Revoked subscription emitted data or failed to terminate'
                terminal = 'operation_error'
            except ConnectionClosed as closed:
                assert closed.code in (4401, 4403), 'Unexpected websocket termination'
                terminal = 'authorization_close'
            return {'authorizedDelivery': True, 'disjointUserDenied': True,
                    'activeRevocation': True, 'revocationOutcome': terminal}
    finally:
        if revoked:
            await restore_callback()


async def run_expiry(base_url, token, workspace, timeout=45):
    """The caller supplies a genuinely signed JWT expiring within timeout seconds."""
    url = websocket_url(base_url)
    async with connect(url, subprotocols=['graphql-transport-ws'], **transport_options(url)) as ws:
        await initialize(ws, token, workspace)
        try:
            while True:
                await asyncio.wait_for(ws.recv(), timeout)
        except ConnectionClosed as closed:
            assert closed.code == 4401, 'Expiry did not close with authentication status'
            return {'activeExpiry': True, 'closeCode': closed.code}


def token_file(path):
    text = Path(path).read_text().strip()
    if text.startswith('{'):
        return json.loads(text)['access_token']
    return text


async def main(args):
    actions = json.loads(Path(args.actions_file).read_text())

    async def command(name, phase=None):
        argv = actions[name]
        if not isinstance(argv, list) or not argv or not all(isinstance(x, str) for x in argv):
            raise ValueError('Action commands must be nonempty JSON argv arrays')
        argv = [part.replace('{phase}', str(phase)) for part in argv]
        process = await asyncio.create_subprocess_exec(*argv, stdout=asyncio.subprocess.DEVNULL,
                                                       stderr=asyncio.subprocess.DEVNULL)
        try:
            code = await asyncio.wait_for(process.wait(), 90)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            raise
        if code:
            raise RuntimeError(name + ' action failed')

    result = await run_revocation(args.endpoint, token_file(args.alice_token_file),
        token_file(args.bob_token_file), args.workspace, args.entity,
        lambda phase: command('publish', phase), lambda: command('revoke'), lambda: command('restore'))
    if args.expiring_token_file:
        result.update(await run_expiry(args.endpoint, token_file(args.expiring_token_file), args.workspace))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--alice-token-file', required=True)
    parser.add_argument('--bob-token-file', required=True)
    parser.add_argument('--workspace', required=True)
    parser.add_argument('--entity', required=True)
    parser.add_argument('--actions-file', required=True, help='JSON argv arrays: publish/revoke/restore; publish can use {phase}')
    parser.add_argument('--expiring-token-file')
    asyncio.run(main(parser.parse_args()))
