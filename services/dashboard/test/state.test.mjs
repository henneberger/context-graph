import test from 'node:test';
import assert from 'node:assert/strict';
import {mergeMetrics, mediaUrl, websocketUrl} from '../public/state.js';
test('replay replaces same window and respects retained history bound',()=>{
 const m={entity_id:'a',metric:'t',window_start:'2026-01-01',window_end:'2026-01-02',avg_value:1};
 assert.equal(mergeMetrics([m],[{...m,avg_value:2}])[0].avg_value,2);
 assert.equal(mergeMetrics([m],[{...m,entity_id:'b'}],1).length,1);
});
test('media URIs stay on local proxy',()=>{assert.equal(mediaUrl('https://example.com/video'),null);assert.equal(mediaUrl('/media/../secret'),null);assert.equal(mediaUrl('/media/video/a/index.m3u8'),'/media/video/a/index.m3u8');});
test('secure pages use secure modern websocket transport',()=>assert.equal(websocketUrl({protocol:'https:',host:'example.com'}),'wss://example.com/graphql'));
