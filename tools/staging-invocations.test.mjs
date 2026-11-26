import assert from 'node:assert/strict';
import test from 'node:test';
import { InvocationCollector } from './staging-invocations.mjs';

const metadata = { service: 'pintle-staging', type: 'cf-worker-event', id: 'shared-log-id' };
const worker = {
  executionModel: 'stateless', eventType: 'fetch', outcome: 'ok', cpuTimeMs: 1, wallTimeMs: 3,
  event: {request: {url: 'https://example.invalid/TryAcquire'}},
};
function event(meta, fields) {
  return {timestamp: 123, $metadata: {...metadata, ...meta}, $workers: {...worker, ...fields}};
}

test('shared log IDs preserve distinct gateway requests and authority invocations across polls', () => {
  const collector = new InvocationCollector();
  const rayA = 'abc0000000000001', rayB = 'abc0000000000002';
  const a = event({rayId: rayA});
  const b = event({rayId: rayB}, {cpuTimeMs: 2, outcome: 'canceled'});
  const object = {executionModel: 'durableObject', durableObjectId: 'object-a'};
  const ns = '1795705790123456789';
  const fetch1 = event({rayId: rayA, requestId: rayA, timestampNs: ns}, object);
  const fetch2 = event({rayId: rayA, requestId: rayA, timestampNs: '1795705790123456790'}, object);
  const alarm = event({timestampNs: ns}, {...object, eventType: 'alarm'});
  collector.add([a, fetch1, b, alarm]);
  collector.add([alarm, fetch2, fetch1, b, a]);
  const report = collector.report();
  assert.equal(collector.gatewayCount, 2);
  assert.equal(report.observed_gateway_events, 2);
  assert.equal(report.observed_authority_invocations, 3);
  assert.equal(report.groups['authority/TryAcquire'].events, 2);
  assert.equal(report.groups['authority/alarm'].events, 1);
  const gateway = report.groups['gateway/TryAcquire'];
  assert.equal(gateway.events, 2);
  assert.equal(gateway.cpu.sum_ms, 3);
  assert.deepEqual(gateway.outcomes, {ok: 1, canceled: 1});
});

test('a CF-Ray header identifies the same gateway request and missing identity fails closed', () => {
  const collector = new InvocationCollector();
  const request = {url: worker.event.request.url, headers: {'cf-ray': 'ABC0000000000001-BEG'}};
  collector.add([event({}, {event: {request}}), event({id: 'another-log-id', rayId: 'abc0000000000001'})]);
  assert.equal(collector.gatewayCount, 1);
  assert.throws(() => collector.add([event({requestId: 'unrelated'})]), /requires a Ray ID/);
  assert.throws(() => collector.add([event({}, {entrypoint: 'PintleAuthority'})]), /Durable Object invocation identity/);
});
