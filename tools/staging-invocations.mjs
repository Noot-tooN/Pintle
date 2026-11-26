const methods = new Set([
  'Acquire', 'TryAcquire', 'Renew', 'Release', 'OpenSession',
  'AcknowledgeSession', 'GetRequestStatus', 'CancelPending',
]);
const outcomes = new Set(['ok', 'canceled', 'exception', 'exceededCpu', 'exceededMemory']);

function gatewayRayId(metadata, worker) {
  const headers = worker.event?.request?.headers;
  const ray = metadata.rayId || headers?.['cf-ray'] || headers?.['CF-Ray'];
  const match = typeof ray === 'string' && /^([0-9a-f]+)(?:-[a-z]{3})?$/i.exec(ray);
  if (!match) throw Error('Gateway invocation requires a Ray ID');
  return match[1].toLowerCase();
}

function authorityKey(event) {
  const { $metadata: metadata, $workers: worker } = event;
  const requestId = metadata.requestId || worker.requestId;
  const time = metadata.timestampNs || event.timestamp;
  // Related calls can share a request ID. Alarms can have no request ID at all.
  if (!worker.eventType || time == null || (!requestId && !worker.durableObjectId)) {
    throw Error('Missing Durable Object invocation identity');
  }
  return JSON.stringify([
    worker.durableObjectId, worker.eventType, requestId, metadata.spanId || worker.spanId,
    time, metadata.id,
  ]);
}

function summarize(values) {
  values.sort((a, b) => a - b);
  if (!values.length) return {samples: 0};
  const percentile = p => values[Math.ceil(values.length * p) - 1];
  return {
    samples: values.length, sum_ms: values.reduce((sum, value) => sum + value, 0),
    p50_ms: percentile(0.5), p95_ms: percentile(0.95), p99_ms: percentile(0.99),
    max_ms: values.at(-1),
  };
}

export class InvocationCollector {
  #gateway = new Map();
  #authority = new Map();

  get gatewayCount() { return this.#gateway.size; }

  add(events) {
    for (const event of events) {
      const { $metadata: metadata, $workers: worker } = event;
      if (metadata?.service !== 'pintle-staging' || metadata.type !== 'cf-worker-event' ||
          !metadata.id || !worker) throw Error('Unexpected telemetry source');
      if (worker.truncated) throw Error('Truncated invocation event');

      const authority = worker.executionModel === 'durableObject' ||
        worker.entrypoint === 'PintleAuthority' || Boolean(worker.durableObjectId);
      const records = authority ? this.#authority : this.#gateway;
      const key = authority ? authorityKey(event) : gatewayRayId(metadata, worker);
      const method = worker.event?.request?.url?.split('/').at(-1);
      const operation = worker.eventType === 'alarm' ? 'alarm' : methods.has(method) ? method : 'other';
      records.set(key, {
        group: (authority ? 'authority/' : 'gateway/') + operation,
        cpu: worker.cpuTimeMs, wall: worker.wallTimeMs,
        outcome: outcomes.has(worker.outcome) ? worker.outcome : 'other',
      });
      if (this.#gateway.size + this.#authority.size >= 2000) throw Error('Telemetry union exceeds event budget');
    }
  }

  report() {
    const groups = {};
    for (const records of [this.#gateway, this.#authority]) {
      for (const sample of records.values()) {
        const group = groups[sample.group] ??= {events: 0, cpu: [], wall: [], outcomes: {}};
        group.events++;
        group.outcomes[sample.outcome] = (group.outcomes[sample.outcome] ?? 0) + 1;
        for (const field of ['cpu', 'wall']) {
          if (Number.isFinite(sample[field]) && sample[field] >= 0) group[field].push(sample[field]);
        }
      }
    }
    for (const group of Object.values(groups)) {
      group.cpu = summarize(group.cpu);
      group.wall = summarize(group.wall);
    }
    return {
      observed_gateway_events: this.#gateway.size,
      observed_authority_invocations: this.#authority.size,
      groups,
    };
  }
}
