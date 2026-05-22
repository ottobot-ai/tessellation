/**
 * awaitChainEvent — reactive event-stream client for gl0's LocalEvents gRPC service.
 *
 * Replaces polling-based test waits (e.g. `for (i=0; i<300; i++) { poll(); sleep(1000); }`)
 * with subscription to the gl0 process's event stream. Each Subscribe stream begins with
 * a StreamStarted envelope carrying the current finalized ordinal so the test can bridge
 * to REST `/snapshots/at-ordinal/N` if it needs a snapshot view at the subscribe instant
 * without missing any subsequent delta.
 *
 * Design references:
 * - p2p/proto/local_events.proto                       (wire schema)
 * - docs/nakamoto/LOCAL-EVENTS-SERVICE-DESIGN.md       (Section 10 — Test-Client Helper Shape)
 *
 * Failure semantics: when `maxWait` elapses without the predicate firing, the helper
 * throws a STRUCTURED error containing:
 *   - the configured tag (so a failure in CI logs identifies the call site at a glance)
 *   - the filter that was applied
 *   - elapsed time
 *   - the last 10 envelopes the helper actually saw (diagnostic dump)
 * The error never silently hangs — the maxWait timer always fires.
 *
 * The helper is a thin facade over gRPC; convenience wrappers
 * (awaitTokenLockExpired / awaitBalanceMatches / awaitTransactionAccepted /
 * awaitSnapshotOrdinal) configure the right filter + predicate for the common cases.
 *
 * The helper IS idempotent w.r.t. subscription teardown: success, predicate exception,
 * and timeout all close the stream cleanly via `call.cancel()`.
 */

const path = require('path')
const grpc = require('@grpc/grpc-js')
const protoLoader = require('@grpc/proto-loader')

const PROTO_PATH = path.resolve(__dirname, '..', '..', '..', 'p2p', 'proto', 'local_events.proto')

let _cachedProtoDescriptor = null
const loadProto = () => {
  if (_cachedProtoDescriptor) return _cachedProtoDescriptor
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false, // snake_case → camelCase per JS convention
    longs: String,   // int64 / uint64 as JS strings (safer than Number for 2^53+)
    enums: String,   // enum values as their string names — predicate code reads cleaner
    defaults: true,
    oneofs: true,
    bytes: Buffer
  })
  _cachedProtoDescriptor = grpc.loadPackageDefinition(packageDef)
  return _cachedProtoDescriptor
}

const EventKind = Object.freeze({
  STREAM_STARTED: 'STREAM_STARTED',
  SNAPSHOT_FINALIZED: 'SNAPSHOT_FINALIZED',
  BALANCE_CHANGE: 'BALANCE_CHANGE',
  TOKEN_LOCK_STATE_CHANGE: 'TOKEN_LOCK_STATE_CHANGE',
  ALLOW_SPEND_STATE_CHANGE: 'ALLOW_SPEND_STATE_CHANGE',
  TRANSACTION_ACCEPTED: 'TRANSACTION_ACCEPTED',
  METAGRAPH_SNAPSHOT_ACCEPTED: 'METAGRAPH_SNAPSHOT_ACCEPTED',
  METAGRAPH_BALANCE_CHANGE: 'METAGRAPH_BALANCE_CHANGE'
})

/**
 * Parse a maxWait specification into milliseconds.
 *   '30min'  → 30 * 60 * 1000
 *   '90s'    → 90 * 1000
 *   '500ms'  → 500
 *    12345   → 12345                  (already ms)
 */
const parseMaxWait = (raw) => {
  if (typeof raw === 'number') return raw
  if (typeof raw !== 'string') {
    throw new Error(`awaitChainEvent: maxWait must be number or duration string, got ${typeof raw}`)
  }
  const m = raw.match(/^(\d+(?:\.\d+)?)(ms|s|min|m|h)?$/i)
  if (!m) throw new Error(`awaitChainEvent: invalid duration string "${raw}"`)
  const n = parseFloat(m[1])
  const unit = (m[2] || 'ms').toLowerCase()
  switch (unit) {
    case 'ms': return Math.round(n)
    case 's':  return Math.round(n * 1000)
    case 'm':
    case 'min': return Math.round(n * 60 * 1000)
    case 'h':  return Math.round(n * 60 * 60 * 1000)
    default:   throw new Error(`awaitChainEvent: unknown unit "${unit}"`)
  }
}

/**
 * Resolve a gl0 endpoint for the test container.
 *
 * Lookup order:
 *   1. opts.endpoint  (caller-supplied; takes precedence)
 *   2. LOCAL_EVENTS_ENDPOINT env (e2e harness writes this)
 *   3. ${TEST_HOST_HOST}:${LOCAL_EVENTS_PORT}  (deconstructed override)
 *   4. localhost:50054   (default — docker-compose host-port mapping)
 *
 * Note: TEST_HOST in the existing test scripts is typically `http://localhost`, so we
 * strip the scheme when composing the gRPC endpoint.
 */
const resolveEndpoint = (override) => {
  if (override) return override
  if (process.env.LOCAL_EVENTS_ENDPOINT) return process.env.LOCAL_EVENTS_ENDPOINT
  const port = process.env.LOCAL_EVENTS_PORT || '50054'
  // TEST_HOST defaults to http://localhost. Strip scheme + trailing slash → hostname-only.
  const rawHost = process.env.LOCAL_EVENTS_HOST || process.env.TEST_HOST || 'http://localhost'
  const host = rawHost.replace(/^https?:\/\//, '').replace(/\/$/, '').split(':')[0] || 'localhost'
  return `${host}:${port}`
}

/**
 * Convert an EventEnvelope into the shape the user-facing predicate sees. The grpc-js
 * dynamic loader gives us a "oneof" key on each envelope (`envelope.event` === one of
 * the field names); we surface that as `e.kind` (string name) plus the inner payload
 * fields hoisted directly onto the envelope for ergonomic predicates:
 *
 *   if (e.kind === 'TOKEN_LOCK_STATE_CHANGE' && e.tokenLockStateChange.transition === 'EXPIRED')
 *
 * The raw envelope is preserved on `e._raw` for advanced predicates that need it.
 */
const normalizeEnvelope = (env) => {
  const oneofKey = env.event // proto-loader sets oneofs:true → this is the active variant field name
  const kindFromOneof = (() => {
    switch (oneofKey) {
      case 'streamStarted':              return EventKind.STREAM_STARTED
      case 'snapshotFinalized':          return EventKind.SNAPSHOT_FINALIZED
      case 'balanceChange':              return EventKind.BALANCE_CHANGE
      case 'tokenLockStateChange':       return EventKind.TOKEN_LOCK_STATE_CHANGE
      case 'allowSpendStateChange':      return EventKind.ALLOW_SPEND_STATE_CHANGE
      case 'transactionAccepted':        return EventKind.TRANSACTION_ACCEPTED
      case 'metagraphSnapshotAccepted':  return EventKind.METAGRAPH_SNAPSHOT_ACCEPTED
      case 'metagraphBalanceChange':     return EventKind.METAGRAPH_BALANCE_CHANGE
      default:                           return null
    }
  })()
  return {
    kind: kindFromOneof,
    seq: env.seq,
    ordinal: env.ordinal,
    publishedAtMillis: env.publishedAtMillis,
    streamStarted:             env.streamStarted,
    snapshotFinalized:         env.snapshotFinalized,
    balanceChange:             env.balanceChange,
    tokenLockStateChange:      env.tokenLockStateChange,
    allowSpendStateChange:     env.allowSpendStateChange,
    transactionAccepted:       env.transactionAccepted,
    metagraphSnapshotAccepted: env.metagraphSnapshotAccepted,
    metagraphBalanceChange:    env.metagraphBalanceChange,
    _raw: env
  }
}

/**
 * Compact dump of an envelope for diagnostic error messages — strips Buffer fields
 * down to short hex prefixes so the dump stays readable in CI logs.
 */
const compactEnvelope = (e) => {
  const out = { kind: e.kind, seq: e.seq, ordinal: e.ordinal }
  if (e.streamStarted) {
    out.streamStarted = {
      startOrdinal: e.streamStarted.startOrdinal,
      epochProgress: e.streamStarted.epochProgress,
      serverSessionId: e.streamStarted.serverSessionId
    }
  }
  if (e.snapshotFinalized) {
    out.snapshotFinalized = {
      finalizedOrdinal: e.snapshotFinalized.finalizedOrdinal,
      slot: e.snapshotFinalized.slot
    }
  }
  if (e.balanceChange) out.balanceChange = { ...e.balanceChange }
  if (e.tokenLockStateChange) {
    const t = e.tokenLockStateChange
    out.tokenLockStateChange = {
      address: t.address,
      transition: t.transition,
      amount: t.amount,
      unlockEpoch: t.unlockEpoch,
      tokenLockRef: t.tokenLockRef ? Buffer.from(t.tokenLockRef).toString('hex').slice(0, 16) + '…' : null
    }
  }
  if (e.allowSpendStateChange) {
    const t = e.allowSpendStateChange
    out.allowSpendStateChange = {
      address: t.address,
      transition: t.transition,
      amount: t.amount,
      destination: t.destination,
      expiryEpoch: t.expiryEpoch
    }
  }
  if (e.transactionAccepted) {
    const t = e.transactionAccepted
    out.transactionAccepted = {
      source: t.source,
      destination: t.destination,
      amount: t.amount,
      fee: t.fee,
      txHash: t.txHash ? Buffer.from(t.txHash).toString('hex').slice(0, 16) + '…' : null
    }
  }
  if (e.metagraphSnapshotAccepted) {
    out.metagraphSnapshotAccepted = {
      metagraphAddress: e.metagraphSnapshotAccepted.metagraphAddress,
      metagraphOrdinal: e.metagraphSnapshotAccepted.metagraphOrdinal,
      gl0Ordinal: e.metagraphSnapshotAccepted.gl0Ordinal
    }
  }
  if (e.metagraphBalanceChange) out.metagraphBalanceChange = { ...e.metagraphBalanceChange }
  return out
}

/**
 * Source-agnostic core: subscribes via an event source (real gRPC by default, or a
 * caller-supplied mock for unit tests) and runs the predicate over each event after
 * StreamStarted. Returns the matching envelope; rejects with structured error on timeout.
 *
 * The mock injection point is `opts._eventSource(filter, clientTag)` which must return
 * an object exposing { on(eventName, cb), cancel(): void } where eventName ∈ {'data','error','end'}.
 * This shape exactly mirrors a grpc-js ClientReadableStream.
 *
 * @param {Object} opts
 * @param {string} [opts.endpoint] — host:port; resolved via env if absent
 * @param {Object} [opts.filters] — { kinds: ['TOKEN_LOCK_STATE_CHANGE'], addresses: [...], metagraphAddresses: [...], minOrdinal, maxOrdinal }
 * @param {Function} opts.predicate — (event) => bool; event is a normalized envelope
 * @param {string|number} [opts.maxWait='30min'] — duration string or millis
 * @param {string} [opts.tag] — diagnostic label
 * @param {Function} [opts._eventSource] — test-only injection
 * @param {boolean} [opts.requireStreamStarted=true] — if true, the FIRST envelope must
 *   be StreamStarted and the predicate runs ONLY against subsequent envelopes.
 */
const awaitChainEvent = async (opts) => {
  if (!opts || typeof opts.predicate !== 'function') {
    throw new Error('awaitChainEvent: opts.predicate (Function) is required')
  }
  const tag = opts.tag || 'awaitChainEvent'
  const maxWaitMs = parseMaxWait(opts.maxWait || '30min')
  const requireStreamStarted = opts.requireStreamStarted !== false
  const filter = buildFilter(opts.filters || {})
  const clientTag = opts.tag || ''

  const startedAt = Date.now()
  const lastEvents = [] // ring buffer of last 10 envelopes for diagnostic dump
  const pushDiag = (e) => {
    lastEvents.push(compactEnvelope(e))
    if (lastEvents.length > 10) lastEvents.shift()
  }

  // Resolve event source (default: real gRPC).
  const eventSource = opts._eventSource
    ? opts._eventSource(filter, clientTag)
    : openGrpcStream(opts.endpoint, filter, clientTag)

  let streamStarted = null

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      try { eventSource.cancel() } catch (_) {}
      reject(buildTimeoutError({
        tag,
        elapsedMs: Date.now() - startedAt,
        maxWaitMs,
        filter: opts.filters || {},
        lastEvents,
        streamStarted
      }))
    }, maxWaitMs)

    const cleanup = () => {
      clearTimeout(timer)
      try { eventSource.cancel() } catch (_) {}
    }

    eventSource.on('data', (envRaw) => {
      const env = normalizeEnvelope(envRaw)
      pushDiag(env)
      if (env.kind === EventKind.STREAM_STARTED) {
        streamStarted = env.streamStarted
        // The first envelope on every Subscribe stream is StreamStarted. We expose it
        // to the caller via the returned event's `_streamStarted` field for callers
        // that need to bridge with REST snapshots. Don't run predicate against it
        // unless caller opts out via requireStreamStarted=false.
        if (!requireStreamStarted) {
          tryPredicate(env)
        }
        return
      }
      tryPredicate(env)
    })

    eventSource.on('error', (err) => {
      cleanup()
      reject(new Error(`awaitChainEvent[${tag}]: stream error after ${Date.now() - startedAt}ms: ${err.message || err}`))
    })

    eventSource.on('end', () => {
      cleanup()
      reject(new Error(`awaitChainEvent[${tag}]: stream ended without match after ${Date.now() - startedAt}ms; ${lastEvents.length} events seen`))
    })

    const tryPredicate = (env) => {
      let matched = false
      try {
        matched = !!opts.predicate(env)
      } catch (predicateErr) {
        cleanup()
        reject(new Error(`awaitChainEvent[${tag}]: predicate threw: ${predicateErr.message || predicateErr}`))
        return
      }
      if (matched) {
        cleanup()
        env._streamStarted = streamStarted
        env._elapsedMs = Date.now() - startedAt
        resolve(env)
      }
    }
  })
}

/**
 * Build a single EventFilter from the helper-friendly `filters` object. The proto schema
 * supports an ARRAY of filters (conjunctive) — for now we collapse the helper API down
 * to a single filter, which covers all current call sites. If we need disjunction later
 * we'll widen this.
 */
const buildFilter = (f) => {
  const kindToEnum = {
    STREAM_STARTED: 14,
    SNAPSHOT_FINALIZED: 1,
    BALANCE_CHANGE: 2,
    TOKEN_LOCK_STATE_CHANGE: 3,
    ALLOW_SPEND_STATE_CHANGE: 4,
    TRANSACTION_ACCEPTED: 5,
    METAGRAPH_SNAPSHOT_ACCEPTED: 6,
    METAGRAPH_BALANCE_CHANGE: 7
  }
  const filter = {
    kinds: (f.kinds || []).map((k) => kindToEnum[k] != null ? kindToEnum[k] : k),
    addresses: f.addresses || [],
    metagraphAddresses: f.metagraphAddresses || [],
    minOrdinal: f.minOrdinal != null ? f.minOrdinal : -1,
    maxOrdinal: f.maxOrdinal != null ? f.maxOrdinal : -1
  }
  return filter
}

/**
 * Open a real gRPC stream against the LocalEvents service.
 *
 * Default `kind` filter includes STREAM_STARTED so the first envelope arrives — the
 * helper depends on that lifecycle event for ordinal bridging.
 */
const openGrpcStream = (endpointOverride, filter, clientTag) => {
  const endpoint = resolveEndpoint(endpointOverride)
  const proto = loadProto()
  // proto package: nakamoto.local_events; service: LocalEvents
  const LocalEvents = proto.nakamoto.local_events.LocalEvents
  const client = new LocalEvents(endpoint, grpc.credentials.createInsecure())

  // Always include STREAM_STARTED so the lifecycle envelope reaches us (the server
  // sends it unconditionally; this is belt-and-braces in case the filter rejects it).
  const STREAM_STARTED_ENUM = 14
  const filterWithLifecycle = {
    ...filter,
    kinds: filter.kinds && filter.kinds.length > 0
      ? (filter.kinds.includes(STREAM_STARTED_ENUM) ? filter.kinds : [...filter.kinds, STREAM_STARTED_ENUM])
      : filter.kinds // empty → server returns ALL
  }

  const request = {
    filters: [filterWithLifecycle],
    clientTag: clientTag || ''
  }

  const call = client.Subscribe(request)
  // Attach client to call so closure on cancel doesn't leak the channel.
  call._client = client
  const origCancel = call.cancel.bind(call)
  call.cancel = () => {
    try { origCancel() } catch (_) {}
    try { client.close() } catch (_) {}
  }
  return call
}

/**
 * Structured timeout error.
 */
const buildTimeoutError = ({ tag, elapsedMs, maxWaitMs, filter, lastEvents, streamStarted }) => {
  const err = new Error(
    `awaitChainEvent[${tag}]: no matching event within ${Math.round(maxWaitMs / 1000)}s ` +
    `(elapsed=${Math.round(elapsedMs / 1000)}s, eventsSeen=${lastEvents.length}, ` +
    `streamStartedOrd=${streamStarted ? streamStarted.startOrdinal : 'n/a'}). ` +
    `Filter=${JSON.stringify(filter)}. Last events: ${JSON.stringify(lastEvents)}`
  )
  err.tag = tag
  err.elapsedMs = elapsedMs
  err.maxWaitMs = maxWaitMs
  err.filter = filter
  err.lastEvents = lastEvents
  err.streamStarted = streamStarted
  err.code = 'EVENT_WAIT_TIMEOUT'
  return err
}

// ─── Convenience wrappers ─────────────────────────────────────────────

/**
 * Wait for the token-lock at `address` matching `tokenLockHash` (optional) to transition
 * to EXPIRED. If `tokenLockHash` is omitted, the first EXPIRED transition for the
 * address matches.
 *
 * tokenLockHash may be a hex-string (e.g. the test's `hash` variable) — the proto carries
 * the lock-ref as bytes, so we compare via hex.
 */
const awaitTokenLockExpired = (address, opts = {}) => {
  const targetHash = opts.tokenLockHash ? String(opts.tokenLockHash).toLowerCase() : null
  return awaitChainEvent({
    ...opts,
    filters: { kinds: ['TOKEN_LOCK_STATE_CHANGE'], addresses: [address], ...(opts.filters || {}) },
    predicate: (e) => {
      if (e.kind !== EventKind.TOKEN_LOCK_STATE_CHANGE) return false
      if (e.tokenLockStateChange.transition !== 'EXPIRED') return false
      if (e.tokenLockStateChange.address !== address) return false
      if (targetHash) {
        const refHex = e.tokenLockStateChange.tokenLockRef
          ? Buffer.from(e.tokenLockStateChange.tokenLockRef).toString('hex')
          : ''
        if (refHex !== targetHash) return false
      }
      return true
    },
    tag: opts.tag || `tokenLockExpired:${address.slice(0, 12)}`
  })
}

/**
 * Wait until `address` has balance === `expectedBalance`. The predicate matches on the
 * `newBalance` field; matches as soon as a BalanceChange brings the address to that value.
 *
 * NOTE: this matches on the FIRST event whose newBalance equals expectedBalance — if your
 * scenario crosses the target value and continues changing (rare in tests but possible),
 * pin the call to a specific ordinal range via opts.filters.minOrdinal.
 */
const awaitBalanceMatches = (address, expectedBalance, opts = {}) => {
  const expected = String(expectedBalance) // proto returns uint64 as String via longs:String
  return awaitChainEvent({
    ...opts,
    filters: { kinds: ['BALANCE_CHANGE'], addresses: [address], ...(opts.filters || {}) },
    predicate: (e) =>
      e.kind === EventKind.BALANCE_CHANGE &&
      e.balanceChange.address === address &&
      String(e.balanceChange.newBalance) === expected,
    tag: opts.tag || `balanceMatches:${address.slice(0, 12)}=${expected}`
  })
}

/**
 * Wait for a transaction with `txHash` (hex) to be accepted.
 */
const awaitTransactionAccepted = (txHash, opts = {}) => {
  const targetHash = String(txHash).toLowerCase()
  return awaitChainEvent({
    ...opts,
    filters: { kinds: ['TRANSACTION_ACCEPTED'], ...(opts.filters || {}) },
    predicate: (e) => {
      if (e.kind !== EventKind.TRANSACTION_ACCEPTED) return false
      const refHex = e.transactionAccepted.txHash
        ? Buffer.from(e.transactionAccepted.txHash).toString('hex')
        : ''
      return refHex === targetHash
    },
    tag: opts.tag || `txAccepted:${targetHash.slice(0, 12)}`
  })
}

/**
 * Wait until a finalized snapshot at ordinal >= `targetOrd` is observed. Useful for the
 * "advance past epoch boundary" pattern that token-lock tests need: subscribe, watch
 * SnapshotFinalized events, succeed when `finalized_ordinal >= targetOrd`.
 */
const awaitSnapshotOrdinal = (targetOrd, opts = {}) => {
  const target = Number(targetOrd)
  return awaitChainEvent({
    ...opts,
    filters: { kinds: ['SNAPSHOT_FINALIZED'], ...(opts.filters || {}) },
    predicate: (e) =>
      e.kind === EventKind.SNAPSHOT_FINALIZED &&
      Number(e.snapshotFinalized.finalizedOrdinal) >= target,
    tag: opts.tag || `snapshotOrd>=${target}`
  })
}

module.exports = {
  awaitChainEvent,
  awaitTokenLockExpired,
  awaitBalanceMatches,
  awaitTransactionAccepted,
  awaitSnapshotOrdinal,
  EventKind,
  // Exported for tests + advanced callers
  parseMaxWait,
  resolveEndpoint,
  normalizeEnvelope,
  buildFilter,
  loadProto
}
