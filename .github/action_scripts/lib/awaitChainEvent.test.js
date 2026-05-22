/**
 * Unit tests for awaitChainEvent — uses a mock event source (no real gRPC).
 *
 * Run with: `node .github/action_scripts/lib/awaitChainEvent.test.js`
 *
 * The mock injection point is `opts._eventSource`, which mirrors the shape of
 * a grpc-js ClientReadableStream: {on(eventName, cb), cancel()}.
 *
 * No external test framework — a tiny in-file harness keeps the helper self-contained
 * and lets the test run inside the same minimal node toolchain the e2e scripts use.
 */

const {
  awaitChainEvent,
  awaitTokenLockExpired,
  awaitBalanceMatches,
  awaitTransactionAccepted,
  awaitSnapshotOrdinal,
  pollWithEventKick,
  parseMaxWait,
  resolveEndpoint,
  normalizeEnvelope,
  buildFilter,
  EventKind
} = require('./awaitChainEvent')

// ─── Mini test harness ──────────────────────────────────────────────

const results = { passed: 0, failed: 0, failures: [] }
const test = async (name, fn) => {
  try {
    await fn()
    results.passed++
    console.log(`  PASS  ${name}`)
  } catch (err) {
    results.failed++
    results.failures.push({ name, err })
    console.log(`  FAIL  ${name}\n        ${err.message || err}`)
  }
}

const assert = (cond, msg) => {
  if (!cond) throw new Error(`assertion failed: ${msg}`)
}
const assertEq = (a, b, msg) => {
  if (a !== b) throw new Error(`${msg || 'expected equal'}: got ${JSON.stringify(a)}, expected ${JSON.stringify(b)}`)
}

// ─── Mock event source ──────────────────────────────────────────────

/**
 * Build a mock event source that emits a scripted sequence of envelopes.
 *
 * `script` is an array of envelopes (raw shape, like proto-loader would produce
 * with `oneofs: true` — i.e. each envelope has `event: '<fieldName>'` plus the
 * payload at that field name).
 *
 * `delays` is an optional same-length array of millis to wait before emitting each.
 * Default 1ms staggers so events arrive in test-time real-order.
 */
const mockEventSource = (script, delays = null) => {
  const listeners = { data: [], error: [], end: [] }
  let cancelled = false
  const stream = {
    on(eventName, cb) {
      if (!listeners[eventName]) throw new Error(`unknown event ${eventName}`)
      listeners[eventName].push(cb)
      return stream
    },
    cancel() {
      cancelled = true
    },
    _listeners: listeners,
    _emitNext: null
  }
  // Drive the script via a chained setTimeout — runs on next tick so the caller
  // installs listeners before any data arrives.
  setImmediate(async () => {
    for (let i = 0; i < script.length; i++) {
      if (cancelled) return
      const ms = delays ? delays[i] : 1
      await new Promise(r => setTimeout(r, ms))
      if (cancelled) return
      for (const cb of listeners.data) cb(script[i])
    }
    if (!cancelled) {
      for (const cb of listeners.end) cb()
    }
  })
  return stream
}

const streamStarted = (startOrdinal = 42, epochProgress = 7, sessionId = '0') => ({
  event: 'streamStarted',
  seq: '0',
  publishedAtMillis: '0',
  ordinal: String(startOrdinal),
  streamStarted: { startOrdinal: String(startOrdinal), epochProgress: String(epochProgress), serverSessionId: sessionId }
})
const tokenLockExpired = (address, refHex, ordinal = 100) => ({
  event: 'tokenLockStateChange',
  seq: '1',
  publishedAtMillis: '0',
  ordinal: String(ordinal),
  tokenLockStateChange: {
    address,
    transition: 'EXPIRED',
    amount: '100',
    unlockEpoch: '50',
    tokenLockRef: Buffer.from(refHex, 'hex')
  }
})
const tokenLockCreated = (address, refHex, ordinal = 99) => ({
  event: 'tokenLockStateChange',
  seq: '1',
  publishedAtMillis: '0',
  ordinal: String(ordinal),
  tokenLockStateChange: {
    address,
    transition: 'CREATED',
    amount: '100',
    unlockEpoch: '50',
    tokenLockRef: Buffer.from(refHex, 'hex')
  }
})
const balanceChange = (address, newBal, ordinal = 110) => ({
  event: 'balanceChange',
  seq: '2',
  publishedAtMillis: '0',
  ordinal: String(ordinal),
  balanceChange: { address, oldBalance: '0', newBalance: String(newBal), cause: 'block' }
})
const transactionAccepted = (txHashHex, source, destination, ordinal = 120) => ({
  event: 'transactionAccepted',
  seq: '3',
  publishedAtMillis: '0',
  ordinal: String(ordinal),
  transactionAccepted: {
    txHash: Buffer.from(txHashHex, 'hex'),
    source,
    destination,
    amount: '1000',
    fee: '1'
  }
})
const snapshotFinalized = (finalizedOrdinal) => ({
  event: 'snapshotFinalized',
  seq: '4',
  publishedAtMillis: '0',
  ordinal: String(finalizedOrdinal),
  snapshotFinalized: { finalizedOrdinal: String(finalizedOrdinal), slot: '500', triggerMask: 1 }
})

// ─── Tests ──────────────────────────────────────────────────────────

const main = async () => {
  console.log('awaitChainEvent unit tests')
  console.log('==========================')

  // ── parseMaxWait ──
  await test('parseMaxWait: numbers pass through', () => assertEq(parseMaxWait(1234), 1234))
  await test('parseMaxWait: ms suffix', () => assertEq(parseMaxWait('500ms'), 500))
  await test('parseMaxWait: s suffix', () => assertEq(parseMaxWait('30s'), 30000))
  await test('parseMaxWait: min suffix', () => assertEq(parseMaxWait('30min'), 30 * 60 * 1000))
  await test('parseMaxWait: m suffix (short)', () => assertEq(parseMaxWait('5m'), 5 * 60 * 1000))
  await test('parseMaxWait: h suffix', () => assertEq(parseMaxWait('1h'), 60 * 60 * 1000))
  await test('parseMaxWait: bare integer string', () => assertEq(parseMaxWait('100'), 100))
  await test('parseMaxWait: bad string throws', () => {
    let threw = false
    try { parseMaxWait('foo') } catch (_) { threw = true }
    assert(threw, 'expected throw on bad duration')
  })

  // ── resolveEndpoint ──
  await test('resolveEndpoint: explicit override wins', () => {
    assertEq(resolveEndpoint('host:1234'), 'host:1234')
  })
  await test('resolveEndpoint: env LOCAL_EVENTS_ENDPOINT', () => {
    process.env.LOCAL_EVENTS_ENDPOINT = 'envhost:5678'
    assertEq(resolveEndpoint(), 'envhost:5678')
    delete process.env.LOCAL_EVENTS_ENDPOINT
  })
  await test('resolveEndpoint: strips scheme from TEST_HOST', () => {
    process.env.TEST_HOST = 'http://localhost'
    delete process.env.LOCAL_EVENTS_HOST
    delete process.env.LOCAL_EVENTS_PORT
    assertEq(resolveEndpoint(), 'localhost:50054')
    delete process.env.TEST_HOST
  })
  await test('resolveEndpoint: default localhost:50054', () => {
    delete process.env.LOCAL_EVENTS_ENDPOINT
    delete process.env.LOCAL_EVENTS_HOST
    delete process.env.LOCAL_EVENTS_PORT
    delete process.env.TEST_HOST
    assertEq(resolveEndpoint(), 'localhost:50054')
  })

  // ── normalizeEnvelope ──
  await test('normalizeEnvelope: token-lock variant', () => {
    const n = normalizeEnvelope(tokenLockExpired('DAG9abc', 'aabbccdd'))
    assertEq(n.kind, 'TOKEN_LOCK_STATE_CHANGE')
    assertEq(n.tokenLockStateChange.transition, 'EXPIRED')
    assertEq(n.tokenLockStateChange.address, 'DAG9abc')
  })
  await test('normalizeEnvelope: stream-started variant', () => {
    const n = normalizeEnvelope(streamStarted(50))
    assertEq(n.kind, 'STREAM_STARTED')
    assertEq(n.streamStarted.startOrdinal, '50')
  })

  // ── buildFilter ──
  await test('buildFilter: kind names → enum ints', () => {
    const f = buildFilter({ kinds: ['TOKEN_LOCK_STATE_CHANGE', 'BALANCE_CHANGE'] })
    assert(Array.isArray(f.kinds), 'kinds is array')
    assertEq(f.kinds[0], 3) // TOKEN_LOCK_STATE_CHANGE
    assertEq(f.kinds[1], 2) // BALANCE_CHANGE
  })
  await test('buildFilter: empty defaults', () => {
    const f = buildFilter({})
    assertEq(f.kinds.length, 0)
    assertEq(f.addresses.length, 0)
    assertEq(f.minOrdinal, -1)
  })

  // ── awaitChainEvent: happy path matches expired ──
  await test('awaitChainEvent: matches token-lock EXPIRED via predicate', async () => {
    const refHex = 'aabbccdd'
    const e = await awaitChainEvent({
      _eventSource: () => mockEventSource([
        streamStarted(10),
        tokenLockCreated('DAG87', refHex, 50),
        balanceChange('DAG87', 1000, 60),
        tokenLockExpired('DAG87', refHex, 100)
      ]),
      predicate: (env) =>
        env.kind === 'TOKEN_LOCK_STATE_CHANGE' && env.tokenLockStateChange.transition === 'EXPIRED',
      maxWait: '5s',
      tag: 'unit-test-happy-path'
    })
    assertEq(e.kind, 'TOKEN_LOCK_STATE_CHANGE')
    assertEq(e.tokenLockStateChange.transition, 'EXPIRED')
    assert(e._streamStarted, 'streamStarted captured')
    assertEq(e._streamStarted.startOrdinal, '10')
  })

  // ── awaitChainEvent: timeout produces structured error ──
  await test('awaitChainEvent: timeout produces structured diagnostic error', async () => {
    let err = null
    // Build a longer script with delays summing to more than maxWait so the timeout
    // fires before the stream ends. Each event spaced 100ms; 20 events = 2000ms > 200ms.
    const script = [streamStarted(10)]
    for (let i = 0; i < 20; i++) script.push(balanceChange('DAG99', i, 50 + i))
    const delays = script.map((_, i) => i === 0 ? 1 : 100)
    try {
      await awaitChainEvent({
        _eventSource: () => mockEventSource(script, delays),
        predicate: () => false, // never matches
        maxWait: '200ms',
        tag: 'unit-test-timeout'
      })
    } catch (e) {
      err = e
    }
    assert(err, 'expected throw')
    assertEq(err.code, 'EVENT_WAIT_TIMEOUT')
    assertEq(err.tag, 'unit-test-timeout')
    assert(Array.isArray(err.lastEvents), 'lastEvents is array')
    assert(err.lastEvents.length > 0, 'at least some events captured')
    assert(err.elapsedMs >= 100, 'elapsed time captured (>= half maxWait)')
    assert(err.message.includes('unit-test-timeout'), 'tag in message')
    assert(err.message.includes('Filter='), 'filter in message')
  })

  // ── awaitChainEvent: predicate exception propagates ──
  await test('awaitChainEvent: predicate throw rejects with diagnostic', async () => {
    let err = null
    try {
      await awaitChainEvent({
        _eventSource: () => mockEventSource([
          streamStarted(10),
          tokenLockCreated('DAG88', 'aabb', 50)
        ]),
        predicate: () => { throw new Error('boom') },
        maxWait: '1s'
      })
    } catch (e) { err = e }
    assert(err, 'predicate throw propagates')
    assert(err.message.includes('boom'), 'underlying error in message')
  })

  // ── awaitChainEvent: stream end before match ──
  await test('awaitChainEvent: stream end before match rejects', async () => {
    let err = null
    try {
      await awaitChainEvent({
        _eventSource: () => mockEventSource([streamStarted(1)]),
        predicate: () => false,
        maxWait: '5s'
      })
    } catch (e) { err = e }
    assert(err, 'stream end rejects')
    assert(err.message.includes('stream ended'), 'message identifies cause')
  })

  // ── awaitChainEvent: stream error rejects ──
  await test('awaitChainEvent: stream error rejects', async () => {
    let err = null
    const failingSource = () => {
      const stream = { on(name, cb) { stream._listeners[name] = cb; return stream }, _listeners: {}, cancel() {} }
      setImmediate(() => stream._listeners.error && stream._listeners.error(new Error('grpc connect failed')))
      return stream
    }
    try {
      await awaitChainEvent({
        _eventSource: failingSource,
        predicate: () => true,
        maxWait: '5s'
      })
    } catch (e) { err = e }
    assert(err, 'stream error rejects')
    assert(err.message.includes('grpc connect failed'), 'underlying error in message')
  })

  // ── awaitTokenLockExpired ──
  await test('awaitTokenLockExpired: filter + predicate compose correctly', async () => {
    const refHex = 'aabbcc'
    const e = await awaitTokenLockExpired('DAG-target', {
      tokenLockHash: refHex,
      maxWait: '5s',
      _eventSource: () => mockEventSource([
        streamStarted(5),
        tokenLockCreated('DAG-target', refHex, 20),
        tokenLockExpired('DAG-target', refHex, 80)
      ])
    })
    assertEq(e.tokenLockStateChange.transition, 'EXPIRED')
  })

  await test('awaitTokenLockExpired: wrong-hash EXPIRED does NOT match', async () => {
    let err = null
    // 30 events at 30ms = 900ms > 300ms maxWait; timeout fires first.
    const script = [streamStarted(5)]
    for (let i = 0; i < 30; i++) script.push(tokenLockExpired('DAG-target', 'ddeeff', 80 + i))
    const delays = script.map((_, i) => i === 0 ? 1 : 30)
    try {
      await awaitTokenLockExpired('DAG-target', {
        tokenLockHash: 'aabbcc',
        maxWait: '300ms',
        _eventSource: () => mockEventSource(script, delays)
      })
    } catch (e) { err = e }
    assert(err, 'mismatched hash should time out')
    assertEq(err.code, 'EVENT_WAIT_TIMEOUT')
  })

  // ── awaitBalanceMatches ──
  await test('awaitBalanceMatches: matches on exact balance', async () => {
    const e = await awaitBalanceMatches('DAG-bal', 12345, {
      maxWait: '5s',
      _eventSource: () => mockEventSource([
        streamStarted(1),
        balanceChange('DAG-bal', 100, 10),
        balanceChange('DAG-bal', 5000, 20),
        balanceChange('DAG-bal', 12345, 30)
      ])
    })
    assertEq(e.balanceChange.newBalance, '12345')
  })

  // ── awaitTransactionAccepted ──
  await test('awaitTransactionAccepted: matches on tx hash', async () => {
    const e = await awaitTransactionAccepted('deadbeef', {
      maxWait: '5s',
      _eventSource: () => mockEventSource([
        streamStarted(1),
        transactionAccepted('cafebabe', 'A', 'B', 10),
        transactionAccepted('deadbeef', 'A', 'C', 20)
      ])
    })
    assert(e.transactionAccepted.txHash, 'tx hash present')
  })

  // ── awaitSnapshotOrdinal ──
  await test('awaitSnapshotOrdinal: matches on >= target', async () => {
    const e = await awaitSnapshotOrdinal(100, {
      maxWait: '5s',
      _eventSource: () => mockEventSource([
        streamStarted(50),
        snapshotFinalized(60),
        snapshotFinalized(95),
        snapshotFinalized(110)
      ])
    })
    assertEq(e.snapshotFinalized.finalizedOrdinal, '110')
  })

  // ── awaitChainEvent: requireStreamStarted=false runs predicate against streamStarted ──
  await test('awaitChainEvent: requireStreamStarted=false runs predicate against lifecycle', async () => {
    const e = await awaitChainEvent({
      _eventSource: () => mockEventSource([streamStarted(99)]),
      requireStreamStarted: false,
      predicate: (env) => env.kind === 'STREAM_STARTED' && env.streamStarted.startOrdinal === '99',
      maxWait: '5s'
    })
    assertEq(e.kind, 'STREAM_STARTED')
  })

  // ── pollWithEventKick ──
  await test('pollWithEventKick: succeeds on first immediate check', async () => {
    let calls = 0
    const result = await pollWithEventKick({
      checkFn: async () => { calls++; return { ok: 'first try' } },
      maxWait: '5s',
      // Won't actually subscribe because checkImmediately=true and first call succeeds.
      _eventSource: () => mockEventSource([])
    })
    assertEq(result.ok, 'first try')
    assertEq(calls, 1)
  })

  await test('pollWithEventKick: re-checks on each kick event until success', async () => {
    let checkCount = 0
    const result = await pollWithEventKick({
      checkFn: async () => {
        checkCount++
        if (checkCount < 3) throw new Error(`not ready yet (call ${checkCount})`)
        return { ord: checkCount }
      },
      maxWait: '5s',
      _eventSource: () => mockEventSource([
        streamStarted(1),
        snapshotFinalized(10),
        snapshotFinalized(11),
        snapshotFinalized(12),
        snapshotFinalized(13)
      ])
    })
    assert(checkCount >= 3, `expected at least 3 checks, got ${checkCount}`)
    assertEq(result.ord, 3)
  })

  await test('pollWithEventKick: timeout produces structured diagnostic', async () => {
    let err = null
    const script = [streamStarted(1)]
    for (let i = 0; i < 10; i++) script.push(snapshotFinalized(20 + i))
    const delays = script.map((_, i) => i === 0 ? 1 : 60) // 10*60 = 600ms > 200ms maxWait
    try {
      await pollWithEventKick({
        checkFn: async () => { throw new Error('never ready') },
        maxWait: '200ms',
        tag: 'kick-timeout',
        _eventSource: () => mockEventSource(script, delays)
      })
    } catch (e) { err = e }
    assert(err, 'expected throw')
    assertEq(err.code, 'EVENT_WAIT_TIMEOUT')
    assertEq(err.tag, 'kick-timeout')
  })

  // ── Summary ──
  console.log('\n--------------------------')
  console.log(`Total: ${results.passed + results.failed}  passed: ${results.passed}  failed: ${results.failed}`)
  if (results.failed > 0) {
    console.log('Failures:')
    for (const f of results.failures) {
      console.log(`  ${f.name}`)
      console.log(`    ${f.err.stack || f.err.message || f.err}`)
    }
    process.exit(1)
  }
}

main().catch((err) => {
  console.error('Test harness crashed:', err)
  process.exit(2)
})
