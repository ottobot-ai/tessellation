const http = require('http')
const { dag4 } = require('@stardust-collective/dag4')
const { parseSharedArgs, logWorkflow, isStaleParentError } = require('../shared')
const { pollWithEventKick } = require('../lib/awaitChainEvent')

const createConfig = () => {
  const args = process.argv.slice(2)

  if (args.length < 5) {
    throw new Error(
      'Usage: node script.js <dagl0-port-prefix> <dagl1-port-prefix> <ml0-port-prefix> <cl1-port-prefix> <datal1-port-prefix>',
    )
  }

  const sharedArgs = parseSharedArgs(args.slice(0, 5))
  return { ...sharedArgs }
}

const SLEEP_TIME_UNTIL_QUERY = 180 * 1000

const FIRST_WALLET_SEED_PHRASE =
  'right off artist rare copy zebra shuffle excite evidence mercy isolate raise'
const SECOND_WALLET_SEED_PHRASE =
  'gauge shell cactus system resemble garlic pioneer theme doll grocery tiger spend'

const FIRST_WALLET_ADDRESS = 'DAG0d6yzQqBZTCnq7kB9hL8p4cCiFejfM5m6FBJB'
const SECOND_WALLET_ADDRESS = 'DAG87hragrbzrEQEz6VC5B7hvtm4wAemS7Zg8KFj'
const THIRD_WALLET_ADDRESS = 'DAG0DQPuvVThrHnz66S4V6cocrtpg59oesAWyRMb'

const logMessage = (message) => {
  logWorkflow.info(message)
}

const sleep = (ms) => {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

const fetchJson = (urlString) => {
  return new Promise((resolve, reject) => {
    const url = new URL(urlString)
    http.get(url, (res) => {
      let data = ''
      res.on('data', (chunk) => { data += chunk })
      res.on('end', () => {
        try {
          resolve(JSON.parse(data))
        } catch (e) {
          reject(new Error(`Failed to parse response from ${urlString}: ${data}`))
        }
      })
    }).on('error', reject)
  })
}

// After a metagraph transfer lands on CL0 (balance confirmed), CL1 may not have
// processed the snapshot yet. The sender's last-reference on CL1 still reflects
// pre-transfer state, so a subsequent transfer built from that ref will have a
// parent hash that disagrees with CL0's lastTxRefs → ParentHashNotEqLastTxHash.
//
// Poll CL1 until the sender's last-reference hash changes from its pre-transfer
// value, which means CL1 has processed the snapshot containing the transfer.
const waitForCL1Alignment = async (l1MetagraphUrl, address, beforeHash) => {
  const timeoutMs = SLEEP_TIME_UNTIL_QUERY
  logMessage(`Waiting for CL1 alignment (${address.slice(0, 12)}..., timeout ${timeoutMs / 1000}s)...`)
  const pollStart = Date.now()
  try {
    await pollWithEventKick({
      endpoint: process.env.LOCAL_EVENTS_ENDPOINT,
      maxWait: timeoutMs,
      tag: `cl1Alignment:${address.slice(0, 12)}`,
      kickFilter: { kinds: ['METAGRAPH_SNAPSHOT_ACCEPTED', 'SNAPSHOT_FINALIZED'] },
      checkFn: async () => {
        const ref = await fetchJson(`${l1MetagraphUrl}/transactions/last-reference/${address}`)
        if (ref.hash !== beforeHash) {
          return ref
        }
        throw new Error(`CL1 lastRef still ${beforeHash.slice(0, 12)}.. (not advanced)`)
      }
    })
    logMessage(`CL1 aligned after ${Math.round((Date.now() - pollStart) / 1000)}s`)
  } catch (_) {
    // Preserve legacy "proceed anyway" semantic — log the timeout and continue.
    logMessage(`CL1 alignment timeout — proceeding anyway`)
  }
}

// Poll GL0 for the latest CL0 snapshot ordinal it has committed for this metagraph.
// Returns -1 if the metagraph has no committed snapshot yet.
//
// gl0 serves lastCurrencySnapshots as an Either-encoded value:
//   {"<addr>": {"Right": [signedSnapshot, snapshotInfo]}}  (incremental)
//   {"<addr>": {"Left":  [hashedSnapshot, snapshotInfo]}}  (genesis/full)
// where the first element of either array carries the snapshot, with `.value.ordinal`
// for incremental or `.signed.value.ordinal` for full. Try both shapes; the prior
// implementation missed the Either wrapper entirely and reported -1 forever.
const getMetagraphOrdinalOnGL0 = async (gl0Url, metagraphAddress) => {
  const info = await fetchJson(`${gl0Url}/global-snapshots/latest/info`)
  const entry = info.lastCurrencySnapshots && info.lastCurrencySnapshots[metagraphAddress]
  if (!entry) return -1
  const inner = entry.Right ?? entry.Left ?? entry
  // inner may be an array [snapshot, info] or a bare snapshot object.
  const snap = Array.isArray(inner) ? inner[0] : inner
  if (!snap) return -1
  const ord = snap.value?.ordinal
            ?? snap.signed?.value?.ordinal
            ?? snap.ordinal
  if (typeof ord === 'number') return ord
  if (typeof ord === 'object' && typeof ord?.value === 'number') return ord.value
  return -1
}

// After a metagraph transfer, wait until GL0 has committed a strictly newer CL0
// snapshot than it had before. This confirms that GL0's view of the metagraph has
// absorbed at least one post-transfer CL0 snapshot — preventing the next transfer
// from racing ahead of CL0's globalSyncView catch-up.
const waitForGL0MetagraphAlignment = async (gl0Url, metagraphAddress, ordinalBefore) => {
  const timeoutMs = SLEEP_TIME_UNTIL_QUERY
  logMessage(`Waiting for GL0 to advance ${metagraphAddress.slice(0, 12)}... past ord ${ordinalBefore} (timeout ${timeoutMs / 1000}s)...`)
  const pollStart = Date.now()
  try {
    await pollWithEventKick({
      endpoint: process.env.LOCAL_EVENTS_ENDPOINT,
      maxWait: timeoutMs,
      tag: `gl0MgAlignment:${metagraphAddress.slice(0, 12)}`,
      // Wake on either a gl0 snapshot finalization OR a metagraph snapshot acceptance
      // — both are points at which GL0's view of this metagraph could have advanced.
      kickFilter: { kinds: ['METAGRAPH_SNAPSHOT_ACCEPTED', 'SNAPSHOT_FINALIZED'] },
      checkFn: async () => {
        const current = await getMetagraphOrdinalOnGL0(gl0Url, metagraphAddress)
        if (current > ordinalBefore) {
          return current
        }
        throw new Error(`GL0 metagraph ord ${current} <= ${ordinalBefore}`)
      }
    })
    logMessage(`GL0 advanced ${metagraphAddress.slice(0, 12)}... past ord ${ordinalBefore} after ${Math.round((Date.now() - pollStart) / 1000)}s`)
  } catch (_) {
    logMessage(`GL0 metagraph alignment timeout — proceeding anyway`)
  }
}

// Readiness gate for the FIRST L0-token (metagraph-currency) transfer.
//
// A metagraph's currency is not transactable until GL0 has committed its currency
// state into `lastCurrencySnapshots[mg]`: cl1 bootstraps its first currency snapshot
// FROM gl0's seeded view (CurrencySnapshotProcessor reads
// `globalState.lastCurrencySnapshots.get(identifier)`), and cl1's tx-validation has
// nothing to validate against until then (TransactionService waits for the first
// currency snapshot, else times out → the SDK send fails). Under sharding the seed
// travels mg → shard-committee ShardCheckpoint → gl0 verifyEmbedded-adopt, which at
// cold start takes minutes — longer than cl1's tx-validation timeout. So before the
// first metagraph transfer we poll the SAME field cl1 bootstraps from until it is
// populated (ordinal >= 0). This makes the test transact only once the currency is
// genuinely live on the global — mirroring production sequencing (a metagraph seeded
// onto a running global state isn't transactable the instant its genesis is sent).
//
// Fail LOUDLY on timeout (do NOT "proceed anyway"): a currency that never goes live is
// a real seeding regression and must surface as the cause, not be masked by the
// downstream send error.
const waitForMetagraphCurrencyLive = async (gl0Url, metagraphId) => {
  const timeoutMs = 8 * 60 * 1000
  const intervalMs = 5000
  const start = Date.now()
  logMessage(`Waiting for metagraph currency ${metagraphId.slice(0, 12)}... to go live on GL0 (lastCurrencySnapshots; timeout ${timeoutMs / 60000}m)...`)
  while (Date.now() - start < timeoutMs) {
    try {
      // Read lastCurrencySnapshots via /latest/combined (FinalizedSnapshotReader-backed,
      // ALWAYS available) rather than /latest/info, which 503s whenever gl0's head is
      // ahead of finalized — the common case under depth-k finality — and would stall
      // this poll. Combined is Either-encoded as [snapshot, info]; info is index 1.
      const data = await fetchJson(`${gl0Url}/global-snapshots/latest/combined`)
      const info = Array.isArray(data) && data.length >= 2 ? data[1] : (data && data.value && data.value.info) || {}
      const lcs = (info && info.lastCurrencySnapshots) || {}
      if (Object.prototype.hasOwnProperty.call(lcs, metagraphId)) {
        logMessage(`Metagraph currency live on GL0 (in lastCurrencySnapshots) after ${Math.round((Date.now() - start) / 1000)}s`)
        return
      }
    } catch (_) { /* gl0 transient — retry */ }
    await sleep(intervalMs)
  }
  throw new Error(
    `Metagraph currency ${metagraphId} never appeared in GL0 lastCurrencySnapshots within ${timeoutMs / 1000}s — ` +
    `currency seeding (mg → shard-checkpoint → gl0 verifyEmbedded-adopt) did not complete. ` +
    `Inspect gl0 ShardCheckpointGl0AcceptanceManager (verifyEmbedded accept/reject) + the per-shard binary buffer.`
  )
}

// Detect race-class errors that surface from the dag4 SDK or gl1 contextual
// validator. The SDK's `generateBatchTransactions` queries `lastReference`
// fresh — if gl1's view lags a prior scenario's accepted tx by a snapshot or
// two, the chain it builds has a stale parent. Three error shapes can come
// back from `sendBatchTransactions`: ContextualValidator 400 with one of
// `ParentOrdinalLowerThenLastProcessedTxOrdinal`, `HasNoMatchingParent`,
// `Conflict`, OR a chain-acceptance-time `ParentOrdinalBelowLastTxOrdinal`
// surfaced as a thrown SDK error string. All three are unambiguously
// stale-lastRef; rebuilding with a fresh query is safe.
const isSdkStaleParentError = (error) => {
  if (isStaleParentError(error)) return true
  const msg = (error && (error.message || String(error))) || ''
  return msg.includes('ParentOrdinalBelowLastTxOrdinal') ||
         msg.includes('ParentOrdinalLowerThenLastProcessedTxOrdinal') ||
         msg.includes('HasNoMatchingParent') ||
         msg.includes('Conflict')
}

// Post-#122 finality-gating: dl1's lastSnapshotStorage tracks finalized gl0
// only, so just-credited balances can appear as 0 to the mempool validator
// for a short window before finality catches up. Same root cause family as
// the TooFar fix (commit 91d43872). Retry on this transient state.
const isTransientInsufficientBalance = (error) => {
  const msg = (error && (error.message || String(error))) || ''
  return msg.includes('InsufficientBalance') && msg.includes('balance=0')
}

const batchTransaction = async (
  origin,
  destination,
  amount = 10,
  fee = 1,
  num = 100,
  lastRefUrl = null,
) => {
  const maxAttempts = 3
  const retryDelayMs = 1000
  let lastError = null
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const txnsData = []
      for (let idx = 0; idx < num; idx++) {
        const txnBody = {
          address: destination.address,
          amount,
          fee,
        }

        txnsData.push(txnBody)
      }

      // Diagnostic: capture origin's lastReference as seen by gl1 before submit.
      // Lets us correlate "what parent the SDK built on" against post-submit state
      // when a duplicate-tx race surfaces in balance assertions downstream.
      let lastRefBefore = null
      if (lastRefUrl) {
        try {
          lastRefBefore = await fetchJson(`${lastRefUrl}/transactions/last-reference/${origin.address}`)
        } catch (_) { /* best-effort */ }
      }

      const generatedTransactions = await origin.generateBatchTransactions(
        txnsData,
      )

      const hashes = await origin.sendBatchTransactions(generatedTransactions)

      const summary = Array.isArray(hashes)
        ? `${hashes.length} hashes [first=${hashes[0]?.slice(0, 12)}.. last=${hashes[hashes.length - 1]?.slice(0, 12)}..]`
        : `result=${JSON.stringify(hashes)?.slice(0, 200)}`
      logMessage(
        `DAG transaction from: ${origin.address} sent - batch of ${num}. attempt=${attempt} ` +
        `gl1LastRefBefore={ord=${lastRefBefore?.ordinal},hash=${lastRefBefore?.hash?.slice(0, 12)}..} returned=${summary}`,
      )

      return hashes
    } catch (e) {
      lastError = e
      if (isSdkStaleParentError(e) && attempt < maxAttempts) {
        logMessage(
          `batchTransaction: stale-parent race on attempt ${attempt}/${maxAttempts} ` +
          `(${e.message || e}). Retrying with fresh lastRef.`
        )
        await sleep(retryDelayMs)
        continue
      }
      if (isTransientInsufficientBalance(e) && attempt < maxAttempts) {
        logMessage(
          `batchTransaction: transient zero-balance (finality lag) on attempt ${attempt}/${maxAttempts} ` +
          `(${e.message || e}). Retrying after delay for finality to catch up.`
        )
        await sleep(5000)
        continue
      }
      throw Error(`Error when sending batch transaction: ${e}`)
    }
  }
  throw Error(`Error when sending batch transaction after ${maxAttempts} attempts: ${lastError}`)
}

const batchMetagraphTransaction = async (
  metagraphTokenClient,
  origin,
  destination,
  amount = 10,
  fee = 1,
  num = 100,
  cl1Url = null,
) => {
  const maxAttempts = 3
  const retryDelayMs = 1000
  let lastError = null
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const txnsData = []
      for (let idx = 0; idx < num; idx++) {
        const txnBody = {
          address: destination.address,
          amount,
          fee,
        }

        txnsData.push(txnBody)
      }

      let lastRefBefore = null
      if (cl1Url) {
        try {
          lastRefBefore = await fetchJson(`${cl1Url}/transactions/last-reference/${origin.address}`)
        } catch (_) { /* best-effort */ }
      }

      const generatedTransactions = await metagraphTokenClient.generateBatchTransactions(
        txnsData,
      )

      const hashes = await metagraphTokenClient.sendBatchTransactions(
        generatedTransactions,
      )

      const summary = Array.isArray(hashes)
        ? `${hashes.length} hashes [first=${hashes[0]?.slice(0, 12)}.. last=${hashes[hashes.length - 1]?.slice(0, 12)}..]`
        : `result=${JSON.stringify(hashes)?.slice(0, 200)}`
      logMessage(
        `L0 token transaction from: ${origin.address} sent - batch of ${num}. attempt=${attempt} ` +
        `cl1LastRefBefore={ord=${lastRefBefore?.ordinal},hash=${lastRefBefore?.hash?.slice(0, 12)}..} returned=${summary}`,
      )

      return hashes
    } catch (e) {
      lastError = e
      if (isSdkStaleParentError(e) && attempt < maxAttempts) {
        logMessage(
          `batchMetagraphTransaction: stale-parent race on attempt ${attempt}/${maxAttempts} ` +
          `(${e.message || e}). Retrying with fresh lastRef.`
        )
        await sleep(retryDelayMs)
        continue
      }
      if (isTransientInsufficientBalance(e) && attempt < maxAttempts) {
        logMessage(
          `batchMetagraphTransaction: transient zero-balance (finality lag) on attempt ${attempt}/${maxAttempts} ` +
          `(${e.message || e}). Retrying after delay for finality to catch up.`
        )
        await sleep(5000)
        continue
      }
      throw Error(`Error when sending batch transaction: ${e}`)
    }
  }
  throw Error(`Error when sending batch transaction after ${maxAttempts} attempts: ${lastError}`)
}

const handleBatchTransactions = async (
  networkOptions,
  origin,
  destination,
  amount,
  fee,
  txnCount,
) => {
  if (networkOptions) {
    await origin.connect({
      networkVersion: '2.0',
      l0Url: networkOptions.l0GlobalUrl,
      l1Url: networkOptions.dagL1UrlFirstNode,
      testnet: true,
    })
  }

  try {
    const dagL1Url = networkOptions?.dagL1UrlFirstNode || null
    await batchTransaction(origin, destination, amount, fee, txnCount, dagL1Url)

    // Poll for expected balances. In Nakamoto consensus, fork convergence
    // adds latency — the balance endpoint reads snapshotStorage.head which may lag.
    const expectedOriginDelta = -(amount + fee) * txnCount
    const expectedDestDelta = amount * txnCount
    const startOriginBalance = await origin.getBalance()
    const startDestBalance = await destination.getBalance()
    const expectedOriginBalance = startOriginBalance + expectedOriginDelta
    const expectedDestBalance = startDestBalance + expectedDestDelta

    // Reactive replacement for the prior 5s-interval wall-clock poll. Each gl0
    // SNAPSHOT_FINALIZED event (and METAGRAPH_SNAPSHOT_ACCEPTED for completeness)
    // kicks a fresh balance check — same correctness as the legacy polling but
    // wakes only on actual cluster progress. The full 100-tx batch is required
    // to land (this was the original semantic — break-on-first-change was the bug
    // the polling block fixed). On 3min timeout we fall through into the diagnostic
    // block below.
    let originBalance = startOriginBalance
    let destinationBalance = startDestBalance
    const startTime = Date.now()
    logMessage(`Polling for balance settlement via event stream (timeout ${SLEEP_TIME_UNTIL_QUERY}ms, expect ${expectedOriginDelta}/${expectedDestDelta} delta)...`)
    try {
      await pollWithEventKick({
        endpoint: process.env.LOCAL_EVENTS_ENDPOINT,
        maxWait: SLEEP_TIME_UNTIL_QUERY,
        tag: `dagBatchSettle:${origin.address.slice(0, 12)}->${destination.address.slice(0, 12)}`,
        checkFn: async () => {
          originBalance = await origin.getBalance()
          destinationBalance = await destination.getBalance()
          if (originBalance === expectedOriginBalance && destinationBalance === expectedDestBalance) {
            return { originBalance, destinationBalance }
          }
          throw new Error(`not settled yet: origin=${originBalance}/${expectedOriginBalance} dest=${destinationBalance}/${expectedDestBalance}`)
        }
      })
      logMessage(`Balance settled after ${Math.round((Date.now() - startTime) / 1000)}s`)
    } catch (_) {
      // Timeout — fall through into the diagnostic dump below (preserves prior behavior:
      // we do NOT throw here, we continue and let the caller's final assertion surface
      // the mismatch with full context). The structured event-timeout error message is
      // logged so post-mortem analysis sees the last 10 cluster events.
      logMessage(`pollWithEventKick timed out; falling through to legacy diagnostic block.`)
    }
    if (originBalance !== expectedOriginBalance || destinationBalance !== expectedDestBalance) {
      logMessage(`Balance did not fully settle after ${SLEEP_TIME_UNTIL_QUERY / 1000}s — origin=${originBalance}/${expectedOriginBalance} dest=${destinationBalance}/${expectedDestBalance} (falling through to final check)`)
      // Diagnostic dump: post-failure lastReference for both wallets on gl1 lets
      // us tell whether more txs landed than we sent (chain ordinal advanced
      // beyond expected) vs balance-endpoint staleness (ordinal as expected,
      // balance just hasn't propagated yet).
      if (dagL1Url) {
        try {
          const [originRefAfter, destRefAfter] = await Promise.all([
            fetchJson(`${dagL1Url}/transactions/last-reference/${origin.address}`).catch(() => null),
            fetchJson(`${dagL1Url}/transactions/last-reference/${destination.address}`).catch(() => null),
          ])
          logMessage(
            `[DIAG] post-failure DAG state: origin(${origin.address.slice(0, 12)}..) ` +
            `lastRef={ord=${originRefAfter?.ordinal},hash=${originRefAfter?.hash?.slice(0, 12)}..} ` +
            `dest(${destination.address.slice(0, 12)}..) ` +
            `lastRef={ord=${destRefAfter?.ordinal},hash=${destRefAfter?.hash?.slice(0, 12)}..} ` +
            `expectedSentCount=${txnCount}`
          )
        } catch (_) { /* best-effort */ }
      }
    }

    return { originBalance, destinationBalance }
  } catch (error) {
    const errorMessage = `Error when sending transactions between wallets, message: ${error}`
    logMessage(errorMessage)
    throw error
  }
}

const handleMetagraphBatchTransactions = async (
  networkOptions,
  origin,
  destination,
  amount,
  fee,
  txnCount,
) => {
  try {
    await origin.connect({
      networkVersion: '2.0',
      l0Url: networkOptions.l0GlobalUrl,
      l1Url: networkOptions.dagL1UrlFirstNode,
      testnet: true,
    })

    const metagraphTokenClient = origin.createMetagraphTokenClient({
      id: networkOptions.metagraphId,
      l0Url: networkOptions.l0MetagraphUrl,
      l1Url: networkOptions.l1MetagraphUrl,
      testnet: true,
    })

    // Capture the SENDER's CL1 last-reference before sending. cl1's
    // /transactions/last-reference is keyed by SOURCE address (TransactionStorage
    // `getLastProcessedTransaction(source)` — the outgoing tx chain), so only the
    // SENDER's ref advances for this transfer; the destination merely receives and
    // its source-chain ref never moves. Waiting on the destination (the prior bug)
    // therefore timed out the full 180s on EVERY transfer. Wait on the origin
    // instead — matching this helper's own doc ("the sender's last-reference ...
    // reflects pre-transfer state") — which advances once cl1 processes the tx
    // (already true by the time the balance settle below returns), so it resolves
    // immediately instead of stalling.
    const originRefBefore = await fetchJson(
      `${networkOptions.l1MetagraphUrl}/transactions/last-reference/${origin.address}`
    )

    // Capture GL0's current CL0 ordinal for this metagraph before sending.
    // We'll wait for this to advance after the transfer completes — that signals
    // GL0 has absorbed a post-transfer CL0 snapshot, so the next transfer won't
    // race ahead of GL0's metagraph view and trigger a globalSyncView mismatch.
    const gl0OrdBefore = await getMetagraphOrdinalOnGL0(
      networkOptions.l0GlobalUrl,
      networkOptions.metagraphId
    )

    await batchMetagraphTransaction(
      metagraphTokenClient,
      origin,
      destination,
      amount,
      fee,
      txnCount,
      networkOptions.l1MetagraphUrl,
    )

    const startOriginBalance = await metagraphTokenClient.getBalance()
    const startDestBalance = await metagraphTokenClient.getBalanceFor(destination.address)
    const expectedOriginDelta = -(amount + fee) * txnCount
    const expectedDestDelta = amount * txnCount
    const expectedOriginBalance = startOriginBalance + expectedOriginDelta
    const expectedDestBalance = startDestBalance + expectedDestDelta

    // Reactive replacement for the prior 5s-interval wall-clock poll. Driven off
    // METAGRAPH_SNAPSHOT_ACCEPTED + SNAPSHOT_FINALIZED kicks; the L0 token batch
    // (count=100) e2e test was the one that flaked in retry #2 — moving to event-kicked
    // settles lets the test wake immediately when the cluster makes progress, instead
    // of sleeping a fixed 5s past each tick.
    let originBalance = startOriginBalance, destinationBalance = startDestBalance
    const startTime = Date.now()
    logMessage(`Polling for L0 token balance settlement via event stream (timeout ${SLEEP_TIME_UNTIL_QUERY}ms, expect ${expectedOriginDelta}/${expectedDestDelta} delta)...`)
    try {
      await pollWithEventKick({
        endpoint: process.env.LOCAL_EVENTS_ENDPOINT,
        maxWait: SLEEP_TIME_UNTIL_QUERY,
        tag: `mgBatchSettle:${origin.address.slice(0, 12)}->${destination.address.slice(0, 12)}`,
        kickFilter: { kinds: ['METAGRAPH_SNAPSHOT_ACCEPTED', 'SNAPSHOT_FINALIZED'] },
        checkFn: async () => {
          originBalance = await metagraphTokenClient.getBalance()
          destinationBalance = await metagraphTokenClient.getBalanceFor(destination.address)
          if (originBalance === expectedOriginBalance && destinationBalance === expectedDestBalance) {
            return { originBalance, destinationBalance }
          }
          throw new Error(`not settled yet: origin=${originBalance}/${expectedOriginBalance} dest=${destinationBalance}/${expectedDestBalance}`)
        }
      })
      logMessage(`L0 token balance settled after ${Math.round((Date.now() - startTime) / 1000)}s`)
    } catch (_) {
      // Fall through; the caller's final assertion will report the mismatch.
      logMessage(`pollWithEventKick timed out; falling through to legacy diagnostic block.`)
    }
    if (originBalance !== expectedOriginBalance || destinationBalance !== expectedDestBalance) {
      logMessage(`L0 token balance did not fully settle after ${SLEEP_TIME_UNTIL_QUERY / 1000}s — origin=${originBalance}/${expectedOriginBalance} dest=${destinationBalance}/${expectedDestBalance} (falling through to final check)`)
      // Diagnostic dump: post-failure cl1 lastReference for both wallets +
      // gl0 metagraph ord. If origin's cl1 ord advanced > expected, more txs
      // landed than we sent (dag4-SDK / cl1 race). If ord matches expected,
      // the balance endpoint is just stale (snapshot lag).
      try {
        const [originRefAfter, destRefAfter, gl0OrdAfter] = await Promise.all([
          fetchJson(`${networkOptions.l1MetagraphUrl}/transactions/last-reference/${origin.address}`).catch(() => null),
          fetchJson(`${networkOptions.l1MetagraphUrl}/transactions/last-reference/${destination.address}`).catch(() => null),
          getMetagraphOrdinalOnGL0(networkOptions.l0GlobalUrl, networkOptions.metagraphId).catch(() => -1),
        ])
        logMessage(
          `[DIAG] post-failure L0-token state: origin(${origin.address.slice(0, 12)}..) ` +
          `cl1LastRef={ord=${originRefAfter?.ordinal},hash=${originRefAfter?.hash?.slice(0, 12)}..} ` +
          `dest(${destination.address.slice(0, 12)}..) ` +
          `cl1LastRef={ord=${destRefAfter?.ordinal},hash=${destRefAfter?.hash?.slice(0, 12)}..} ` +
          `gl0MetagraphOrd=${gl0OrdAfter} (was ${gl0OrdBefore} before submit) ` +
          `expectedSentCount=${txnCount}`
        )
      } catch (_) { /* best-effort */ }
    }

    // Wait for CL1 to process the snapshot containing this transfer before
    // returning. Without this, a subsequent reverse transfer may read stale
    // last-reference state from CL1 and build a transaction whose parent hash
    // disagrees with CL0's lastTxRefs.
    await waitForCL1Alignment(
      networkOptions.l1MetagraphUrl,
      origin.address,
      originRefBefore.hash
    )

    // Wait for GL0 to advance its view of this metagraph past its pre-transfer
    // CL0 ordinal. This prevents the next transfer from landing in a CL0 round
    // whose globalSyncView is too stale relative to GL0's current head, which
    // causes SnapshotDifferentThanExpected rejection at the validator.
    await waitForGL0MetagraphAlignment(
      networkOptions.l0GlobalUrl,
      networkOptions.metagraphId,
      gl0OrdBefore
    )

    return { originBalance, destinationBalance }
  } catch (error) {
    const errorMessage = `Error when sending transactions between wallets, message: ${error}`
    logMessage(errorMessage)
    throw error
  }
}

const doubleSpendTest = async (networkOptions, isMetagraph) => {
  logMessage(
    `========= Starting double spend transaction test (${
      isMetagraph ? 'L0 token' : 'DAG'
    }) =========`,
  )

  const sendAmount = 1000
  const sendFee = 0

  const connectConfig = {
    networkVersion: '2.0',
    l0Url: networkOptions.l0GlobalUrl,
    l1Url: networkOptions.dagL1UrlFirstNode,
    testnet: true,
  }

  const accountFirstNode = dag4.createAccount()
  accountFirstNode.loginSeedPhrase(FIRST_WALLET_SEED_PHRASE)
  accountFirstNode.connect(connectConfig)

  let sendingClient
  if (isMetagraph) {
    sendingClient = accountFirstNode.createMetagraphTokenClient({
      id: networkOptions.metagraphId,
      l0Url: networkOptions.l0MetagraphUrl,
      l1Url: networkOptions.l1MetagraphUrl,
    })
  } else {
    sendingClient = accountFirstNode
  }

  const lastRef = await sendingClient.network.getAddressLastAcceptedTransactionRef(
    FIRST_WALLET_ADDRESS,
  )

  const firstToSecondTx = await accountFirstNode.generateSignedTransaction(
    SECOND_WALLET_ADDRESS,
    sendAmount,
    sendFee,
    lastRef,
  )

  const firstToThirdTx = await accountFirstNode.generateSignedTransaction(
    THIRD_WALLET_ADDRESS,
    sendAmount,
    sendFee,
    lastRef,
  )

  try {
    const startBalance1 = await sendingClient.getBalanceFor(
      FIRST_WALLET_ADDRESS,
    )
    const startBalance2 = await sendingClient.getBalanceFor(
      SECOND_WALLET_ADDRESS,
    )
    const startBalance3 = await sendingClient.getBalanceFor(
      THIRD_WALLET_ADDRESS,
    )

    logMessage('Sending txns w/same lastRef')
    const [firstToSecondSucceeded, firstToThirdSucceeded] = await Promise.all([
      sendingClient.network
        .postTransaction(firstToSecondTx)
        .then((v) => true)
        .catch((e) => false),
      sendingClient.network
        .postTransaction(firstToThirdTx)
        .then((v) => true)
        .catch((e) => false),
    ])

    logMessage(
      `Waiting ${SLEEP_TIME_UNTIL_QUERY}ms until fetch wallet balances`,
    )
    await sleep(SLEEP_TIME_UNTIL_QUERY)

    const balance1 = await sendingClient.getBalanceFor(FIRST_WALLET_ADDRESS)
    const balance2 = await sendingClient.getBalanceFor(SECOND_WALLET_ADDRESS)
    const balance3 = await sendingClient.getBalanceFor(THIRD_WALLET_ADDRESS)

    logMessage(`FirstWalletBalance: ${balance1}`)
    logMessage(`SecondWalletBalance: ${balance2}`)
    logMessage(`ThirdWalletBalance: ${balance3}`)
    logMessage(`firstToSecondSucceeded: ${firstToSecondSucceeded}`)
    logMessage(`firstToThirdSucceeded: ${firstToThirdSucceeded}`)

    if (
      firstToSecondSucceeded &&
      balance1 === startBalance1 - sendAmount - sendFee &&
      balance2 === startBalance2 + sendAmount &&
      balance3 === startBalance3
    ) {
      logMessage(`No double spend: Amount sent to second wallet`)
      return
    }

    if (
      firstToThirdSucceeded &&
      balance1 === startBalance1 - sendAmount - sendFee &&
      balance2 === startBalance2 &&
      balance3 === startBalance3 + sendAmount
    ) {
      logMessage(`No double spend: Amount sent to third wallet`)
      return
    }

    throw Error(`Double spend occurred`)
  } catch (error) {
    const errorMessage = `Error when sending double spend transaction between wallets, message: ${error}`
    logMessage(errorMessage)
    throw error
  }
}

const assertBalances = async (
  account1Balance,
  account2Balance,
  expectedAccount1Balance,
  expectedAccount2Balance,
) => {
  if (
    Number(account1Balance) !== Number(expectedAccount1Balance) ||
    Number(account2Balance) !== Number(expectedAccount2Balance)
  ) {
    throw Error(`
        Error sending transactions. Wallet balances are different than expected:
        expectedAccount1Balance: ${expectedAccount1Balance} ---- actual: ${account1Balance}
        expectedAccount2Balance: ${expectedAccount2Balance} ---- actual: ${account2Balance}
        `)
  }

  logMessage(`Correct Account 1 Balance: ${expectedAccount1Balance}`)
  logMessage(`Correct Account 2 Balance: ${expectedAccount2Balance}`)
}

// After a transfer settles, the balance-READ source (dag4 getBalance, which reads gl0's finalized
// GlobalSnapshotInfo) can lag the event-stream settlement that the batch helpers poll on — the settle
// fires as soon as finality observes the delta, but the finalized read the NEXT transfer uses for its
// start balance trails by a few finalization cycles. The next transfer then reads a STALE start and
// computes a wrong expected delta (observed as "Wallet balances are different than expected" on the
// 2nd DAG transfer). This is pure read-timing fragility, not a ledger bug, and it surfaces whenever gl0
// finalization is slower (e.g. under the #259 metagraph-currency adopt work). Poll the SAME read source
// transferTest uses for its start balance until it reflects this transfer, bounded; proceed on timeout.
const waitForReadSourceCatchUp = async (getFromBal, getToBal, expectedFrom, expectedTo) => {
  const timeoutMs = 60000
  const intervalMs = 2000
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    try {
      const [f, t] = await Promise.all([getFromBal(), getToBal()])
      if (Number(f) === Number(expectedFrom) && Number(t) === Number(expectedTo)) return
    } catch (_) { /* transient read error — retry */ }
    await sleep(intervalMs)
  }
  logMessage(`read-source catch-up timeout — proceeding (getBalance still lags the settled balances)`)
}

const transferTest = async (
  fromAccount,
  toAccount,
  amount,
  fee,
  txnCount,
  metagraphOpts,
) => {
  const isMetagraph = !!metagraphOpts
  let getFromBal, getToBal
  if (metagraphOpts) {
    const metagraphTokenClient = fromAccount.createMetagraphTokenClient({
      id: metagraphOpts.metagraphId,
      l0Url: metagraphOpts.l0MetagraphUrl,
      l1Url: metagraphOpts.l1MetagraphUrl,
    })
    getFromBal = () => metagraphTokenClient.getBalance()
    getToBal = () => metagraphTokenClient.getBalanceFor(toAccount.address)
  } else {
    getFromBal = () => fromAccount.getBalance()
    getToBal = () => toAccount.getBalance()
  }
  const fromAccountStart = await getFromBal()
  const toAccountStart = await getToBal()

  logMessage(
    `========= Transfer test (${isMetagraph ? 'L0 token' : 'DAG'}): ${
      fromAccount.address
    } to ${toAccount.address} w/fee (${fee}) and count (${txnCount}) =========`,
  )

  const batchFunc = metagraphOpts
    ? handleMetagraphBatchTransactions
    : handleBatchTransactions

  const { originBalance, destinationBalance } = await batchFunc(
    metagraphOpts,
    fromAccount,
    toAccount,
    amount,
    fee,
    txnCount,
  )

  const totalAmount = txnCount * amount
  const totalFee = txnCount * fee

  const expectedFromBalance = fromAccountStart - totalAmount - totalFee
  const expectedToBalance = toAccountStart + totalAmount

  await assertBalances(
    originBalance,
    destinationBalance,
    expectedFromBalance,
    expectedToBalance,
  )

  // Don't return until the read source reflects this transfer, so the next transferTest's start read is current.
  await waitForReadSourceCatchUp(getFromBal, getToBal, expectedFromBalance, expectedToBalance)
}

const sendTransactionsUsingUrls = async (networkOptions) => {
  const dagConfig = {
    networkVersion: '2.0',
    l0Url: networkOptions.l0GlobalUrl,
    l1Url: networkOptions.dagL1UrlFirstNode,
    testnet: true,
  }

  const account1 = dag4.createAccount()
  account1.loginSeedPhrase(FIRST_WALLET_SEED_PHRASE)
  account1.connect(dagConfig)

  const account2 = dag4.createAccount()
  account2.loginSeedPhrase(SECOND_WALLET_SEED_PHRASE)
  account2.connect(dagConfig)

  // DAG
  await transferTest(account1, account2, 10, 0, 1)
  await transferTest(account2, account1, 10, 0, 1)

  await transferTest(account1, account2, 10, 0.02, 100)
  await transferTest(account2, account1, 10, 0.02, 100)

  // Metagraph — gate on the currency being LIVE on gl0 first (seeding-race fix):
  // under sharding the genesis currency takes minutes to travel mg → shard-checkpoint
  // → gl0 adopt, and cl1 can't validate currency txs until gl0 has seeded it.
  await waitForMetagraphCurrencyLive(networkOptions.l0GlobalUrl, networkOptions.metagraphId)
  await transferTest(account1, account2, 10, 0, 1, networkOptions)
  await transferTest(account2, account1, 10, 0, 1, networkOptions)

  await transferTest(account1, account2, 10, 0.02, 100, networkOptions)
  await transferTest(account2, account1, 10, 0.02, 100, networkOptions)

  // Double spends
  await doubleSpendTest(networkOptions, false)
  await doubleSpendTest(networkOptions, true)

  logMessage('Script finished')
  return
}

const sendTransactions = async () => {
  const {
    dagL0PortPrefix,
    dagL1PortPrefix,
    metagraphL0PortPrefix,
    currencyL1PortPrefix,
  } = createConfig()

  // gl1 EXTERNAL host port for node 0 = GL1_EXT_PORT_BASE (9600 band), NOT the
  // dagL1PortPrefix (9100) container-internal band. GL1_URL (set by set-env.sh)
  // takes priority; the fallback mirrors the external base.
  const gl1ExtPortBase = parseInt(process.env.GL1_EXT_PORT_BASE || `${parseInt(dagL1PortPrefix, 10) * 100}`, 10)
  const networkOptions = {
    // Fallback "custom_id" is the dag4.js SDK placeholder for "look up the
    // metagraph from the network config"; it's NOT a real DAG address. Real
    // GL0 lookups (e.g. waitForGL0MetagraphAlignment) need the actual DAG
    // address, which compose-runner exports as METAGRAPH_ID.
    metagraphId: process.env.METAGRAPH_ID || 'custom_id',
    l0GlobalUrl: process.env.GL0_URL || `${process.env.TEST_HOST || 'http://localhost'}:${dagL0PortPrefix}00`,
    dagL1UrlFirstNode: process.env.GL1_URL || `${process.env.TEST_HOST || 'http://localhost'}:${gl1ExtPortBase}`,
    l0MetagraphUrl: process.env.ML0_URL || `${process.env.TEST_HOST || 'http://localhost'}:${metagraphL0PortPrefix}00`,
    l1MetagraphUrl: process.env.CL1_URL || `${process.env.TEST_HOST || 'http://localhost'}:${currencyL1PortPrefix}00`,
  }

  await sendTransactionsUsingUrls(networkOptions)
}

sendTransactions().catch((err) => {
  if (process.env.RUN_ENV === 'local') {
    console.log('Failed: ')
    console.log(err)
    return
  }

  throw err
})
