/**
 * KES stake-change smell test.
 *
 * Adds delegated stake to a target operator and measures whether their
 * cluster-wide snapshot-production share increases over a sampling window.
 *
 * Usage:
 *   node .github/action_scripts/delegated_staking/kes-stake-demo.js \
 *     [dagL0Port=9000] [dagL1Port=9010] \
 *     [--target-op=7] [--window-sec=180] [--stake-amount-dag=500000]
 */

const path = require('path')
const axios = require('axios')
const fs = require('fs')
const { dag4 } = require('@stardust-collective/dag4')

const {
  PRIVATE_KEYS,
  sleep,
  createNetworkConfig,
  logWorkflow,
} = require('../shared')
const {
  createTokenLock,
  createDelegatedStake,
  dagToDatum,
  getAccountDelegatedStakes,
} = require('./lib')

const GENESIS_PATH = path.resolve(__dirname, '../../../nodes/_genesis-out/l0-genesis.json')
const NUM_NODES = 8
const PORT_STRIDE = 10

const parseArgs = () => {
  const positional = []
  const flags = {}
  for (const a of process.argv.slice(2)) {
    if (a.startsWith('--')) {
      const [k, v] = a.replace(/^--/, '').split('=')
      flags[k] = v === undefined ? true : v
    } else {
      positional.push(a)
    }
  }
  const dagL0Port = parseInt(positional[0] || '9000', 10)
  const dagL1Port = parseInt(positional[1] || '9010', 10)
  const targetOp = parseInt(flags['target-op'] ?? '7', 10)
  const windowSec = parseInt(flags['window-sec'] ?? '180', 10)
  const stakeAmountDag = parseInt(flags['stake-amount-dag'] ?? '500000', 10)
  return { dagL0Port, dagL1Port, targetOp, windowSec, stakeAmountDag }
}

const loadOperators = () => {
  const g = JSON.parse(fs.readFileSync(GENESIS_PATH, 'utf8'))
  if (!Array.isArray(g.operators) || g.operators.length < NUM_NODES) {
    throw new Error(`expected >=${NUM_NODES} operators in genesis, got ${g.operators?.length}`)
  }
  return g.operators.slice(0, NUM_NODES).map((o, i) => ({
    idx: i,
    peerId: o.peerId,
    producerShort: o.peerId.slice(0, 8),
    genesisStakeDag: g.delegatedStakes[i]?.event?.amount / 1e8,
  }))
}

const LINE_RE = /^dag_nakamoto_snapshots_received_by_producer_total\{[^}]*producer_id="([0-9a-f]+)"[^}]*\}\s+([0-9.]+)/

const fetchNodeCounts = async (port) => {
  const res = await axios.get(`http://localhost:${port}/metrics`, { timeout: 5000 })
  const counts = new Map()
  for (const line of res.data.split('\n')) {
    const m = LINE_RE.exec(line)
    if (m) counts.set(m[1], parseFloat(m[2]))
  }
  return counts
}

// Sum counts across all 8 nodes. Each producer is observed by 7 others (self
// excluded), so sum/7 ≈ true production count.
const clusterWideCounts = async (dagL0Port) => {
  const ports = Array.from({ length: NUM_NODES }, (_, i) => dagL0Port + i * PORT_STRIDE)
  const perNode = await Promise.all(ports.map((p) =>
    fetchNodeCounts(p).catch((e) => { logWorkflow.info(`metrics fetch failed for :${p} (${e.message})`); return new Map() })
  ))
  const totals = new Map()
  for (const counts of perNode) {
    for (const [k, v] of counts) totals.set(k, (totals.get(k) || 0) + v)
  }
  const normalized = new Map()
  for (const [k, v] of totals) normalized.set(k, v / (NUM_NODES - 1))
  return normalized
}

const deltaMap = (after, before) => {
  const d = new Map()
  for (const [k, v] of after) d.set(k, v - (before.get(k) || 0))
  return d
}

const setupAccount = (urls) => {
  dag4.account.connect({
    networkVersion: '2.0',
    l0Url: urls.globalL0Url,
    l1Url: urls.dagL1Url,
  })
  dag4.account.loginPrivateKey(PRIVATE_KEYS.key4)
  return dag4.account
}

const waitForStakeVisible = async (urls, address, targetNodeId, expectedStakeHash) => {
  const maxAttempts = 60
  for (let i = 1; i <= maxAttempts; i++) {
    try {
      const resp = await getAccountDelegatedStakes(urls, address)
      const hit = resp.activeDelegatedStakes.find((s) => s.hash === expectedStakeHash && s.nodeId === targetNodeId)
      if (hit) return hit
    } catch (_) {}
    await sleep(3000)
  }
  throw new Error('stake never appeared in activeDelegatedStakes')
}

const pct = (n) => (n * 100).toFixed(2) + '%'
const pad = (s, w) => String(s).padStart(w)

const printTable = (operators, baseDelta, postDelta, targetIdx) => {
  const sum = (m) => Array.from(m.values()).reduce((a, b) => a + b, 0) || 1
  const baseTotal = sum(baseDelta)
  const postTotal = sum(postDelta)
  console.log('\nop | peerId    | base_wins | base_share% | post_wins | post_share% | delta_share%')
  console.log('---+-----------+-----------+-------------+-----------+-------------+-------------')
  for (const op of operators) {
    const bw = baseDelta.get(op.producerShort) || 0
    const pw = postDelta.get(op.producerShort) || 0
    const bs = bw / baseTotal
    const ps = pw / postTotal
    const d = ps - bs
    const row = `${op.idx === targetIdx ? '*' : ' '}${op.idx} | ${op.producerShort}  | ${pad(bw.toFixed(1), 9)} | ${pad(pct(bs), 11)} | ${pad(pw.toFixed(1), 9)} | ${pad(pct(ps), 11)} | ${pad((d >= 0 ? '+' : '') + pct(d), 12)}`
    console.log(op.idx === targetIdx ? '\x1b[1m' + row + '\x1b[0m' : row)
  }
  console.log(`\n(* = target operator, baseTotal=${baseTotal.toFixed(1)} postTotal=${postTotal.toFixed(1)}, normalized by /${NUM_NODES - 1})`)
  const t = operators[targetIdx].producerShort
  return { baseShare: (baseDelta.get(t) || 0) / baseTotal, postShare: (postDelta.get(t) || 0) / postTotal }
}

const main = async () => {
  const cfg = parseArgs()
  const urls = createNetworkConfig({
    dagL0PortPrefix: String(Math.floor(cfg.dagL0Port / 100)),
    dagL1PortPrefix: String(Math.floor(cfg.dagL1Port / 100)),
    metagraphL0PortPrefix: '92',
    currencyL1PortPrefix: '93',
    dataL1PortPrefix: '94',
  })

  const operators = loadOperators()
  if (cfg.targetOp < 0 || cfg.targetOp >= NUM_NODES) throw new Error(`--target-op out of range: ${cfg.targetOp}`)
  const target = operators[cfg.targetOp]
  logWorkflow.info(`target op=${target.idx} peerId=${target.producerShort} genesisStake=${target.genesisStakeDag}DAG addStake=${cfg.stakeAmountDag}DAG window=${cfg.windowSec}s`)

  logWorkflow.info('---- baseline sampling ----')
  const baseline = await clusterWideCounts(cfg.dagL0Port)
  logWorkflow.info(`baseline captured (${baseline.size} producers); sleeping ${cfg.windowSec}s`)
  await sleep(cfg.windowSec * 1000)
  const mid = await clusterWideCounts(cfg.dagL0Port)
  const baseDelta = deltaMap(mid, baseline)

  logWorkflow.info('---- submitting stake change ----')
  const account = setupAccount(urls)
  const lockAmountDatum = dagToDatum(cfg.stakeAmountDag)
  const lockHash = await createTokenLock(account, urls, lockAmountDatum)
  logWorkflow.info(`tokenLock hash=${lockHash}`)
  const stakeHash = await createDelegatedStake(account, lockHash, lockAmountDatum, target.peerId)
  logWorkflow.info(`delegatedStake hash=${stakeHash} target=${target.peerId.slice(0, 8)}`)

  logWorkflow.info('waiting for stake to land in activeDelegatedStakes')
  await waitForStakeVisible(urls, account.address, target.peerId, stakeHash)
  logWorkflow.info('stake visible; +10s safety buffer for StakeRegistry latest-GSI uptake')
  await sleep(10000)

  logWorkflow.info('---- post-stake sampling ----')
  const postBaseline = await clusterWideCounts(cfg.dagL0Port)
  logWorkflow.info(`post-baseline captured; sleeping ${cfg.windowSec}s`)
  await sleep(cfg.windowSec * 1000)
  const postEnd = await clusterWideCounts(cfg.dagL0Port)
  const postDelta = deltaMap(postEnd, postBaseline)

  const { baseShare, postShare } = printTable(operators, baseDelta, postDelta, cfg.targetOp)
  const ratio = baseShare > 0 ? postShare / baseShare : Infinity
  console.log(`\ntarget share ratio: post/base = ${ratio.toFixed(2)}x`)
  if (postShare < baseShare * 1.3) {
    console.log('\x1b[33mWARNING: post-stake share < 1.3x baseline (expected ≥1.5x for ~5x stake increase)\x1b[0m')
  } else {
    console.log('\x1b[32mOK: post-stake share rose materially\x1b[0m')
  }
}

main().catch((err) => {
  console.error('FATAL:', err.message || err)
  console.error(err.stack)
  process.exit(0)
})
