/**
 * MPT Rollback E2E Tests
 * 
 * Tests that StateProof remains valid after rollback scenarios with delegated stakes.
 * 
 * Local run instructions:
 * - Start Euclid from genesis (`hydra start-genesis`)
 * - RUN_ENV=local node .github/action_scripts/mpt_rollback/mpt-rollback-test.js 90 91 testBasicRollback
 * - Reset Euclid to run again (`hydra stop && hydra start-genesis`)
 */

const path = require('path')
const fs = require('fs')
const { execSync, spawn } = require('child_process')
const axios = require('axios')
const { dag4 } = require('@stardust-collective/dag4')

const RUN_ENV = process.env.RUN_ENV || 'ci'

const {
  parseSharedArgs,
  PRIVATE_KEYS,
  sleep,
  withRetry,
  createNetworkConfig,
  logWorkflow,
} = require('../shared')

const {
  dagToDatum,
  getPrivateKeyAndNodeIdFromFile,
  postNodeParamsNodeId,
  createDelegatedStake,
  createTokenLock,
  getAccountDelegatedStakes,
} = require('../delegated_staking/lib')

// Parse arguments
const createConfig = () => {
  const args = process.argv.slice(2)
  if (args.length < 3) {
    throw new Error('Usage: node mpt-rollback-test.js <dagl0-port-prefix> <dagl1-port-prefix> <workflow-name>')
  }
  return parseSharedArgs(args.slice(0, 3), false)
}

const setupDag4Account = (urls) => {
  dag4.account.connect({
    networkVersion: '2.0',
    l0Url: urls.globalL0Url,
    l1Url: urls.dagL1Url,
  })
  return dag4.account
}

const extractKeysAndAccount = (filePath) => {
  const { privateKeyString, nodeId } = getPrivateKeyAndNodeIdFromFile(filePath)
  const account = dag4.createAccount(privateKeyString)
  return { privateKeyString, nodeId, account }
}

// Get current ordinal
const getCurrentOrdinal = async (urls) => {
  const response = await axios.get(`${urls.globalL0Url}/global-snapshots/latest`)
  return response.data.value.ordinal
}

// Get latest snapshot hash
const getLatestHash = async (urls) => {
  const response = await axios.get(`${urls.globalL0Url}/global-snapshots/latest`)
  return response.data.value.lastSnapshotHash
}

// Get delegated stakes for address
const getDelegatedStakes = async (urls, address) => {
  try {
    const response = await axios.get(`${urls.globalL0Url}/global-snapshots/latest/combined`)
    const stakes = response.data[1]?.activeDelegatedStakes?.[address] || []
    return stakes.map(s => ({ createdAt: s.createdAt, nodeId: s.event?.value?.nodeId?.slice(0, 8) }))
  } catch (e) {
    return []
  }
}

// Find and kill genesis node process
const killGenesisNode = () => {
  logWorkflow.info('Killing genesis node process...')
  try {
    const result = execSync('pgrep -f "dag-l0.jar.*run-genesis"').toString().trim()
    if (result) {
      const pids = result.split('\n')
      pids.forEach(pid => {
        try {
          execSync(`kill -9 ${pid}`)
          logWorkflow.info(`Killed process ${pid}`)
        } catch (e) {
          // Process may have already exited
        }
      })
    }
  } catch (e) {
    logWorkflow.warn('No genesis process found to kill')
  }
  return sleep(2000)
}

// Delete snapshot files for specific ordinals
const deleteOrdinals = (startOrdinal, endOrdinal) => {
  const genesisDir = path.join(process.cwd(), '.github/code/hypergraph/dag-l0/genesis-node')
  const dataDir = path.join(genesisDir, 'data')
  
  logWorkflow.info(`Deleting ordinals ${startOrdinal} to ${endOrdinal}...`)
  
  const dirsToClean = ['snapshot_info', 'mpt_snapshot_info', 'incremental_snapshot/ordinal/0']
  
  for (let ordinal = startOrdinal; ordinal <= endOrdinal; ordinal++) {
    dirsToClean.forEach(dir => {
      const filePath = path.join(dataDir, dir, String(ordinal))
      try {
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath)
        }
      } catch (e) {
        // File may not exist
      }
    })
  }
}

// Start genesis node with rollback mode
const startGenesisWithRollback = async (portPrefix, rollbackHash) => {
  const rootDir = process.cwd()
  const genesisDir = path.join(rootDir, '.github/code/hypergraph/dag-l0/genesis-node')
  const jarPath = path.join(rootDir, '.github/code/hypergraph/dag-l0.jar')
  
  logWorkflow.info(`Starting genesis node with rollback hash: ${rollbackHash}`)
  
  const env = {
    ...process.env,
    CL_KEYSTORE: 'token-key.p12',
    CL_KEYALIAS: 'token-key',
    CL_PASSWORD: 'password',
    CL_PUBLIC_HTTP_PORT: `${portPrefix}00`,
    CL_P2P_HTTP_PORT: `${portPrefix}01`,
    CL_CLI_HTTP_PORT: `${portPrefix}02`,
    CL_APP_ENV: 'dev',
    CL_COLLATERAL: '0',
  }
  
  const logFile = fs.openSync(path.join(genesisDir, 'dag-l0-rollback.log'), 'a')
  
  const child = spawn('java', [
    '-Denvironment=dev',
    '-jar', jarPath,
    'run-rollback', rollbackHash
  ], {
    cwd: genesisDir,
    env,
    detached: true,
    stdio: ['ignore', logFile, logFile]
  })
  
  child.unref()
  
  // Wait for node to start
  await sleep(10000)
  
  return child.pid
}

// Check for StateProof errors in logs
const checkForStateProofErrors = () => {
  const genesisDir = path.join(process.cwd(), '.github/code/hypergraph/dag-l0/genesis-node')
  const logFiles = ['dag-l0-genesis.log', 'dag-l0-rollback.log']
  
  let hasErrors = false
  let errorLines = []
  
  logFiles.forEach(logFile => {
    const logPath = path.join(genesisDir, logFile)
    try {
      if (fs.existsSync(logPath)) {
        const content = fs.readFileSync(logPath, 'utf8')
        const lines = content.split('\n').filter(line => 
          line.toLowerCase().includes('stateproof') && 
          line.toLowerCase().includes('broken')
        )
        if (lines.length > 0) {
          hasErrors = true
          errorLines = errorLines.concat(lines)
        }
      }
    } catch (e) {
      // Log file may not exist
    }
  })
  
  return { hasErrors, errorLines }
}

// Wait for node to be ready
const waitForNodeReady = async (urls, maxAttempts = 30) => {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const response = await axios.get(`${urls.globalL0Url}/node/info`)
      if (response.data.state === 'Ready') {
        logWorkflow.info('Node is ready')
        return true
      }
    } catch (e) {
      // Node not ready yet
    }
    await sleep(2000)
  }
  throw new Error('Node failed to become ready after rollback')
}

// Wait for ordinal to advance
const waitForOrdinal = async (urls, targetOrdinal, maxAttempts = 60) => {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const current = await getCurrentOrdinal(urls)
      if (current >= targetOrdinal) {
        return current
      }
    } catch (e) {
      // Node may not be ready
    }
    await sleep(2000)
  }
  throw new Error(`Ordinal did not reach ${targetOrdinal}`)
}

// ============================================================================
// TEST: Basic Rollback with 2 Delegated Stakes
// ============================================================================
const testBasicRollback = async (config) => {
  logWorkflow.info('==========================================')
  logWorkflow.info('TEST: Basic Rollback with 2 Delegated Stakes')
  logWorkflow.info('==========================================')
  
  const { urls, portPrefix } = config
  setupDag4Account(urls)
  
  const keysDir = path.join(__dirname, '../delegated_staking/keys')
  const { privateKeyString: pk1, nodeId: node1, account: account1 } = 
    extractKeysAndAccount(path.join(keysDir, 'account1_private_key.txt'))
  const { privateKeyString: pk2, nodeId: node2, account: account2 } = 
    extractKeysAndAccount(path.join(keysDir, 'account2_private_key.txt'))
  
  // Create TL#1 + DS#1
  logWorkflow.info('Creating TL#1 + DS#1...')
  await postNodeParamsNodeId(urls, node1, account1, pk1, 'TestNode1', 1000)
  const tl1Hash = await createTokenLock(account1, urls, dagToDatum(6000))
  logWorkflow.info(`TL#1: ${tl1Hash}`)
  await sleep(30000)
  
  const ds1Hash = await createDelegatedStake(account1, tl1Hash, dagToDatum(6000), node1)
  logWorkflow.info(`DS#1: ${ds1Hash}`)
  await sleep(30000)
  
  const ds1Ordinal = await getCurrentOrdinal(urls)
  logWorkflow.info(`DS#1 confirmed at ordinal ~${ds1Ordinal}`)
  
  // Create TL#2 + DS#2
  logWorkflow.info('Creating TL#2 + DS#2...')
  await postNodeParamsNodeId(urls, node2, account2, pk2, 'TestNode2', 1000)
  const tl2Hash = await createTokenLock(account2, urls, dagToDatum(6000))
  logWorkflow.info(`TL#2: ${tl2Hash}`)
  await sleep(30000)
  
  const ds2Hash = await createDelegatedStake(account2, tl2Hash, dagToDatum(6000), node2)
  logWorkflow.info(`DS#2: ${ds2Hash}`)
  await sleep(30000)
  
  const ds2Ordinal = await getCurrentOrdinal(urls)
  logWorkflow.info(`DS#2 confirmed at ordinal ~${ds2Ordinal}`)
  
  // Get stakes
  const stakes = await getDelegatedStakes(urls, account1.address)
  logWorkflow.info(`Active stakes for account1: ${JSON.stringify(stakes)}`)
  
  // Get rollback hash before killing
  const rollbackHash = await getLatestHash(urls)
  const currentOrdinal = await getCurrentOrdinal(urls)
  logWorkflow.info(`Current ordinal: ${currentOrdinal}, rollback hash: ${rollbackHash}`)
  
  // Kill genesis node
  await killGenesisNode()
  
  // Delete all MPT data
  const genesisDir = path.join(process.cwd(), '.github/code/hypergraph/dag-l0/genesis-node')
  const mptDir = path.join(genesisDir, 'data/mpt_snapshot_info')
  try {
    if (fs.existsSync(mptDir)) {
      fs.rmSync(mptDir, { recursive: true })
      logWorkflow.info('Deleted MPT snapshot info directory')
    }
  } catch (e) {
    logWorkflow.warn('Failed to delete MPT directory')
  }
  
  // Start with rollback
  await startGenesisWithRollback(portPrefix, rollbackHash)
  
  // Wait for node to be ready
  await waitForNodeReady(urls)
  
  // Wait for ordinal to advance past previous
  await waitForOrdinal(urls, currentOrdinal + 1)
  
  // Check for errors
  const { hasErrors, errorLines } = checkForStateProofErrors()
  
  if (hasErrors) {
    logWorkflow.error('StateProof errors found!')
    errorLines.forEach(line => logWorkflow.error(line))
    throw new Error('TEST FAILED: StateProof errors detected')
  }
  
  logWorkflow.info('✅ TEST PASSED: Basic Rollback')
}

// ============================================================================
// TEST: Rollback Between DS Creations
// ============================================================================
const testBetweenDS = async (config) => {
  logWorkflow.info('==========================================')
  logWorkflow.info('TEST: Rollback Between DS Creations')
  logWorkflow.info('==========================================')
  
  const { urls, portPrefix } = config
  setupDag4Account(urls)
  
  const keysDir = path.join(__dirname, '../delegated_staking/keys')
  const { privateKeyString: pk1, nodeId: node1, account: account1 } = 
    extractKeysAndAccount(path.join(keysDir, 'account1_private_key.txt'))
  const { privateKeyString: pk2, nodeId: node2, account: account2 } = 
    extractKeysAndAccount(path.join(keysDir, 'account2_private_key.txt'))
  
  // Create TL#1 + DS#1
  logWorkflow.info('Creating TL#1 + DS#1...')
  await postNodeParamsNodeId(urls, node1, account1, pk1, 'TestNode1', 1000)
  const tl1Hash = await createTokenLock(account1, urls, dagToDatum(6000))
  await sleep(30000)
  const ds1Hash = await createDelegatedStake(account1, tl1Hash, dagToDatum(6000), node1)
  await sleep(30000)
  
  const ds1Ordinal = await getCurrentOrdinal(urls)
  logWorkflow.info(`DS#1 confirmed at ordinal ~${ds1Ordinal}`)
  
  // Wait a few ordinals to create a gap
  const gapStartOrdinal = ds1Ordinal + 2
  await waitForOrdinal(urls, gapStartOrdinal)
  
  // Get rollback hash from the gap
  const rollbackHash = await getLatestHash(urls)
  const rollbackOrdinal = await getCurrentOrdinal(urls)
  logWorkflow.info(`Gap ordinal: ${rollbackOrdinal}`)
  
  // Create TL#2 + DS#2
  logWorkflow.info('Creating TL#2 + DS#2...')
  await postNodeParamsNodeId(urls, node2, account2, pk2, 'TestNode2', 1000)
  const tl2Hash = await createTokenLock(account2, urls, dagToDatum(6000))
  await sleep(30000)
  const ds2Hash = await createDelegatedStake(account2, tl2Hash, dagToDatum(6000), node2)
  await sleep(30000)
  
  const ds2Ordinal = await getCurrentOrdinal(urls)
  logWorkflow.info(`DS#2 confirmed at ordinal ~${ds2Ordinal}`)
  
  // Kill genesis node
  await killGenesisNode()
  
  // Delete ordinals from gap to current
  deleteOrdinals(rollbackOrdinal + 1, ds2Ordinal + 5)
  
  // Start with rollback
  await startGenesisWithRollback(portPrefix, rollbackHash)
  
  // Wait for node to be ready
  await waitForNodeReady(urls)
  
  // Wait for ordinal to advance past DS#2
  await waitForOrdinal(urls, ds2Ordinal + 1)
  
  // Check for errors
  const { hasErrors, errorLines } = checkForStateProofErrors()
  
  if (hasErrors) {
    logWorkflow.error('StateProof errors found!')
    errorLines.forEach(line => logWorkflow.error(line))
    throw new Error('TEST FAILED: StateProof errors detected')
  }
  
  logWorkflow.info('✅ TEST PASSED: Rollback Between DS')
}

// ============================================================================
// TEST: Token Lock Replacement
// ============================================================================
const testReplacement = async (config) => {
  logWorkflow.info('==========================================')
  logWorkflow.info('TEST: Token Lock Replacement')
  logWorkflow.info('==========================================')
  
  const { urls, portPrefix } = config
  setupDag4Account(urls)
  
  const keysDir = path.join(__dirname, '../delegated_staking/keys')
  const { privateKeyString: pk1, nodeId: node1, account: account1 } = 
    extractKeysAndAccount(path.join(keysDir, 'account1_private_key.txt'))
  const { privateKeyString: pk2, nodeId: node2, account: account2 } = 
    extractKeysAndAccount(path.join(keysDir, 'account2_private_key.txt'))
  
  // Create TL#1 + DS#1
  logWorkflow.info('Creating TL#1 + DS#1...')
  await postNodeParamsNodeId(urls, node1, account1, pk1, 'TestNode1', 1000)
  const tl1Hash = await createTokenLock(account1, urls, dagToDatum(6000))
  await sleep(30000)
  const ds1Hash = await createDelegatedStake(account1, tl1Hash, dagToDatum(6000), node1)
  await sleep(30000)
  
  // Create TL#2 + DS#2
  logWorkflow.info('Creating TL#2 + DS#2...')
  await postNodeParamsNodeId(urls, node2, account2, pk2, 'TestNode2', 1000)
  const tl2Hash = await createTokenLock(account2, urls, dagToDatum(6000))
  await sleep(30000)
  const ds2Hash = await createDelegatedStake(account2, tl2Hash, dagToDatum(6000), node2)
  await sleep(30000)
  
  const beforeReplaceOrdinal = await getCurrentOrdinal(urls)
  logWorkflow.info(`Before replacement, ordinal: ${beforeReplaceOrdinal}`)
  
  // Get rollback hash before replacement
  const rollbackHash = await getLatestHash(urls)
  
  // Create replacement token lock
  logWorkflow.info('Creating replacement token lock...')
  const tlReplaceHash = await createTokenLock(account1, urls, dagToDatum(7000), tl1Hash, dagToDatum(6000))
  logWorkflow.info(`TL replacement: ${tlReplaceHash}`)
  await sleep(30000)
  
  const afterReplaceOrdinal = await getCurrentOrdinal(urls)
  logWorkflow.info(`After replacement, ordinal: ${afterReplaceOrdinal}`)
  
  // Kill genesis node
  await killGenesisNode()
  
  // Delete ordinals after the rollback point
  deleteOrdinals(beforeReplaceOrdinal + 1, afterReplaceOrdinal + 5)
  
  // Start with rollback
  await startGenesisWithRollback(portPrefix, rollbackHash)
  
  // Wait for node to be ready
  await waitForNodeReady(urls)
  
  // Wait for ordinal to advance past replacement
  await waitForOrdinal(urls, afterReplaceOrdinal + 1)
  
  // Check for errors
  const { hasErrors, errorLines } = checkForStateProofErrors()
  
  if (hasErrors) {
    logWorkflow.error('StateProof errors found!')
    errorLines.forEach(line => logWorkflow.error(line))
    throw new Error('TEST FAILED: StateProof errors detected')
  }
  
  logWorkflow.info('✅ TEST PASSED: Token Lock Replacement')
}

// ============================================================================
// Main
// ============================================================================
const main = async () => {
  const config = createConfig()
  const { workflow } = config
  
  logWorkflow.info(`MPT Rollback E2E Test - ${workflow}`)
  logWorkflow.info(`Environment: ${RUN_ENV}`)
  
  try {
    switch (workflow) {
      case 'testBasicRollback':
        await testBasicRollback(config)
        break
      case 'testBetweenDS':
        await testBetweenDS(config)
        break
      case 'testReplacement':
        await testReplacement(config)
        break
      default:
        throw new Error(`Unknown workflow: ${workflow}`)
    }
    
    logWorkflow.info('==========================================')
    logWorkflow.info('TEST COMPLETED SUCCESSFULLY')
    logWorkflow.info('==========================================')
    process.exit(0)
  } catch (error) {
    logWorkflow.error(`Test failed: ${error.message}`)
    process.exit(1)
  }
}

main()
