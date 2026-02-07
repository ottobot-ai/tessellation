/**
 * Token Lock Replacement Edge Cases - Simplified essential tests
 *
 * Tests core edge cases for delegated stake + token lock replacement:
 * 1. Basic valid replacement (increase amount)
 * 2. Replace with same amount (should fail - ReplacementLowerThanCurrentTokenLock)
 * 3. Replace non-existent token lock ref (should fail - NothingToReplace)
 * 4. Replace while stake is in withdrawal (should fail - NothingToReplace)
 *
 * Usage:
 * - CI: node token-lock-replacement-edge-cases.js 90 91 testTokenLockReplacementEdgeCases
 * - Local: RUN_ENV=local node token-lock-replacement-edge-cases.js 90 91 testTokenLockReplacementEdgeCases
 */

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
  checkOk,
  checkBadRequest,
  dagToDatum,
  getPrivateKeyAndNodeIdFromFile,
  postNodeParamsNodeId,
  createDelegatedStake,
  withdrawDelegatedStake,
  getAccountDelegatedStakes,
  assertDelegatedStakes,
  fetchStakeWithRewardsBalance,
  createTokenLock,
  assertBalanceChange,
  getNodeParams,
} = require('./lib')

const throwUsage = () => {
  throw new Error(
    'Usage: node script.js <dagl0-port-prefix> <dagl1-port-prefix> <workflow-name>',
  )
}

const createConfig = () => {
  const args = process.argv.slice(2)
  if (args.length < 3) return throwUsage()
  const sharedArgs = parseSharedArgs(args.slice(0, 3), false)
  return { ...sharedArgs }
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

/**
 * Helper to create token lock with error handling for expected failures
 */
const createTokenLockExpectError = async (account, urls, lockAmount, replaceRef, expectedErrorSubstring) => {
  try {
    await account.postTokenLock({
      source: account.address,
      amount: lockAmount,
      tokenL1Url: urls.dagL1Url,
      unlockEpoch: null,
      currencyId: null,
      replaceTokenLockRef: replaceRef,
      fee: 0,
    })
    throw new Error(`Expected error containing "${expectedErrorSubstring}" but request succeeded`)
  } catch (error) {
    if (error.message.includes('Expected error')) throw error
    
    // dag4 SDK returns errors as JSON in error.message: {"errors":[{"message":"..."}]}
    // or as plain text. Extract the actual error content.
    const errorStr = error.message || ''
    
    // Check if this looks like an API validation error (contains error message patterns)
    const isValidationError = errorStr.includes('"errors"') || 
                              errorStr.includes('TokenLock') ||
                              errorStr.includes('Replace') ||
                              errorStr.includes('NothingTo')
    
    if (!isValidationError && (errorStr.includes('ECONNREFUSED') || errorStr.includes('ETIMEDOUT'))) {
      throw new Error(`Network error: ${errorStr}`)
    }
    
    if (!errorStr.includes(expectedErrorSubstring)) {
      throw new Error(`Expected error containing "${expectedErrorSubstring}" but got: ${errorStr}`)
    }
    logWorkflow.info(`Got expected error: ${expectedErrorSubstring}`)
    return true
  }
}

/**
 * Test 1: Basic valid replacement (proves functionality works)
 */
const testBasicValidReplacement = async (urls, account, nodeIds) => {
  logWorkflow.info('---- Start testBasicValidReplacement ----')

  // Check if we already have a stake we can use
  const existingStakes = await getAccountDelegatedStakes(urls, account.address)
  let lockHash, stakeHash, lockAmount

  if (existingStakes.activeDelegatedStakes.length > 0) {
    // Reuse existing stake
    const existingStake = existingStakes.activeDelegatedStakes[0]
    lockHash = existingStake.tokenLockRef
    stakeHash = existingStake.hash
    lockAmount = existingStake.amount
    logWorkflow.info(`Reusing existing stake: ${stakeHash.substring(0, 16)}...`)
    logWorkflow.info(`  Token lock: ${lockHash.substring(0, 16)}...`)
    logWorkflow.info(`  Amount: ${lockAmount}`)
  } else {
    // Create new token lock and stake
    lockAmount = 500000000000 // 5000 DAG
    lockHash = await createTokenLock(account, urls, lockAmount)
    logWorkflow.info(`Created initial token lock: ${lockHash}`)

    // Try each node until we find one without an existing stake
    for (const nodeId of nodeIds) {
      try {
        stakeHash = await createDelegatedStake(account, lockHash, lockAmount, nodeId)
        logWorkflow.info(`Created delegated stake on node ${nodeId.substring(0, 16)}...: ${stakeHash}`)
        break
      } catch (e) {
        if (e.message.includes('StakeExistsForNode')) {
          logWorkflow.info(`Stake already exists on node ${nodeId.substring(0, 16)}..., trying next node`)
          continue
        }
        throw e
      }
    }

    if (!stakeHash) {
      throw new Error('Could not create stake on any node')
    }

    // Wait for stake to be included in global snapshot (required for replacement validation)
    logWorkflow.info('Waiting for stake inclusion in global snapshot...')
    await withRetry(
      async () => {
        const stakeResponse = await getAccountDelegatedStakes(urls, account.address)
        const stake = stakeResponse.activeDelegatedStakes.find(s => s.hash === stakeHash)
        if (!stake) throw new Error('Stake not yet in global snapshot')
        logWorkflow.info(`Stake confirmed in snapshot: ${stake.hash.substring(0, 16)}...`)
        return true
      },
      { name: 'waitForStakeInclusion', maxAttempts: 15, interval: 2000, handleError: () => {} }
    )
    
    // Additional wait for L1 state propagation
    await sleep(5000)
  }

  // Perform valid replacement (increase amount)
  const newAmount = lockAmount + 100000000000 // +1000 DAG
  logWorkflow.info(`Replacing lock with increased amount: ${lockAmount} -> ${newAmount}`)

  // Retry if we get NothingToReplace (lock not yet in L1 stored state)
  const newLockHash = await withRetry(
    async () => {
      try {
        return await createTokenLock(account, urls, newAmount, lockHash, lockAmount)
      } catch (e) {
        if (e.message.includes('NothingToReplace')) {
          logWorkflow.info('Lock not yet in L1 state, retrying...')
          throw e // Retry
        }
        throw e // Other errors propagate
      }
    },
    { name: 'validReplacement', maxAttempts: 10, interval: 3000, handleError: () => {} }
  )

  logWorkflow.info(`Created replacement lock: ${newLockHash}`)

  // Verify stake was updated with new lock hash and amount
  await withRetry(
    async () => {
      const stakeResponse = await getAccountDelegatedStakes(urls, account.address)
      const stake = stakeResponse.activeDelegatedStakes.find(s => s.hash === stakeHash)
      if (!stake) throw new Error('Stake not found')
      if (stake.tokenLockRef !== newLockHash) {
        throw new Error(`TokenLockRef not updated: expected ${newLockHash}, got ${stake.tokenLockRef}`)
      }
      if (stake.amount !== newAmount) {
        throw new Error(`Amount not updated: expected ${newAmount}, got ${stake.amount}`)
      }
      return true
    },
    { name: 'verifyReplacementUpdate', maxAttempts: 20, interval: 3000, handleError: () => {} }
  )

  logWorkflow.info('---- End testBasicValidReplacement ----')
  return { lockHash: newLockHash, stakeHash, lockAmount: newAmount }
}

/**
 * Test 2: Replace with same amount (should fail)
 */
const testReplaceSameAmount = async (urls, account, existingLockHash, existingAmount) => {
  logWorkflow.info('---- Start testReplaceSameAmount ----')

  // Try to replace with same amount - should fail with ReplacementLowerThanCurrentTokenLock
  // Retry if we get NothingToReplace (lock not yet in L1 stored state)
  await withRetry(
    async () => {
      try {
        await createTokenLockExpectError(
          account,
          urls,
          existingAmount, // Same amount
          existingLockHash,
          'ReplacementLowerThanCurrentTokenLock'
        )
        return true
      } catch (e) {
        if (e.message.includes('NothingToReplace')) {
          logWorkflow.info('Lock not yet in L1 state, retrying...')
          throw e // Retry
        }
        throw e // Other errors propagate
      }
    },
    { name: 'replaceWithSameAmount', maxAttempts: 10, interval: 3000, handleError: () => {} }
  )

  logWorkflow.info('---- End testReplaceSameAmount ----')
}

/**
 * Test 3: Replace non-existent token lock ref (should fail)
 */
const testReplaceNonExistentRef = async (urls, account) => {
  logWorkflow.info('---- Start testReplaceNonExistentRef ----')

  const fakeRef = '0000000000000000000000000000000000000000000000000000000000000000'
  const amount = 500000000000

  await createTokenLockExpectError(
    account,
    urls,
    amount,
    fakeRef,
    'NothingToReplace'
  )

  logWorkflow.info('---- End testReplaceNonExistentRef ----')
}

/**
 * Test 4: Replace while stake is in withdrawal (should fail)
 */
const testReplaceWhileInWithdrawal = async (urls, account, nodeId) => {
  logWorkflow.info('---- Start testReplaceWhileInWithdrawal ----')

  // Create a fresh token lock and stake for this test
  const lockAmount = 500000000000 // 5000 DAG
  const lockHash = await createTokenLock(account, urls, lockAmount)
  logWorkflow.info(`Created token lock for withdrawal test: ${lockHash}`)

  const stakeHash = await createDelegatedStake(account, lockHash, lockAmount, nodeId)
  logWorkflow.info(`Created delegated stake: ${stakeHash}`)

  await sleep(5000)

  // Initiate withdrawal
  await withdrawDelegatedStake(account, stakeHash)
  logWorkflow.info('Initiated stake withdrawal')

  // Verify stake is in pendingWithdrawals
  await withRetry(
    async () => {
      const stakeResponse = await getAccountDelegatedStakes(urls, account.address)
      const pending = stakeResponse.pendingWithdrawals.find(s => s.hash === stakeHash)
      if (!pending) throw new Error('Stake not in pendingWithdrawals')
      return true
    },
    { name: 'verifyPendingWithdrawal', maxAttempts: 10, interval: 2000, handleError: () => {} }
  )

  // Try to replace token lock while stake is in withdrawal - should FAIL
  // Once a stake is withdrawn, the associated token lock cannot be replaced
  // (validator returns NothingToReplace because the lock is no longer active)
  const newAmount = lockAmount + 100000000000 // +1000 DAG
  await createTokenLockExpectError(
    account,
    urls,
    newAmount,
    lockHash,
    'NothingToReplace'
  )
  logWorkflow.info('Correctly rejected replacement of token lock in withdrawal')

  logWorkflow.info('---- End testReplaceWhileInWithdrawal ----')
}

/**
 * Setup node parameters for testing (required for delegated staking)
 */
const setupNodeParameters = async (urls) => {
  logWorkflow.info('---- Setting up node parameters ----')

  // Check if node params already exist (for local testing or re-runs)
  const existingParams = await getNodeParams(urls)
  
  if (existingParams.length >= 1) {
    logWorkflow.info(`Node parameters already configured: ${existingParams.length} nodes`)
    return existingParams
  }

  // For CI, we need to extract keys and set up node params
  if (RUN_ENV !== 'ci') {
    throw new Error('Node parameters must be configured before running local tests. ' +
      'Run the main delegated-staking.js test first, or manually set up node params.')
  }

  const {
    privateKeyString: privateKeyString1,
    nodeId: nodeId1,
    account: account1,
  } = extractKeysAndAccount('../../code/hypergraph/dag-l0/genesis-node/id_ecdsa.hex')

  const {
    privateKeyString: privateKeyString2,
    nodeId: nodeId2,
    account: account2,
  } = extractKeysAndAccount('../../code/hypergraph/dag-l0/validator-1/id_ecdsa.hex')

  // Set up node 1 params
  const ur1 = await postNodeParamsNodeId(
    urls, nodeId1, account1, privateKeyString1,
    'EdgeCaseTestNode1', 5000000
  )
  checkOk(ur1)
  
  // Set up node 2 params
  const ur2 = await postNodeParamsNodeId(
    urls, nodeId2, account2, privateKeyString2,
    'EdgeCaseTestNode2', 6000000
  )
  checkOk(ur2)
  
  await sleep(5000)

  const nodeParams = await getNodeParams(urls)
  logWorkflow.info(`Node parameters configured: ${nodeParams.length} nodes`)

  return nodeParams
}

/**
 * Main test runner - simplified to 4 essential tests
 */
const testTokenLockReplacementEdgeCases = async (urls) => {
  logWorkflow.info('========================================')
  logWorkflow.info('Token Lock Replacement Edge Cases Tests (Simplified)')
  logWorkflow.info('========================================')

  // Setup - use key3 to avoid conflicts with other tests using key4
  const account = setupDag4Account(urls)
  account.loginPrivateKey(PRIVATE_KEYS.key3)
  logWorkflow.info(`Using account: ${account.address}`)

  const nodeParams = await setupNodeParameters(urls)
  const nodeIds = nodeParams.map(p => p.peerId)
  const nodeId2 = nodeParams.length > 1 ? nodeParams[1].peerId : nodeParams[0].peerId

  // Test 1: Basic valid replacement (proves functionality works)
  const { lockHash, stakeHash, lockAmount } = await testBasicValidReplacement(urls, account, nodeIds)

  // Test 2: Replace with same amount (should fail)
  await testReplaceSameAmount(urls, account, lockHash, lockAmount)

  // Test 3: Replace non-existent token lock ref (should fail)
  await testReplaceNonExistentRef(urls, account)

  // Test 4: Replace while stake is in withdrawal (should fail)
  await testReplaceWhileInWithdrawal(urls, account, nodeId2)

  logWorkflow.info('========================================')
  logWorkflow.info('All essential edge case tests completed!')
  logWorkflow.info('========================================')
}

const executeWorkflowByType = async (workflowType) => {
  const config = createConfig()
  const urls = createNetworkConfig(config)

  switch (workflowType) {
    case 'testTokenLockReplacementEdgeCases':
      await testTokenLockReplacementEdgeCases(urls)
      break
    default:
      throw new Error(`Unknown workflow type: ${workflowType}`)
  }
}

const workflowType = process.argv[4]
if (!workflowType) {
  logWorkflow.error('workflowType arg not found.')
  throwUsage()
}

executeWorkflowByType(workflowType).catch((err) => {
  logWorkflow.error('Test failed:', err.message)
  if (RUN_ENV !== 'local') {
    throw err
  }
})