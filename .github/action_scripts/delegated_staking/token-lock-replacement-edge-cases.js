/**
 * Token Lock Replacement Edge Cases - Extended tests for delegated stake + token lock replacement
 *
 * Tests edge cases not covered by the main delegated-staking.js tests:
 * 1. Basic valid replacement (minimum increase) - establishes clean baseline
 * 2. Replace with same amount (should fail - ReplacementLowerThanCurrentTokenLock)
 * 3. Replace with less amount (should fail - ReplacementLowerThanCurrentTokenLock)
 * 4. Replace non-existent token lock ref (should fail - NothingToReplace)
 * 5. Multiple sequential replacements (3 in a row)
 * 6. Replace while stake is in withdrawal (pendingWithdrawals)
 *
 * Transition Set Logic:
 * - Test 1 creates initial state and performs successful replacement
 * - Tests 2-3 operate on the established successful state (failure tests)
 * - Test 4 is independent
 * - Test 5 continues from Test 1's successful state
 * - Test 6 is independent
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
 * Test 1: Basic valid replacement (minimum increase) - establishes clean baseline
 * This creates the initial setup and performs one successful replacement
 */
const testBasicValidReplacement = async (urls, account, nodeIds) => {
  logWorkflow.info('---- Start testBasicValidReplacement ----')

  // Check if we already have a stake we can use
  const existingStakes = await getAccountDelegatedStakes(urls, account.address)
  let initialLockHash, stakeHash, initialAmount

  if (existingStakes.activeDelegatedStakes.length > 0) {
    // Reuse existing stake
    const existingStake = existingStakes.activeDelegatedStakes[0]
    initialLockHash = existingStake.tokenLockRef
    stakeHash = existingStake.hash
    initialAmount = existingStake.amount
    logWorkflow.info(`Reusing existing stake: ${stakeHash.substring(0, 16)}...`)
    logWorkflow.info(`  Token lock: ${initialLockHash.substring(0, 16)}...`)
    logWorkflow.info(`  Amount: ${initialAmount}`)
  } else {
    // Create new token lock and stake
    initialAmount = 600000000000 // 6000 DAG
    initialLockHash = await createTokenLock(account, urls, initialAmount)
    logWorkflow.info(`Created initial token lock: ${initialLockHash}`)

    // Try each node until we find one without an existing stake
    for (const nodeId of nodeIds) {
      try {
        stakeHash = await createDelegatedStake(account, initialLockHash, initialAmount, nodeId)
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

  // Now perform a successful replacement with minimum valid increase
  const newAmount = initialAmount + 1 // Minimum valid increase
  logWorkflow.info(`Performing successful replacement: ${initialAmount} -> ${newAmount}`)
  
  // Retry if we get NothingToReplace (lock not yet in L1 stored state)
  const newLockHash = await withRetry(
    async () => {
      try {
        return await createTokenLock(account, urls, newAmount, initialLockHash, initialAmount)
      } catch (e) {
        if (e.message.includes('NothingToReplace')) {
          logWorkflow.info('Lock not yet in L1 state, retrying...')
          throw e // Retry
        }
        throw e // Other errors propagate
      }
    },
    { name: 'basicValidReplacement', maxAttempts: 10, interval: 3000, handleError: () => {} }
  )
  
  logWorkflow.info(`Created replacement lock: ${newLockHash}`)

  // Verify delegated stake updated AND new lock is active
  logWorkflow.info('Waiting for snapshot inclusion and stake update...')
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
    { name: 'verifyBasicReplacementUpdate', maxAttempts: 20, interval: 3000, handleError: () => {} }
  )
  
  // Extra wait to ensure the new lock is fully active on GL0 for subsequent tests
  logWorkflow.info('Replacement confirmed, waiting for GL0 sync...')
  await sleep(10000)

  logWorkflow.info('---- End testBasicValidReplacement ----')
  return { lockHash: newLockHash, stakeHash, lockAmount: newAmount }
}

/**
 * Test 2: Replace with same amount (should fail)
 * Operates on the established successful state from Test 1
 */
const testReplaceSameAmount = async (urls, account, existingLockHash, existingAmount) => {
  logWorkflow.info('---- Start testReplaceSameAmount ----')

  // Try to replace with same amount - should fail with ReplacementLowerThanCurrentTokenLock
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
 * Test 3: Replace with less amount (should fail)
 * Operates on the established successful state from Test 1
 */
const testReplaceLessAmount = async (urls, account, existingLockHash, existingAmount) => {
  logWorkflow.info('---- Start testReplaceLessAmount ----')

  // Use amount less than current
  const lessAmount = existingAmount - 100000000000 // -1000 DAG

  await withRetry(
    async () => {
      try {
        await createTokenLockExpectError(
          account,
          urls,
          lessAmount,
          existingLockHash,
          'ReplacementLowerThanCurrentTokenLock'
        )
        return true
      } catch (e) {
        if (e.message.includes('NothingToReplace')) {
          logWorkflow.info('Lock not yet in L1 state, retrying...')
          throw e
        }
        throw e
      }
    },
    { name: 'replaceWithLessAmount', maxAttempts: 10, interval: 3000, handleError: () => {} }
  )

  logWorkflow.info('---- End testReplaceLessAmount ----')
}

/**
 * Test 4: Replace non-existent token lock ref (should fail)
 * Independent test - doesn't affect other tests
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
 * Test 5: Multiple sequential replacements
 * Continues from the established successful state from Test 1
 * Note: After each replacement, the OLD lock is removed and NEW lock becomes active.
 * Subsequent replacements must reference the NEW lock hash.
 */
const testMultipleSequentialReplacements = async (urls, account, currentLockHash, currentAmount, stakeHash) => {
  logWorkflow.info('---- Start testMultipleSequentialReplacements ----')

  let lockHash = currentLockHash
  let amount = currentAmount

  // Do 3 sequential replacements
  for (let i = 1; i <= 3; i++) {
    const newAmount = amount + 100000000000 // +1000 DAG each time
    logWorkflow.info(`Sequential replacement ${i}: ${amount} -> ${newAmount}`)
    logWorkflow.info(`  Replacing lock: ${lockHash.substring(0, 16)}...`)

    const newLockHash = await createTokenLock(account, urls, newAmount, lockHash, amount)
    logWorkflow.info(`  Created replacement ${i}: ${newLockHash.substring(0, 16)}...`)

    // Wait for inclusion before verifying
    await sleep(3000)

    // Verify delegated stake updated after each replacement
    await withRetry(
      async () => {
        const stakeResponse = await getAccountDelegatedStakes(urls, account.address)
        const stake = stakeResponse.activeDelegatedStakes.find(s => s.hash === stakeHash)
        if (!stake) throw new Error('Stake not found')
        if (stake.tokenLockRef !== newLockHash) {
          throw new Error(`Replacement ${i}: TokenLockRef not updated. Expected ${newLockHash.substring(0,16)}, got ${stake.tokenLockRef?.substring(0,16)}`)
        }
        if (stake.amount !== newAmount) {
          throw new Error(`Replacement ${i}: Amount not updated. Expected ${newAmount}, got ${stake.amount}`)
        }
        return true
      },
      { name: `verifySequentialReplacement${i}`, maxAttempts: 10, interval: 2000, handleError: () => {} }
    )

    // IMPORTANT: Update lockHash to the NEW lock for the next iteration
    lockHash = newLockHash
    amount = newAmount
    logWorkflow.info(`  Sequential replacement ${i} verified ✓`)
    
    // Wait for snapshot inclusion and GL0 sync before next replacement
    if (i < 3) {
      logWorkflow.info('  Waiting for GL0 sync before next replacement...')
      await sleep(10000)
    }
  }

  logWorkflow.info('---- End testMultipleSequentialReplacements ----')
  return { finalLockHash: lockHash, finalAmount: amount }
}

/**
 * Test 6: Replace while stake is in withdrawal (pendingWithdrawals)
 * Independent test - creates its own setup to avoid affecting other tests
 */
const testReplaceWhileInWithdrawal = async (urls, account, nodeId) => {
  logWorkflow.info('---- Start testReplaceWhileInWithdrawal ----')

  // Create a fresh token lock and stake for this test
  const lockAmount = 600000000000 // 6000 DAG
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
      logWorkflow.info(`Confirmed stake in pendingWithdrawals: ${pending.hash.substring(0, 16)}...`)
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
 * Verify that pending withdrawals retain their original tokenLockRef
 * This is a key requirement - pending withdrawals should not be affected by replacements
 */
const verifyPendingWithdrawalUnchanged = async (urls, account, originalStakeHash, originalLockHash) => {
  logWorkflow.info('---- Verifying pending withdrawal unchanged ----')
  
  await withRetry(
    async () => {
      const stakeResponse = await getAccountDelegatedStakes(urls, account.address)
      const pendingStake = stakeResponse.pendingWithdrawals.find(s => s.hash === originalStakeHash)
      
      if (!pendingStake) {
        throw new Error('Pending withdrawal stake not found')
      }
      
      if (pendingStake.tokenLockRef !== originalLockHash) {
        throw new Error(
          `Pending withdrawal tokenLockRef changed! Expected ${originalLockHash}, got ${pendingStake.tokenLockRef}`
        )
      }
      
      logWorkflow.info(`Confirmed pending withdrawal retains original lock: ${originalLockHash.substring(0, 16)}...`)
      return true
    },
    { name: 'verifyPendingWithdrawalUnchanged', maxAttempts: 15, interval: 3000, handleError: () => {} }
  )
  
  logWorkflow.info('---- End verifyPendingWithdrawalUnchanged ----')
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
 * Main test runner with clean transition sets
 */
const testTokenLockReplacementEdgeCases = async (urls) => {
  logWorkflow.info('========================================')
  logWorkflow.info('Token Lock Replacement Edge Cases Tests')
  logWorkflow.info('Clean Transition Sets - Fixed Flow')
  logWorkflow.info('========================================')

  // Setup - use key3 to avoid conflicts with other tests using key4
  const account = setupDag4Account(urls)
  account.loginPrivateKey(PRIVATE_KEYS.key3)
  logWorkflow.info(`Using account: ${account.address}`)

  const nodeParams = await setupNodeParameters(urls)
  const nodeIds = nodeParams.map(p => p.peerId)
  const nodeId2 = nodeParams.length > 1 ? nodeParams[1].peerId : nodeParams[0].peerId

  // Test 1: Basic valid replacement - establishes clean baseline
  const { lockHash, stakeHash, lockAmount } = await testBasicValidReplacement(urls, account, nodeIds)

  // Test 2: Replace with same amount (should fail) - operates on established state
  await testReplaceSameAmount(urls, account, lockHash, lockAmount)

  // Test 3: Replace with less amount (should fail) - operates on established state
  await testReplaceLessAmount(urls, account, lockHash, lockAmount)

  // Test 4: Replace non-existent token lock ref (should fail) - independent
  await testReplaceNonExistentRef(urls, account)

  // Test 5: Multiple sequential replacements - continues from Test 1's state
  const { finalLockHash } = await testMultipleSequentialReplacements(
    urls, account, lockHash, lockAmount, stakeHash
  )

  // Test 6: Replace while stake is in withdrawal - independent
  await testReplaceWhileInWithdrawal(urls, account, nodeId2)

  // Verify that the pending withdrawal from Test 6 retains its original tokenLockRef
  // This confirms that pending withdrawals are unaffected by token lock replacements
  await verifyPendingWithdrawalUnchanged(urls, account, stakeHash, finalLockHash)

  logWorkflow.info('========================================')
  logWorkflow.info('All edge case tests completed!')
  logWorkflow.info('Clean transition sets verified ✓')
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