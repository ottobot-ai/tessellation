const axios = require('axios')
const elliptic = require('elliptic')
const fs = require('fs')

const {
  sleep,
  withRetry,
  withRetryOrdinal,
  waitForTxInclusion,
  generateProof,
  SerializerType,
  logWorkflow,
} = require('../shared')

const checkOk = (response) => {
  if (response.status !== 200) {
    throw new Error(`Node returned ${response.status} instead of 200`)
  }
}

const checkBadRequest = (response) => {
  if (response.status !== 400) {
    throw new Error(`Node returned ${response.status} instead of 400`)
  }
}

const dagToDatum = (dag) => {
  return Math.round(dag * 1e8)
}

function getPrivateKeyAndNodeIdFromFile(filePath) {
  const privateKeyHex = fs.readFileSync(filePath, 'utf8').trim()

  const privateKeyBuffer = Buffer.from(privateKeyHex, 'hex')

  try {
    const ec = new elliptic.ec('secp256k1')

    const privateKeyString = privateKeyBuffer.toString('hex')
    const keyPair = ec.keyFromPrivate(privateKeyBuffer)

    const uncompressedPublicKey = keyPair.getPublic(false, 'hex') // Uncompressed format
    const nodeId = uncompressedPublicKey.slice(2) // Remove the '0x04' prefix

    return { privateKeyString, nodeId }
  } catch (error) {
    console.error('Error processing the private key:', error)
    throw error
  }
}

const postNodeParamsNodeId = async (
  urls,
  nodeId,
  account,
  privateKeyString,
  parametersName,
  rewardFraction,
) => {
  let parent = {
    ordinal: 0,
    hash: '0000000000000000000000000000000000000000000000000000000000000000',
  }

  try {
    const response = await axios.get(
      `${urls.globalL0Url}/node-params/${nodeId}?t=${Date.now()}`,
      {
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          Pragma: 'no-cache',
          Expires: '0',
        },
      },
    )
    if (response.status === 200 && response.data) {
      parent = response.data.lastRef
    }
  } catch (error) {
    // NOOP
  }

  const unsignedNodeParams = {
    source: account.address,
    delegatedStakeRewardParameters: {
      rewardFraction: rewardFraction,
    },
    nodeMetadataParameters: {
      name: parametersName,
      description: parametersName,
    },
    parent: parent,
  }

  const proof = await generateProof(
    unsignedNodeParams,
    privateKeyString,
    account,
    SerializerType.BROTLI,
  )
  const content = { value: unsignedNodeParams, proofs: [{ ...proof }] }

  try {
    const updateResponse = await axios.post(
      `${urls.globalL0Url}/node-params`,
      content,
    )
    await sleep(2000)
    return updateResponse
  } catch (error) {
    if (axios.isAxiosError(error)) {
      return error.response
    } else {
      throw error
    }
  }
}

const createDelegatedStake = async (account, lockHash, lockAmount, nodeId) => {
  // Retry on InvalidTokenLock and InvalidParent: GL0's delegated-stake
  // validator reads from the local MPT directly (`mptStore.getActiveTokenLocks`,
  // `mptStore.getDelegatedStakes`) while the HTTP routes that advertise
  // tokenLockRef / lastReference back to the client read through the
  // overlay-aware `pendingReader`. The two views disagree during the window
  // between a write landing in the overlay's pending branch and that branch
  // being folded into the underlying base MPT (typically a few hundred ms,
  // longer during reorgs or finality stalls). The route hands the client a
  // parent reference the validator hasn't seen yet → InvalidParent. Retry
  // semantics mirror the InvalidTokenLock case below: 120 × 3s = 6 min.
  // The architecturally correct fix is to migrate the validator to read
  // through the same `GlobalStateReader` the routes use (#198 in the task
  // list); this retry is the JS-side workaround until that lands.
  const maxAttempts = 120
  const intervalMs = 3000
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const { hash } = await account.postDelegatedStake({
        source: account.address,
        nodeId: nodeId,
        amount: lockAmount,
        fee: 0,
        tokenLockRef: lockHash,
      })
      return hash
    } catch (error) {
      const msg = error?.message || String(error)
      if ((msg.includes('InvalidTokenLock') || msg.includes('InvalidParent')) && attempt < maxAttempts) {
        await new Promise(r => setTimeout(r, intervalMs))
        continue
      }
      throw error
    }
  }
}

const withdrawDelegatedStake = async (account, stakeHash) => {
  const { hash } = await account.putWithdrawDelegatedStake({
    source: account.address,
    stakeRef: stakeHash,
  })

  return hash
}

const getAccountDelegatedStakes = async (urls, address) => {
  const response = await axios.get(
    `${urls.globalL0Url}/delegated-stakes/${address}/info?t=${Date.now()}`,
    {
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        Pragma: 'no-cache',
        Expires: '0',
      },
    },
  )
  checkOk(response)
  return response.data
}

// Assert at least the passed keys are present in the array of objects
const assertAllKeysMatch = (arr, obj) => {
  const isValid = arr.some((item) =>
    Object.entries(obj).every(([key, value]) => item[key] === value),
  )

  if (!isValid) {
    // logWorkflow.info(JSON.stringify(arr))
    throw new Error(
      `Expected all keys to be present in response: ${JSON.stringify(obj)}`,
    )
  }
}

const assertDelegatedStakes = (stakeResponse, activeStakes, pendingStakes) => {
  const expectedActiveLength = activeStakes.length
  const actualActiveLength = stakeResponse.activeDelegatedStakes.length
  if (expectedActiveLength !== actualActiveLength) {
    throw new Error(
      `Expected ${expectedActiveLength} active stakes but got ${actualActiveLength}`,
    )
  }

  Object.values(activeStakes).map((stakeItem) => {
    assertAllKeysMatch(stakeResponse.activeDelegatedStakes, stakeItem)
  })

  const expectedPendingLength = pendingStakes.length
  const actualPendingLength = stakeResponse.pendingWithdrawals.length
  if (expectedPendingLength !== actualPendingLength) {
    throw new Error(
      `Expected ${expectedPendingLength} active stakes but got ${actualPendingLength}`,
    )
  }

  Object.values(pendingStakes).map((stakeItem) => {
    assertAllKeysMatch(stakeResponse.pendingWithdrawals, stakeItem)
  })
}

// Get stake to update and wait until it has some rewards
const fetchStakeWithRewardsBalance = async (
  urls,
  address,
  stakeHash,
  nodeId = null,
) => {
  return withRetry(
    async () => {
      const stakeResponse = await getAccountDelegatedStakes(urls, address)
      const stake = stakeResponse.activeDelegatedStakes.find(
        (stake) => stake.hash === stakeHash && stake.rewardAmount > 0,
      )

      if (!stake) {
        throw new Error('Stake not found with rewards balance')
      }

      const stakeAlreadyExists = stakeResponse.activeDelegatedStakes.find(
        (stake) => {
          return (
            (nodeId ? stake.nodeId === nodeId : true) &&
            address === stake.source
          )
        },
      )

      if (stakeAlreadyExists) {
        throw new Error('Cant update, stake already exists')
      }

      return stake
    },
    {
      name: 'FetchStakeWithRewardsBalance',
      maxAttempts: 40,
      interval: 5 * 1000,
      handleError: () => {},
    },
  )
}

// Poll gl1's token-lock last-reference until it reflects `expectedHash` as the chain tip.
// gl1's last-reference advances on gl1's OWN follow/accept clock — it mirrors gl0's FINALIZED
// `lastTokenLockRefs` via the inclusion-proof follow — which lags gl0's stake-acceptance clock
// that the rest of this test polls. Used as the chain-parent precondition for replacements.
const waitForTokenLockLastRef = async (urls, address, expectedHash, options = {}) => {
  const maxAttempts = options.maxAttempts || 90
  const intervalMs = options.interval || 2000
  let lastSeen = null
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const response = await axios.get(
      `${urls.dagL1Url}/token-locks/last-reference/${address}?t=${Date.now()}`,
      {
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          Pragma: 'no-cache',
          Expires: '0',
        },
      }
    )
    checkOk(response)
    lastSeen = response.data && response.data.hash
    if (lastSeen === expectedHash) return response.data
    await new Promise(r => setTimeout(r, intervalMs))
  }
  throw new Error(
    `gl1 token-lock last-reference for ${address.substring(0, 12)}... did not advance to ` +
      `${expectedHash.substring(0, 16)}... within ${(maxAttempts * intervalMs) / 1000}s ` +
      `(last seen: ${(lastSeen || 'none').substring(0, 16)}...)`
  )
}

const createTokenLock = async (account, urls, lockAmount, replaceRef = null, replaceBalance = 0) => {
  const initialBalance = dagToDatum(await account.getBalance())

  // CHAIN-PARENT PRECONDITION (gl1 follow-lag, 2026-05-29): for a replacement, the dag4 SDK builds the
  // new lock's chain parent from gl1's CURRENT /token-locks/last-reference. That advances on gl1's own
  // follow/accept clock (mirroring gl0's FINALIZED lastTokenLockRefs via the inclusion-proof follow),
  // which lags gl0's stake-acceptance clock the rest of this test polls. If we submit before gl1 reflects
  // the lock being replaced as the chain tip, the SDK fetches a STALE parent, the replacement is assigned
  // the same ordinal as the lock it replaces, and gl1 rejects it with Conflict{ordinal=...}. Wait until
  // gl1's last-reference IS the lock we're replacing so the replacement chains on the correct parent.
  // Correct-by-design precondition (wait for the dependency), not an error-string retry.
  if (replaceRef) {
    await waitForTokenLockLastRef(urls, account.address, replaceRef)
  }

  // Retry on NothingToReplace: L1's `TokenLockService.offer` validator reads from the local MPT
  // (`mptStore.getActiveTokenLocks`). Under Nakamoto, an accepted token lock can transiently disappear
  // from the serving node's MPT during a chain reorg — the MPT rewinds to savepoint and reapplies
  // deltas, so there's a short window where a replacement-reference lock isn't in `activeTokenLocks`.
  // Same pattern as `createDelegatedStake`'s InvalidTokenLock retry at line 119.
  const maxAttempts = 120
  const intervalMs = 3000
  let hash
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const resp = await account.postTokenLock({
        source: account.address,
        amount: lockAmount,
        tokenL1Url: urls.dagL1Url,
        unlockEpoch: null,
        currencyId: null,
        replaceTokenLockRef: replaceRef,
        fee: 0,
      })
      hash = resp.hash
      break
    } catch (error) {
      const msg = error?.message || String(error)
      // Only retry when the replacement reference is transiently missing; surface other errors immediately.
      if (replaceRef && msg.includes('NothingToReplace') && attempt < maxAttempts) {
        await new Promise(r => setTimeout(r, intervalMs))
        continue
      }
      throw error
    }
  }

  if (!hash) {
    throw new Error('Failed to create TokenLock')
  }

  // Use atLeast because delegator rewards accrue between the balance
  // snapshot and the retry check. Without this, the exact-match assertion
  // fails as rewards push the actual balance above the expected value.
  await withRetry(
    async () => assertBalanceChange(account, initialBalance - lockAmount + replaceBalance, { atLeast: true }),
    {
      name: 'assertBalanceChangeAfterTokenLock',
      maxAttempts: 60,
      interval: 2000,
      handleError: () => {},
    },
  )

  return hash
}

const assertBalanceChange = async (account, expectedBalanceDatum, { atLeast = false } = {}) => {
  const balance = dagToDatum(await account.getBalance())

  if (atLeast) {
    if (balance < expectedBalanceDatum) {
      throw new Error(
        `Invalid balance: Expected balance to be at least ${expectedBalanceDatum} but got ${balance}`,
      )
    }
  } else if (balance !== expectedBalanceDatum) {
    throw new Error(
      `Invalid balance: Expected balance to be ${expectedBalanceDatum} but got ${balance}`,
    )
  }
}

const getNodeParams = async (urls) => {
  logWorkflow.info(`Request to: ${urls.globalL0Url}/node-params`)
  const response = await axios.get(
    `${urls.globalL0Url}/node-params?t=${Date.now()}`,
    {
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        Pragma: 'no-cache',
        Expires: '0',
      },
    },
  )
  checkOk(response)
  return response.data
}

const fetchSnapshot = async (urls, ordinal) => {
  logWorkflow.info(`Fetching snapshot: ${ordinal} `)

  const response = await axios.get(
    `${urls.globalL0Url}/global-snapshots/${ordinal}?t=${Date.now()}`,
    {
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        Pragma: 'no-cache',
        Expires: '0',
      },
    },
  )
  checkOk(response)
  return response.data
}

const assertRewardTxnInSnapshot = async (snapshot, account, amount) => {
  // Use >= because additional rewards may accrue between the stake snapshot
  // and the withdrawal processing, especially in Nakamoto mode where
  // snapshots are produced at ~10s intervals.
  const rewardTxn = snapshot.value.rewards.find((txn) => {
    return txn.amount >= amount && txn.destination === account.address
  })

  if (!rewardTxn) {
    throw new Error('Reward txn not found for withdrawal')
  }
}

const assertTokenUnlockInSnapshot = async (
  snapshot,
  account,
  lockHash,
  amount,
) => {
  const tokenUnlock = snapshot.value.artifacts.find((item) => {
    return (
      item.hasOwnProperty('TokenUnlock') &&
      item.TokenUnlock.tokenLockRef === lockHash &&
      item.TokenUnlock.amount === amount &&
      item.TokenUnlock.source === account.address
    )
  })

  if (!tokenUnlock) {
    throw new Error('TokenUnlock not found for withdrawal')
  }
}

/**
 * Wait for a delegated stake to appear in activeDelegatedStakes using ordinal-aware retry.
 * This is more robust than wall-clock retries because it detects dropped transactions.
 * 
 * @param {Object} urls - Network URLs including globalL0Url
 * @param {string} address - Account address
 * @param {string} stakeHash - Expected stake hash
 * @param {Object} [options] - Additional options for withRetryOrdinal
 * @returns {Promise<Object>} - The stake object when found
 */
const waitForStakeInclusion = async (urls, address, stakeHash, options = {}) => {
  return withRetryOrdinal(
    async () => {
      const response = await getAccountDelegatedStakes(urls, address)
      const stake = response.activeDelegatedStakes.find(s => s.hash === stakeHash)
      if (!stake) {
        throw new Error(`Stake ${stakeHash.substring(0, 16)}... not in activeDelegatedStakes`)
      }
      return stake
    },
    {
      globalL0Url: urls.globalL0Url,
      name: 'waitForStakeInclusion',
      // 8-node reorg-storm budget: txs can sit in event mempool 15-25 ordinals before
      // a non-paused producer drains them. Default 10 was too tight (~one ordinal short
      // in observed runs). Same value as testUpdateDelegatedStake's per-call override.
      maxOrdinalMisses: 40,
      maxStalledChecks: 75,
      interval: 2000,
      ...options
    }
  )
}

/**
 * Wait for a delegated stake to move to pendingWithdrawals using ordinal-aware retry.
 * 
 * @param {Object} urls - Network URLs including globalL0Url
 * @param {string} address - Account address
 * @param {string} stakeHash - Expected stake hash
 * @param {Object} [options] - Additional options for withRetryOrdinal
 * @returns {Promise<Object>} - The pending withdrawal object when found
 */
const waitForStakeWithdrawal = async (urls, address, stakeHash, options = {}) => {
  return withRetryOrdinal(
    async () => {
      const response = await getAccountDelegatedStakes(urls, address)
      const pending = response.pendingWithdrawals.find(s => s.hash === stakeHash)
      if (!pending) {
        throw new Error(`Stake ${stakeHash.substring(0, 16)}... not in pendingWithdrawals`)
      }
      return pending
    },
    {
      globalL0Url: urls.globalL0Url,
      name: 'waitForStakeWithdrawal',
      // 8-node reorg-storm budget: txs can sit in event mempool 15-25 ordinals before
      // a non-paused producer drains them. Default 10 was too tight (~one ordinal short
      // in observed runs). Same value as testUpdateDelegatedStake's per-call override.
      maxOrdinalMisses: 40,
      maxStalledChecks: 75,
      interval: 2000,
      ...options
    }
  )
}

/**
 * Wait for a token lock to appear in account's active token locks using ordinal-aware retry.
 * 
 * @param {Object} urls - Network URLs including globalL0Url  
 * @param {string} address - Account address
 * @param {string} lockHash - Expected lock hash
 * @param {Object} [options] - Additional options for withRetryOrdinal
 * @returns {Promise<Object>} - The token lock object when found
 */
const waitForTokenLockInclusion = async (urls, address, lockHash, options = {}) => {
  return withRetryOrdinal(
    async () => {
      const response = await axios.get(
        `${urls.globalL0Url}/token-locks/${address}?t=${Date.now()}`,
        {
          headers: {
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            Pragma: 'no-cache',
            Expires: '0',
          },
        }
      )
      checkOk(response)
      const lock = response.data.find(l => l.hash === lockHash)
      if (!lock) {
        throw new Error(`Token lock ${lockHash.substring(0, 16)}... not found`)
      }
      return lock
    },
    {
      globalL0Url: urls.globalL0Url,
      name: 'waitForTokenLockInclusion',
      // 8-node reorg-storm budget: txs can sit in event mempool 15-25 ordinals before
      // a non-paused producer drains them. Default 10 was too tight (~one ordinal short
      // in observed runs). Same value as testUpdateDelegatedStake's per-call override.
      maxOrdinalMisses: 40,
      maxStalledChecks: 75,
      interval: 2000,
      ...options
    }
  )
}

/**
 * Get active token locks for an address from GL0.
 * 
 * @param {Object} urls - Network URLs including globalL0Url
 * @param {string} address - Account address
 * @returns {Promise<Array>} - Array of active token lock objects
 */
const getActiveTokenLocks = async (urls, address) => {
  const response = await axios.get(
    `${urls.globalL0Url}/token-locks/${address}?t=${Date.now()}`,
    {
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        Pragma: 'no-cache',
        Expires: '0',
      },
    }
  )
  checkOk(response)
  return response.data || []
}

module.exports = {
  checkOk,
  checkBadRequest,
  dagToDatum,
  getPrivateKeyAndNodeIdFromFile,
  postNodeParamsNodeId,
  createDelegatedStake,
  withdrawDelegatedStake,
  getAccountDelegatedStakes,
  assertAllKeysMatch,
  assertDelegatedStakes,
  fetchStakeWithRewardsBalance,
  createTokenLock,
  assertBalanceChange,
  getNodeParams,
  fetchSnapshot,
  assertRewardTxnInSnapshot,
  assertTokenUnlockInSnapshot,
  // Ordinal-aware helpers
  waitForStakeInclusion,
  waitForStakeWithdrawal,
  waitForTokenLockInclusion,
  getActiveTokenLocks,
}
