const http = require('http')
const { dag4 } = require('@stardust-collective/dag4')
const { parseSharedArgs, logWorkflow } = require('../shared')

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
  const deadline = pollStart + timeoutMs
  const pollInterval = 5000
  while (Date.now() < deadline) {
    await sleep(pollInterval)
    try {
      const ref = await fetchJson(`${l1MetagraphUrl}/transactions/last-reference/${address}`)
      if (ref.hash !== beforeHash) {
        logMessage(`CL1 aligned after ${Math.round((Date.now() - pollStart) / 1000)}s`)
        return
      }
    } catch (e) {
      logMessage(`CL1 alignment poll error: ${e.message}`)
    }
  }
  logMessage(`CL1 alignment timeout — proceeding anyway`)
}

const batchTransaction = async (
  origin,
  destination,
  amount = 10,
  fee = 1,
  num = 100,
) => {
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

    const generatedTransactions = await origin.generateBatchTransactions(
      txnsData,
    )

    const hashes = await origin.sendBatchTransactions(generatedTransactions)

    logMessage(
      `DAG transaction from: ${origin.address} sent - batch of ${num}.`,
    )

    return hashes
  } catch (e) {
    throw Error(`Error when sending batch transaction: ${e}`)
  }
}

const batchMetagraphTransaction = async (
  metagraphTokenClient,
  origin,
  destination,
  amount = 10,
  fee = 1,
  num = 100,
) => {
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

    const generatedTransactions = await metagraphTokenClient.generateBatchTransactions(
      txnsData,
    )

    const hashes = await metagraphTokenClient.sendBatchTransactions(
      generatedTransactions,
    )

    logMessage(
      `L0 token transaction from: ${origin.address} sent - batch of ${num}.`,
    )

    return hashes
  } catch (e) {
    throw Error(`Error when sending batch transaction: ${e}`)
  }
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
    await batchTransaction(origin, destination, amount, fee, txnCount)

    // Poll for expected balances with timeout. In Nakamoto consensus, fork convergence
    // adds latency — the balance endpoint reads snapshotStorage.head which may lag.
    const expectedOriginDelta = -(amount + fee) * txnCount
    const expectedDestDelta = amount * txnCount
    const startOriginBalance = await origin.getBalance()
    const startDestBalance = await destination.getBalance()
    const expectedOriginBalance = startOriginBalance + expectedOriginDelta
    const expectedDestBalance = startDestBalance + expectedDestDelta

    logMessage(`Polling for balance change (timeout ${SLEEP_TIME_UNTIL_QUERY}ms)...`)
    const pollInterval = 5000
    const deadline = Date.now() + SLEEP_TIME_UNTIL_QUERY
    let originBalance, destinationBalance
    while (Date.now() < deadline) {
      await sleep(pollInterval)
      originBalance = await origin.getBalance()
      destinationBalance = await destination.getBalance()
      if (originBalance !== startOriginBalance || destinationBalance !== startDestBalance) {
        logMessage(`Balance changed after ${Math.round((Date.now() - (deadline - SLEEP_TIME_UNTIL_QUERY)) / 1000)}s`)
        break
      }
    }
    if (originBalance === startOriginBalance && destinationBalance === startDestBalance) {
      logMessage(`Balance unchanged after ${SLEEP_TIME_UNTIL_QUERY / 1000}s — falling back to final check`)
      originBalance = await origin.getBalance()
      destinationBalance = await destination.getBalance()
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

    // Capture destination's CL1 last-reference before sending. The destination's
    // ref only changes when CL1 processes the snapshot (not on local block
    // acceptance), so it's the correct signal for snapshot round-trip completion.
    const destRefBefore = await fetchJson(
      `${networkOptions.l1MetagraphUrl}/transactions/last-reference/${destination.address}`
    )

    await batchMetagraphTransaction(
      metagraphTokenClient,
      origin,
      destination,
      amount,
      fee,
      txnCount,
    )

    const startOriginBalance = await metagraphTokenClient.getBalance()
    const startDestBalance = await metagraphTokenClient.getBalanceFor(destination.address)

    logMessage(`Polling for L0 token balance change (timeout ${SLEEP_TIME_UNTIL_QUERY}ms)...`)
    const pollInterval = 5000
    const deadline = Date.now() + SLEEP_TIME_UNTIL_QUERY
    let originBalance = startOriginBalance, destinationBalance = startDestBalance
    while (Date.now() < deadline) {
      await sleep(pollInterval)
      originBalance = await metagraphTokenClient.getBalance()
      destinationBalance = await metagraphTokenClient.getBalanceFor(destination.address)
      if (originBalance !== startOriginBalance || destinationBalance !== startDestBalance) {
        logMessage(`L0 token balance changed after ${Math.round((Date.now() - (deadline - SLEEP_TIME_UNTIL_QUERY)) / 1000)}s`)
        break
      }
    }

    // Wait for CL1 to process the snapshot containing this transfer before
    // returning. Without this, a subsequent reverse transfer may read stale
    // last-reference state from CL1 and build a transaction whose parent hash
    // disagrees with CL0's lastTxRefs.
    await waitForCL1Alignment(
      networkOptions.l1MetagraphUrl,
      destination.address,
      destRefBefore.hash
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

const transferTest = async (
  fromAccount,
  toAccount,
  amount,
  fee,
  txnCount,
  metagraphOpts,
) => {
  let fromAccountStart, toAccountStart, isMetagraph
  if (metagraphOpts) {
    isMetagraph = true

    const metagraphTokenClient = fromAccount.createMetagraphTokenClient({
      id: metagraphOpts.metagraphId,
      l0Url: metagraphOpts.l0MetagraphUrl,
      l1Url: metagraphOpts.l1MetagraphUrl,
    })

    fromAccountStart = await metagraphTokenClient.getBalance()
    toAccountStart = await metagraphTokenClient.getBalanceFor(toAccount.address)
  } else {
    fromAccountStart = await fromAccount.getBalance()
    toAccountStart = await toAccount.getBalance()
  }

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

  // Metagraph
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

  const networkOptions = {
    metagraphId: 'custom_id',
    l0GlobalUrl: process.env.GL0_URL || `${process.env.TEST_HOST || 'http://localhost'}:${dagL0PortPrefix}00`,
    dagL1UrlFirstNode: process.env.GL1_URL || `${process.env.TEST_HOST || 'http://localhost'}:${dagL1PortPrefix}00`,
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
