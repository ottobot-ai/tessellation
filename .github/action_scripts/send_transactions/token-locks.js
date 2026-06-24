const { dag4 } = require('@stardust-collective/dag4');
const jsSha256 = require('js-sha256');
const axios = require('axios');
const {
    parseSharedArgs,
    CONSTANTS: sharedConstants,
    PRIVATE_KEYS,
    generateProof,
    sleep,
    withRetry,
    withRetryOrdinal,
    createAndConnectAccount,
    createNetworkConfig,
    getEpochProgress,
    getCombinedSnapshot,
    SerializerType,
    createSerializer
} = require('../shared');

const CONSTANTS = {
    ...sharedConstants,
    // Increased from 5 to 30 for Nakamoto mode. The propagation pipeline
    // (CL1 HTTP POST → CL1 block → ML0 snapshot → GL0 state channel →
    // finalization → ML0 activeTokenLocks) takes ~45-65s, and each epoch
    // advances ~10s. 5 epochs (~50s) is too short — the lock expires before
    // the test can observe it in ML0's activeTokenLocks.
    // 2026-05-13: 30 → 20 + 50 → 30 after #125. With overlay+G0/G1/G2/GC the
    // pipeline observation peak holds steady; expiration test gets 10-epoch margin
    // (~100s at ~10s/epoch), unlock test keeps 20-epoch headroom for the extra
    // DAG-balance-restoration pipeline stage.
    // 2026-05-19 (#217 follow-up): 20 → 200 for 8gl0+4mg+4shards scale. Direct
    // observation (iter-198std-v21): epoch advanced 213 → 920+ during the
    // ~50min `verifyBalance` polling window — chain-link backlog at 4-mg scale
    // makes gl0 cadence ~13s/snapshot under load, and the gl0 `/latest/combined`
    // endpoint had multi-minute serving delays during cluster bootstrap. By the
    // time `verifyGlobalL0` succeeded and `verifyBalance` started polling, the
    // 20-epoch buffer had long expired and CL0 had already restored the balance
    // (activeTokenLocks: {}, balances[source] = initial). The test was looking
    // for `initial - 100` but the lock had already cycled deduct → expire →
    // refund. 200 keeps the lock active well past the verify window at 8mg
    // scale while still firing within the subsequent `verifyTokenLockExpiration`
    // step's 50-min polling budget. The deeper fix is the cluster slowdown
    // itself (#214 chain-link drain at scale), tracked separately.
    EPOCH_PROGRESS_BUFFER_EXPIRATION_TEST: 200,
    // 2026-05-19 (#217 follow-up): 30 → 200 to match expiration test buffer
    // bump. Same root cause: at 8gl0+4mg+4shards scale the chain-link backlog
    // makes verifyBalance polling outrun the lock lifetime.
    EPOCH_PROGRESS_BUFFER_TOKEN_UNLOCK_TEST: 200
};

const createConfig = () => {
    const args = process.argv.slice(2);

    if (args.length < 5) {
        throw new Error(
            "Usage: node script.js <dagl0-port-prefix> <dagl1-port-prefix> <ml0-port-prefix> <cl1-port-prefix> <datal1-port-prefix>"
        );
    }

    const sharedArgs = parseSharedArgs(args.slice(0, 5));

    return { ...sharedArgs };
};

const createTokenLockTransaction = async (sourceAccount, l1Url, l0Url, epochProgressOffset) => {
    const [{ data: lastRef }, currentEpochProgress] = await Promise.all([
        axios.get(`${l1Url}/token-locks/last-reference/${sourceAccount.address}`),
        getEpochProgress(l0Url, true)
    ]);

    console.log(`Current epoch progress: ${currentEpochProgress}, setting unlockEpoch to ${currentEpochProgress + epochProgressOffset}`);

    return {
        amount: 100,
        currencyId: CONSTANTS.CURRENCY_TOKEN_ID,
        fee: 0,
        parent: lastRef,
        source: sourceAccount.address,
        unlockEpoch: currentEpochProgress + epochProgressOffset,
    };
};

const createUpdateWithTokenUnlockTransaction = async (sourceAccount, tokenLockHash) => {
    return {
        UsageUpdateWithTokenUnlock: {
            address: sourceAccount.address,
            currencyId: CONSTANTS.CURRENCY_TOKEN_ID,
            tokenLockRef: tokenLockHash,
            unlockAmount: 100,
            usage: 10
        }
    }
};

// Module-scope helper so it can be called from outside createVerifier
// (e.g., verifyTokenLockExpiration at the end of the file).
const findMatchingHash = async (tokenLocks, targetHash) => {
    return tokenLocks.reduce(async (acc, tokenLock) => {
        const prevResult = await acc;
        if (prevResult) return true;

        const serializer = createSerializer(SerializerType.BROTLI);
        const message = await serializer.serialize(tokenLock.value);
        const tokenLockHash = jsSha256.sha256(Buffer.from(message, 'hex'));
        return tokenLockHash === targetHash;
    }, Promise.resolve(false));
};

const createVerifier = (urls) => {

    const verifyInL1 = async (hash, l1Url, layerName) => {
        await withRetry(
            async () => {
                const response = await axios.get(`${l1Url}/token-locks/${hash}`);
                if (!response.data) {
                    throw new Error('Transaction not found');
                }
                console.log(`TokenLock transaction processed successfully in ${layerName} L1`);
            },
            { name: `${layerName} L1 verification` }
        );
    };

    const verifyTokenLockInSnapshot = async (address, hash, snapshot, tokenId, layerName, isCurrency = false) => {
        const activeTokenLocksForAddress = isCurrency
            ? snapshot[1]?.activeTokenLocks?.[address]
            : snapshot[1]?.lastCurrencySnapshots?.[tokenId]?.Right[1].activeTokenLocks?.[address];

        if (!activeTokenLocksForAddress) {
            throw new Error(`No active token locks found for address ${address} in ${layerName}`);
        }

        const hasMatchingHash = await findMatchingHash(activeTokenLocksForAddress, hash);
        if (!hasMatchingHash) {
            throw new Error(`Token lock with hash ${hash} not found in ${layerName}`);
        }

        console.log(`TokenLock transaction found in ${layerName}`);
    };

    const verifyInL0 = async (address, hash, l0Url, tokenId, layerName, isCurrency = false) => {
        await withRetry(
            async () => {
                const snapshot = await getCombinedSnapshot(
                    `${l0Url}/global-snapshots/latest/combined`
                );
                await verifyTokenLockInSnapshot(address, hash, snapshot, tokenId, `${layerName} L0`, isCurrency);
            },
            { name: `${layerName} L0 verification` }
        );
    };

    const verifyInCurrencyL0 = async (address, hash) => {
        await withRetry(
            async () => {
                const snapshot = await getCombinedSnapshot(
                    `${urls.currencyL0Url}/snapshots/latest/combined`
                );
                await verifyTokenLockInSnapshot(
                    address,
                    hash,
                    snapshot,
                    CONSTANTS.CURRENCY_TOKEN_ID,
                    'Currency L0',
                    true
                );
            },
            { name: 'Currency L0 verification' }
        );
    };

    return {
        verifyCurrencyL1: (hash) => verifyInL1(hash, urls.currencyL1Url, 'Currency'),
        verifyCurrencyL0: (address, hash) => verifyInCurrencyL0(address, hash),
        verifyGlobalL0ForCurrency: (address, hash) =>
            verifyInL0(address, hash, urls.globalL0Url, CONSTANTS.CURRENCY_TOKEN_ID, 'Global', false)
    };
};

const createBalanceManager = (urls) => {
    const getBalance = async (privateKey, l0Url) => {
        const account = dag4.createAccount(privateKey);
        const address = account.address;

        try {
            const snapshotUrl = `${l0Url}/snapshots/latest/combined`

            const snapshot = await getCombinedSnapshot(snapshotUrl);

            const balance = snapshot[1]?.balances?.[address] || 0;

            return balance;
        } catch (error) {
            console.error(`Error fetching balance from ${l0Url}:`, error.message);
            throw error;
        }
    };

    const verifyBalanceChange = async (privateKey, initialBalance, amount, l0Url, layerName) => {
        await withRetry(
            async () => {
                const currentBalance = await getBalance(privateKey, l0Url);
                const expectedBalance = initialBalance - amount;

                if (currentBalance !== expectedBalance) {
                    throw new Error(
                        `Balance mismatch. Expected: ${expectedBalance}, got: ${currentBalance}`
                    );
                }
                console.log(`${layerName} balance verified successfully`);
            },
            { name: `${layerName} balance verification` }
        );
    };

    return {
        getCurrencyBalance: (privateKey) =>
            getBalance(privateKey, urls.currencyL0Url),
        verifyCurrencyBalanceChange: (privateKey, initialBalance, amount) =>
            verifyBalanceChange(
                privateKey,
                initialBalance,
                amount,
                urls.currencyL0Url,
                "Currency"
            )
    };
};

const createTokenLockTransactionHandler = (urls) => {
    const submitTransaction = async (tokenLock, proof, l1Url) => {
        try {
            const body = { value: tokenLock, proofs: [proof] };
            console.log(`TokenLockBody: ${JSON.stringify(body)}`)
            console.log(`Url: ${l1Url}/token-locks`)
            return await axios.post(`${l1Url}/token-locks`, body);
        } catch (error) {
            console.error('Error sending TokenLock transaction');
            throw error;
        }
    };

    const sendTransaction = async (sourcePrivateKey, l0Url, l1Url, epochProgressOffset) => {
        const sourceAccount = createAndConnectAccount(sourcePrivateKey, { l0Url, l1Url });

        const tokenLock = await createTokenLockTransaction(sourceAccount, l1Url, l0Url, epochProgressOffset);
        const proof = await generateProof(tokenLock, sourcePrivateKey, sourceAccount, SerializerType.BROTLI);

        const response = await submitTransaction(tokenLock, proof, l1Url);

        return {
            address: sourceAccount.address,
            hash: response.data.hash,
            amount: tokenLock.amount,
            unlockEpoch: tokenLock.unlockEpoch
        };
    };

    return {
        sendCurrencyTransaction: (sourcePrivateKey, epochProgressOffset) =>
            sendTransaction(sourcePrivateKey, urls.currencyL0Url, urls.currencyL1Url, epochProgressOffset)
    };
};

const createDataUpdateTransactionHandler = (urls) => {
    const submitTransaction = async (dataUpdateWithTokenUnlock, proof, dataL1Url) => {
        try {
            const body = { value: dataUpdateWithTokenUnlock, proofs: [proof] };
            return await axios.post(`${dataL1Url}/data`, body);
        } catch (error) {
            console.error('Error sending DataUpdate transaction');
            throw error;
        }
    };

    const sendTransaction = async (sourcePrivateKey, l0Url, l1Url, dataL1Url, tokenLockHash) => {
        const sourceAccount = createAndConnectAccount(sourcePrivateKey, { l0Url, l1Url });

        const dataUpdateWithTokenUnlock = await createUpdateWithTokenUnlockTransaction(sourceAccount, tokenLockHash);
        const proof = await generateProof(dataUpdateWithTokenUnlock, sourcePrivateKey, sourceAccount, SerializerType.STANDARD);

        await submitTransaction(dataUpdateWithTokenUnlock, proof, dataL1Url);
    };

    return {
        sendDataUpdateTransaction: (sourcePrivateKey, tokenLockHash) =>
            sendTransaction(sourcePrivateKey, urls.currencyL0Url, urls.currencyL1Url, urls.dataL1Url, tokenLockHash)
    };
};

const verifyTokenLockExpiration = async (address, hash, initialBalance, urls, unlockEpoch) => {
    const l0Url = urls.currencyL0Url;
    const snapshotUrl = `${l0Url}/snapshots/latest/combined`;

    // Replaces the previous two-stage wall-clock polling (interval = 10s × 600 attempts
    // each) with ordinal-aware retry (withRetryOrdinal). Checks only advance when the
    // global L0 ordinal progresses, so network stalls never burn budget.
    //
    // The combined check:
    //   (a) epoch progress on currency L0 has advanced past unlockEpoch
    //   (b) the lock has been evicted from activeTokenLocks (or is no longer matchable)
    //   (c) the source balance has been reverted to `initialBalance`
    //
    // The original code asserted (c) at the end as a non-retried failure; we KEEP that
    // semantic by failing the predicate at (c)-success rather than retry, so a balance
    // mismatch surfaces as the structured ordinal-exhaustion error if the cluster never
    // reaches the expected state.
    //
    // EPOCH-GATED wait: epochProgress advances ~1:1 with the gl0 ordinal, so the wait
    // legitimately spans (unlockEpoch − currentEpochProgress) ordinals. Giving it the
    // same maxOrdinalMisses=40 as a TX-SETTLE wait means it always times out long before
    // epochProgress reaches unlockEpoch. Instead, read the current epochProgress once
    // up-front and compute a dynamic miss budget: cover the epoch delta + 100 ordinals
    // of margin, with a floor of 60 to handle the case where unlockEpoch is already
    // nearly reached when we start.
    const currentEpochProgressUpfront = await getEpochProgress(l0Url, true);
    const epochDeltaMisses = Math.max(60, (unlockEpoch - currentEpochProgressUpfront) + 100);
    console.log(
        `verifyTokenLockExpiration: currentEpochProgress=${currentEpochProgressUpfront}, ` +
        `unlockEpoch=${unlockEpoch}, maxOrdinalMisses=${epochDeltaMisses}`
    );
    const snapshot = await withRetryOrdinal(
        async () => {
            const currentEpochProgress = await getEpochProgress(l0Url, true);
            if (currentEpochProgress <= unlockEpoch) {
                throw new Error(`Current epoch progress (${currentEpochProgress}) has not passed unlockEpoch (${unlockEpoch})`);
            }
            const snap = await getCombinedSnapshot(snapshotUrl);
            const activeTokenLocks = snap[1]?.activeTokenLocks?.[address];
            if (activeTokenLocks && activeTokenLocks.length > 0) {
                const hasMatchingHash = await findMatchingHash(activeTokenLocks, hash);
                if (hasMatchingHash) {
                    throw new Error('Token lock still active after expiration');
                }
            }
            return snap;
        },
        {
            globalL0Url: urls.globalL0Url,
            name: `tokenLockExpiration:${address.slice(0, 12)}`,
            maxOrdinalMisses: epochDeltaMisses,
            maxStalledChecks: 120,
            interval: 3000,
            // This wait legitimately spans ~(unlockEpoch − current) epochs (EPOCH_PROGRESS_BUFFER_EXPIRATION_TEST,
            // ~27min at a healthy 2mg cadence) — well over withRetryOrdinal's 15min default ceiling. Give it a
            // generous 60min ceiling so a healthy-but-slow run isn't false-failed, while still capping a finality
            // stall (or torn-down cluster) here at minutes, not the multi-hour hang we hit before.
            maxWallClockMs: 3600000,
        }
    );

    const currentBalance = snapshot[1]?.balances?.[address] || 0;
    const expectedBalance = initialBalance;

    if (currentBalance !== expectedBalance) {
        throw new Error(
            `Balance not reverted correctly after expiration. Expected: ${expectedBalance} (initial), got: ${currentBalance}`
        );
    }

    console.log(`Token lock expired and balance reverted successfully in Currency`);
};

const verifyTriggerTokenUnlock = async (address, initialBalance, urls) => {
    const l0Url = urls.currencyL0Url;
    const snapshotUrl = `${l0Url}/snapshots/latest/combined`;

    // Replaces the previous wall-clock-only `withRetry` (600 attempts × 10s = 100 min)
    // with ordinal-aware retry (withRetryOrdinal). Checks advance when the global L0
    // ordinal progresses; network stalls don't burn the miss budget.
    const snapshot = await withRetryOrdinal(
        async () => {
            const snap = await getCombinedSnapshot(snapshotUrl);
            const activeTokenLocks = snap[1]?.activeTokenLocks?.[address];
            if (activeTokenLocks && Object.keys(activeTokenLocks).length > 0) {
                throw new Error(`TokenLock still active`);
            }
            return snap;
        },
        {
            globalL0Url: urls.globalL0Url,
            name: `tokenUnlockTrigger:${address.slice(0, 12)}`,
            maxOrdinalMisses: 40,
            maxStalledChecks: 120,
            interval: 3000,
        }
    );

    const currentBalance = snapshot[1]?.balances?.[address] || 0;
    const expectedBalance = initialBalance;

    if (currentBalance !== expectedBalance) {
        throw new Error(
            `Balance not reverted correctly after triggering token unlcok. Expected: ${expectedBalance} (initial), got: ${currentBalance}`
        );
    }

    console.log(`Token lock expired and balance reverted successfully in Currency`);
};

const executeWorkflow = async ({
   urls,
   workflowName,
   getInitialBalance,
   sendTransaction,
   verifyL1,
   verifyL0,
   verifyGlobalL0,
   verifyBalance
}, privateKey, epochProgressOffset) => {
    const balanceManager = createBalanceManager(urls);
    const transactionHandler = createTokenLockTransactionHandler(urls);
    const verifier = createVerifier(urls);

    console.log(`Starting ${workflowName} workflow`);

    const initialBalance = await getInitialBalance(balanceManager, privateKey);

    const { address, hash, amount, unlockEpoch } = await sendTransaction(transactionHandler, privateKey, epochProgressOffset);

    console.log(`Token lock transaction sent successfully with unlockEpoch: ${unlockEpoch}`);

    await verifyL1(verifier, hash);
    await sleep(CONSTANTS.SNAPSHOT_WAIT_TIME_MS);
    await verifyL0(verifier, address, hash);

    if (verifyGlobalL0) {
        await verifyGlobalL0(verifier, address, hash);
    }

    await verifyBalance(privateKey, balanceManager, initialBalance, amount);

    return {
        address,
        hash,
        initialBalance,
        unlockEpoch
    }
}

const currencyWorkflow = {
    workflowName: 'Currency',
    getInitialBalance: (balanceManager, privateKey) =>
        balanceManager.getCurrencyBalance(privateKey),
    sendTransaction: (handler, privateKey, epochProgressOffset) =>
        handler.sendCurrencyTransaction(privateKey, epochProgressOffset),
    sendDataUpdate: (handler, privateKey) =>
        handler.sendDataUpdateTransaction(privateKey),
    verifyL1: (verifier, hash) =>
        verifier.verifyCurrencyL1(hash),
    verifyL0: (verifier, address, hash) =>
        verifier.verifyCurrencyL0(address, hash),
    verifyGlobalL0: (verifier, address, hash) =>
        verifier.verifyGlobalL0ForCurrency(address, hash),
    verifyBalance: (privateKey, balanceManager, initialBalance, amount) =>
        balanceManager.verifyCurrencyBalanceChange(privateKey, initialBalance, amount)
};

const validateTokenLockExpiration = async (urls) => {
    console.log(`Starting to test TokenLock expiration`)
    const { address, hash, initialBalance, unlockEpoch } = await executeWorkflow({ urls, ...currencyWorkflow }, PRIVATE_KEYS.key1, CONSTANTS.EPOCH_PROGRESS_BUFFER_EXPIRATION_TEST);
    console.log(`Workflow completed successfully, waiting for expiration...`);

    await verifyTokenLockExpiration(
        address,
        hash,
        initialBalance,
        urls,
        unlockEpoch,
    );
    console.log(`TokenLock expiration tested successfully`)
}

const validateTriggerTokenUnlock = async (urls) => {
    console.log(`Starting to test TokenLock triggering unlock`)
    const { address, hash, initialBalance } = await executeWorkflow({ urls, ...currencyWorkflow }, PRIVATE_KEYS.key2, CONSTANTS.EPOCH_PROGRESS_BUFFER_TOKEN_UNLOCK_TEST);

    const dataUpdateTransactionHandler = await createDataUpdateTransactionHandler(urls)
    await dataUpdateTransactionHandler.sendDataUpdateTransaction(PRIVATE_KEYS.key2, hash)

    await verifyTriggerTokenUnlock(
        address,
        initialBalance,
        urls
    )
    console.log(`TokenLock manual token unlock tested successfully`)
}

const executeAllWorkflows = async () => {
    const config = createConfig();
    const urls = createNetworkConfig(config);

    await validateTokenLockExpiration(urls)
    await validateTriggerTokenUnlock(urls)
};

executeAllWorkflows();