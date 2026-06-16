const {dag4} = require('@stardust-collective/dag4');
const jsSha256 = require('js-sha256');
const axios = require('axios');
const { z } = require('zod');
// Use the SDK's own serializeBrotli (brotli-wasm-backed, key-sorted + null-dropped to match the
// node's circe Printer(sortKeys=true, dropNullValues=true) + brotli4j-q2 signing preimage). The
// standalone `brotli` npm pkg (Emscripten build) crashed the process via an uncaught exception
// (exit 7) AND did not normalize keys, so it never matched the node preimage. The SDK helper is the
// same one the (passing) currency-tx path signs with, so the node accepts these bytes.
const { serializeBrotli } = require('@stardust-collective/dag4-keystore');
const {parseSharedArgs, withRetry} = require('../shared');
const { pollWithEventKick } = require('../lib/awaitChainEvent');

const CliArgsSchema = z.object({
    privateKey: z.string()
        .min(1, "Private key cannot be empty"),
});

const createConfig = () => {
    const args = process.argv.slice(2);

    if (args.length < 6) {
        throw new Error(
            "Usage: node script.js <dagl0-port-prefix> <dagl1-port-prefix> <ml0-port-prefix> <cl1-port-prefix> <datal1-port-prefix> <private-key>"
        );
    }

    const sharedArgs = parseSharedArgs(args.slice(0, 5));
    const [privateKey] = args.slice(5);

    const specificArgs = CliArgsSchema.parse({ privateKey });

    return { ...sharedArgs, ...specificArgs };
};

const sleep = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms))
}

const getEncoded = (value) => {
    const energyValue = JSON.stringify(value);
    return energyValue;
};

const serialize = (msg) => {
    const coded = Buffer.from(msg, 'utf8').toString('hex');
    return coded;
};

const generateProof = async (message, walletPrivateKey, account) => {
    const encoded = getEncoded(message);
    const serializedTx = serialize(encoded);
    const hash = jsSha256.sha256(Buffer.from(serializedTx, 'hex'));
    const signature = await dag4.keyStore.sign(walletPrivateKey, hash);

    const publicKey = account.publicKey;
    const uncompressedPublicKey =
        publicKey.length === 128 ? '04' + publicKey : publicKey;

    return {
        id: uncompressedPublicKey.substring(2),
        signature
    };
};

const generateProofFee = async (message, privateKey, account) => {
    // serializeBrotli returns a Uint8Array of the brotli4j-q2-compatible, key-sorted/null-dropped
    // JSON bytes — the exact preimage the node hashes via Hash.fromBytes (sha256 hex of the raw
    // bytes). Hash those raw bytes directly (no hex round-trip).
    const serializedTx = await serializeBrotli(message);
    const messageHash = jsSha256.sha256(Buffer.from(serializedTx));
    const signature = await dag4.keyStore.sign(privateKey, messageHash);

    const publicKey = account.publicKey;
    const uncompressedPublicKey =
        publicKey.length === 128 ? '04' + publicKey : publicKey;

    return {
        id: uncompressedPublicKey.substring(2),
        signature
    };
};

const getEstimateFeeResponse = async (metagraphL1DataUrl, update) => {
    // Retry for cluster warmup — DL1 returns 500 until it receives its first
    // currency snapshot from ML0 ("Cannot start data own consensus: No currency snapshot").
    const estimateFeeResponse = await withRetry(
        () => axios.post(`${metagraphL1DataUrl}/data/estimate-fee`, update),
        { name: 'POST /data/estimate-fee', maxAttempts: 60, interval: 2000 }
    );
    const {fee, address, updateHash} = estimateFeeResponse.data
    return {
        fee,
        address,
        updateHash
    }
}

const sendDataTransactionsUsingUrls = async (
    globalL0Url,
    metagraphL1DataUrl,
    privateKey
) => {
    const account = dag4.createAccount(privateKey);

    account.connect({
        networkVersion: '2.0',
        l0Url: globalL0Url,
        testnet: true
    });

    const dataUpdate = {
        UsageUpdateWithFee: {
            address: account.address,
            usage: 10
        }
    }
    const dataUpdateProof = await generateProof(dataUpdate, privateKey, account);

    const estimateFeeResponse = await getEstimateFeeResponse(metagraphL1DataUrl, dataUpdate)
    const feeTransaction = {
        amount: estimateFeeResponse.fee,
        dataUpdateRef: estimateFeeResponse.updateHash,
        destination: estimateFeeResponse.address,
        source: account.address
    }
    const feeTransactionProof = await generateProofFee(feeTransaction, privateKey, account);

    const body = {
        data: {
            value: dataUpdate,
            proofs: [
                dataUpdateProof
            ]
        },
        fee: {
            value: feeTransaction,
            proofs: [
                feeTransactionProof
            ]
        }
    };
    console.log(`Transaction body: ${JSON.stringify(body)}`);
    // Retry on DL1 warmup (see getEstimateFeeResponse above). The old try/catch
    // swallowed the error, causing the downstream poll to 404 forever because the
    // tx was never actually sent.
    const response = await withRetry(
        () => axios.post(`${metagraphL1DataUrl}/data`, body),
        { name: 'POST /data', maxAttempts: 60, interval: 2000 }
    );
    console.log(`Response: ${JSON.stringify(response.data)}`);

    return [account.address, estimateFeeResponse];
};

// Reactive (event-kicked) wait: subscribe to gl0's LocalEvents stream and re-run the
// metagraph-L0 REST check on every cluster-progress tick (SNAPSHOT_FINALIZED /
// METAGRAPH_SNAPSHOT_ACCEPTED) instead of a fixed 120×1s wall-clock poll. Robust to ml0
// recovery latency — it wakes on actual cluster progress, with a generous 30min safety
// timeout that dumps the last 10 events on failure. Mirrors the token-locks migration.
const checkDataTransactionInMetagraphL0 = async (metagraphL0Url, address) => {
    await pollWithEventKick({
        endpoint: process.env.LOCAL_EVENTS_ENDPOINT,
        maxWait: '30min',
        tag: `dataTxInMl0:${address.slice(0, 12)}`,
        checkFn: async () => {
            const response = await axios.get(`${metagraphL0Url}/data-application/addresses/${address}`);
            const responseData = response.data;
            if (Object.keys(responseData).length > 0) {
                console.log(`Data transaction processed successfully. Response: ${JSON.stringify(responseData)}`);
                return responseData;
            }
            throw new Error('data-application state not updated yet');
        }
    });
}

const checkFeeTransactionInGlobalL0 = async (globalL0Url, feeWallet) => {
    // Multi-metagraph aware: look up ONLY the target metagraph we sent the data update to.
    // The project template's hardcoded fee address is identical across all metagraphs, so
    // iterating all of them could false-pass on another metagraph's fee-wallet balance.
    // The test always submits to metagraph k=0, whose identifier is exported as METAGRAPH_ID
    // by compose-runner.sh.
    const targetMetagraphId = process.env.METAGRAPH_ID;
    if (!targetMetagraphId) {
        throw new Error('METAGRAPH_ID env var not set — cannot identify target metagraph');
    }
    // Reactive event-kicked wait (see checkDataTransactionInMetagraphL0).
    await pollWithEventKick({
        endpoint: process.env.LOCAL_EVENTS_ENDPOINT,
        maxWait: '30min',
        tag: `feeTxInGl0:${feeWallet.slice(0, 12)}`,
        checkFn: async () => {
            // Roots-only sharding keeps per-metagraph (CL1) currency balances in gl0's MPT (`MgBalances`), NOT in the
            // `lastCurrencySnapshots` blob (empty in the combined view — `info` is no longer the source of truth). Read the
            // metagraph-token balance gl0 mirrors via the dedicated CL1 balance route (gl0 commits to it via `perMetagraphMptRoot`;
            // the verifiable inclusion proof is available via the shard-proof route).
            const response = await axios.get(`${globalL0Url}/currency/${targetMetagraphId}/balance/${feeWallet}`);
            const balance = response.data && response.data.balance;
            if (balance && balance > 0) {
                console.log(`Fee transaction reflected in gl0 for metagraph ${targetMetagraphId}: ${feeWallet} balance=${balance}`);
                return balance;
            }
            throw new Error('fee transaction not yet reflected in gl0 currency balance');
        }
    });
}


const sendDataTransaction = async () => {
    const {dagL0PortPrefix, metagraphL0PortPrefix, dataL1PortPrefix, privateKey} = createConfig()

    const host = process.env.TEST_HOST || 'http://localhost';
    const globalL0Url = process.env.GL0_URL || `${host}:${dagL0PortPrefix}00`;
    const metagraphL0Url = process.env.ML0_URL || `${host}:${metagraphL0PortPrefix}00`;
    const metagraphL1DataUrl = process.env.DL1_URL || `${host}:${dataL1PortPrefix}00`;

    const [address, estimateFeeResponse] = await sendDataTransactionsUsingUrls(globalL0Url, metagraphL1DataUrl, privateKey);

    await checkDataTransactionInMetagraphL0(metagraphL0Url, address);
    await checkFeeTransactionInGlobalL0(globalL0Url, estimateFeeResponse.address);
};

sendDataTransaction();