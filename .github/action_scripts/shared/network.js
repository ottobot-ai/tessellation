const { dag4 } = require('@stardust-collective/dag4');
const axios = require('axios');
const { withRetry } = require("./operations");

const createNetworkConfig = (args) => {
    const { dagL0PortPrefix, dagL1PortPrefix, metagraphL0PortPrefix, currencyL1PortPrefix, dataL1PortPrefix } = args;
    const host = process.env.TEST_HOST || 'http://localhost';

    return {
        globalL0Url: process.env.GL0_URL || `${host}:${dagL0PortPrefix}00`,
        dagL1Url: process.env.GL1_URL || `${host}:${dagL1PortPrefix}00`,
        currencyL0Url: process.env.ML0_URL || `${host}:${metagraphL0PortPrefix}00`,
        currencyL1Url: process.env.CL1_URL || `${host}:${currencyL1PortPrefix}00`,
        dataL1Url: process.env.DL1_URL || `${host}:${dataL1PortPrefix}00`,
        extendedDagL1Url: process.env.EXT_GL1_URL || `${host}:${dagL1PortPrefix}50`,
        extendedDataL1Url: process.env.EXT_DL1_URL || `${host}:${dataL1PortPrefix}50`
    };
};

// Per-metagraph URL helper. K metagraphs share the same gl0/gl1 hypergraph
// but each has its own ml0/cl1/dl1 cluster at port-prefix shifted -10 per k
// (m0=92/93/94, m1=82/83/84, ...). Mirrors the bash logic in
// docker-env-setup.sh that allocates host ports per metagraph.
//
// Args: metagraphIdx (k in [0, NUM_METAGRAPHS)).
// Returns: { metagraphId, metagraphL0Url, currencyL1Url, dataL1Url }.
const getMetagraphUrls = (metagraphIdx) => {
    const k = parseInt(metagraphIdx, 10);
    const host = process.env.TEST_HOST || 'http://localhost';

    // METAGRAPH_IDS_CSV is exported by compose-runner.sh after metagraph
    // genesis; falls back to METAGRAPH_ID for k=0 single-metagraph compat.
    const idsCsv = process.env.METAGRAPH_IDS_CSV || '';
    const ids = idsCsv ? idsCsv.split(',') : [];
    const metagraphId = ids[k] || (k === 0 ? process.env.METAGRAPH_ID : null);
    if (!metagraphId) {
        throw new Error(`No metagraph ID found for k=${k} (METAGRAPH_IDS_CSV="${idsCsv}", METAGRAPH_ID="${process.env.METAGRAPH_ID}")`);
    }

    // Per-k port prefix shift, matching docker-env-setup.sh:
    // M_ML0_PORT_PREFIX = ML0_PORT_PREFIX - k*10
    const ml0PortPrefix = parseInt(process.env.ML0_PORT_PREFIX || '92', 10) - k * 10;
    const cl1PortPrefix = parseInt(process.env.CL1_PORT_PREFIX || '93', 10) - k * 10;
    const dl1PortPrefix = parseInt(process.env.DL1_PORT_PREFIX || '94', 10) - k * 10;

    return {
        metagraphId,
        metagraphL0Url: `${host}:${ml0PortPrefix}00`,
        currencyL1Url: `${host}:${cl1PortPrefix}00`,
        dataL1Url: `${host}:${dl1PortPrefix}00`
    };
};

const createAndConnectAccount = (privateKey, networkConfig) => {
    const account = dag4.createAccount(privateKey);
    account.connect({
        networkVersion: '2.0',
        ...networkConfig
    });

    return account;
};

const checkIfEpochProgressCanBeFetched = async (snapshotUrl, isCurrency) => {
    await withRetry(
        async () => {
            const { data: snapshot } = await axios.get(snapshotUrl);
            const epochProgress = isCurrency ?
                snapshot.value.globalSyncView.epochProgress :
                snapshot.value.epochProgress;

            if (!epochProgress) {
                throw new Error("EpochProgress still in sync process")
            }
        },
        { name: `Get epoch progress` }
    );
}

const getEpochProgress = async (l0Url, isCurrency = false) => {
    const snapshotUrl = isCurrency
        ? `${l0Url}/snapshots/latest`
        : `${l0Url}/global-snapshots/latest`;

    await checkIfEpochProgressCanBeFetched(snapshotUrl, isCurrency)

    const { data: snapshot } = await axios.get(snapshotUrl);
    return isCurrency ?
        snapshot.value.globalSyncView.epochProgress :
        snapshot.value.epochProgress;
}

module.exports = {
    createNetworkConfig,
    createAndConnectAccount,
    getEpochProgress,
    getMetagraphUrls
}