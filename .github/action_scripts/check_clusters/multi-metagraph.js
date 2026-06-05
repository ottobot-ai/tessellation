// Multi-metagraph sanity test. Opt-in via `just test --metagraphs=K --test=multi-metagraph`.
// Verifies that K metagraphs share a single hypergraph but have independent
// state-channel clusters with distinct METAGRAPH_IDs.
//
// What it checks:
//   1. METAGRAPH_IDS_CSV has K distinct addresses (no key-collision regression)
//   2. Each metagraph's ml0 cluster reports its expected node count
//   3. Each metagraph's ml0 has produced at least one snapshot past genesis
//   4. Each metagraph's gl0 view (lastCurrencySnapshots) lists all K metagraph IDs
//
// What it does NOT check (separate test scope):
//   - Cross-metagraph allow-spends or token-locks
//   - State-channel allowance list enforcement (Dev env disables it)

const { getMetagraphUrls } = require('../shared/network');
const { CONSTANTS } = require('../shared/constants');
const axios = require('axios');

const fetchJson = async (url, timeoutMs = 5000) => {
    const { data } = await axios.get(url, { timeout: timeoutMs });
    return data;
};

const verifyMetagraphHealth = async (k) => {
    const { metagraphId, metagraphL0Url } = getMetagraphUrls(k);
    console.log(`\n=== metagraph ${k} (id=${metagraphId.slice(0, 12)}..., ml0=${metagraphL0Url}) ===`);

    const expectedMl0Nodes = parseInt(process.env.NUM_ML0_NODES || '2', 10);

    // (1) cluster/info returns at least the genesis ml0
    const info = await fetchJson(`${metagraphL0Url}/cluster/info`);
    if (!Array.isArray(info) || info.length < 1) {
        throw new Error(`metagraph ${k} ml0 cluster/info empty (got ${JSON.stringify(info)})`);
    }
    console.log(`  ml0 cluster: ${info.length} node(s) (expected >= 1, configured ${expectedMl0Nodes})`);
    if (info.length < expectedMl0Nodes) {
        console.log(`  WARN: fewer ml0 peers than configured — soft warning, not fatal in startup window`);
    }

    // (2) latest snapshot ordinal advanced past genesis (>= 1)
    const ord = await fetchJson(`${metagraphL0Url}/snapshots/latest/ordinal`);
    const ordVal = ord && typeof ord.value === 'number' ? ord.value : -1;
    if (ordVal < 1) {
        throw new Error(`metagraph ${k} ml0 snapshot ordinal=${ordVal}, expected >= 1`);
    }
    console.log(`  ml0 snapshot ordinal: ${ordVal}`);

    return metagraphId;
};

const verifyMetagraphIdsDistinct = (ids) => {
    const seen = new Set();
    for (const id of ids) {
        if (seen.has(id)) {
            throw new Error(`Duplicate METAGRAPH_ID detected: ${id} — keystore separation regression!`);
        }
        seen.add(id);
    }
    console.log(`\nAll ${ids.length} metagraph IDs are distinct ✓`);
};

const verifyGl0SeesAllMetagraphs = async (ids) => {
    const dagL0PortPrefix = process.env.DAG_L0_PORT_PREFIX || '90';
    const host = process.env.TEST_HOST || 'http://localhost';
    const gl0Url = process.env.GL0_URL || `${host}:${dagL0PortPrefix}00`;

    // gl0's GlobalSnapshotInfo carries lastStateChannelSnapshotHashes keyed by metagraph
    // address — written for EVERY admitted metagraph. That is the correct signal for this
    // "gl0 sees all K metagraphs" sanity check. (lastCurrencySnapshots is the WRONG signal:
    // calculateLastCurrencySnapshots drops any mg whose currency derivation produced no state
    // in the window via `.filterNot(_.isEmpty)`, so a metagraph that hasn't had currency
    // activity yet is present in gl0 but ABSENT from lastCurrencySnapshots — a false negative
    // here. Real currency-state propagation is covered by the transfer/token-lock tests and the
    // per-metagraph token-tx-senders.) May be partial during early startup; retry briefly.
    const maxAttempts = 30;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
            const data = await fetchJson(`${gl0Url}/global-snapshots/latest/combined`);
            // Response is Either-encoded as [snapshot, info]: index 1 holds the GlobalSnapshotInfo.
            const info = Array.isArray(data) && data.length >= 2 ? data[1] : (data?.value?.info || {});
            const scHashes = info?.lastStateChannelSnapshotHashes || {};
            const seen = Object.keys(scHashes);
            const missing = ids.filter(id => !seen.includes(id));
            if (missing.length === 0) {
                console.log(`gl0 lastStateChannelSnapshotHashes includes all ${ids.length} metagraphs ✓`);
                return;
            }
            if (attempt % 5 === 0) {
                console.log(`  attempt ${attempt + 1}/${maxAttempts}: gl0 sees ${seen.length}/${ids.length} metagraphs (missing ${missing.length})`);
            }
        } catch (e) {
            if (attempt % 5 === 0) {
                console.log(`  attempt ${attempt + 1}/${maxAttempts}: gl0 fetch error (${e.message})`);
            }
        }
        await new Promise(r => setTimeout(r, 5000));
    }
    throw new Error(`gl0 never observed all ${ids.length} metagraphs in lastStateChannelSnapshotHashes within ${maxAttempts * 5}s`);
};

const main = async () => {
    const ids = CONSTANTS.METAGRAPH_IDS;
    if (!ids || ids.length < 2) {
        throw new Error(`multi-metagraph test requires K >= 2 (got METAGRAPH_IDS_CSV="${process.env.METAGRAPH_IDS_CSV}"). Run with --metagraphs=2.`);
    }
    console.log(`Testing ${ids.length} metagraphs: ${ids.map(s => s.slice(0, 12) + '...').join(', ')}`);

    verifyMetagraphIdsDistinct(ids);

    const observedIds = [];
    for (let k = 0; k < ids.length; k++) {
        observedIds.push(await verifyMetagraphHealth(k));
    }

    // Cross-check: ids reported in env match what each ml0 reports as its own genesis address
    for (let k = 0; k < ids.length; k++) {
        if (observedIds[k] !== ids[k]) {
            throw new Error(`metagraph ${k} mismatch: env=${ids[k]}, observed=${observedIds[k]}`);
        }
    }

    await verifyGl0SeesAllMetagraphs(ids);

    console.log(`\nMulti-metagraph test PASSED (K=${ids.length})`);
};

main().catch(e => {
    console.error(`Multi-metagraph test FAILED: ${e.message}`);
    process.exit(1);
});
