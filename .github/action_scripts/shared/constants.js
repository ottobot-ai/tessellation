const CONSTANTS = {
    // 600 × 1s = 10 min. The 300 (5 min) value was the previous bump from 120
    // (2 min) — sized as ~1 attempt above the observed time-to-include for
    // late-test token locks. Under 8gl0+4mg+4shards load (2026-05-19/20), ml0
    // → gl0 binary propagation runs longer: observed failures at 300 attempts
    // with the cluster still healthy but not yet caught up. 10 min headroom
    // absorbs the longer chain (m0..m3 each x cl1+ml0+dl1; gl0 has 8 voters)
    // without ballooning run-time when healthy (the loop short-circuits as
    // soon as the lock appears in gl0's combined snapshot).
    MAX_VERIFICATION_ATTEMPTS: 600,
    VERIFICATION_INTERVAL_MS: 1000,
    EXPIRATION_VERIFICATION_INTERVAL_MS: 10 * 1000,
    SNAPSHOT_WAIT_TIME_MS: 15 * 1000,
    DEFAULT_COMPRESSION_LEVEL: 2,
    DEFAULT_LAST_VALID_EPOCH_PROGRESS: 50,
    CURRENCY_TOKEN_ID: process.env.METAGRAPH_ID,
    // K metagraph IDs in genesis order; comma-separated by compose-runner.sh.
    // For K=1, METAGRAPH_IDS[0] === METAGRAPH_ID; the legacy CURRENCY_TOKEN_ID
    // continues to point at metagraph 0 for backward compat.
    METAGRAPH_IDS: (process.env.METAGRAPH_IDS_CSV || process.env.METAGRAPH_ID || '')
        .split(',')
        .filter(s => s.length > 0)
};

const PRIVATE_KEYS = {
    key1: '595a30ab6c62ae48a23414951e2703f49f8c0040b9801738ad3550475389d811',
    key2: 'e70e7972630a49f90b0bfb55557287634dbdeb1a6147bba90ac8e3a65e0b41e8',
    key3: '4af811856157548d1f24316ffd9cb9b87fb9e2327d3ce702862cb6e5b39dd219',
    key4: 'dfc636ffb5abd844f6d5600f753923df5d8f63de2851d1eb6dd8ce3ac61e9699',
};

module.exports = {
    CONSTANTS,
    PRIVATE_KEYS
}