const CONSTANTS = {
    // 300 × 1s = 5 min. The previous 120 (2 min) was ~1 attempt above the
    // observed time-to-include for late-test token locks; under cluster load,
    // ML0 falls behind enough that propagation can take 2-3 min after a
    // successful POST. 5 min gives consistent headroom without ballooning
    // run-time when things are healthy (each verification short-circuits as
    // soon as the lock appears).
    MAX_VERIFICATION_ATTEMPTS: 300,
    VERIFICATION_INTERVAL_MS: 1000,
    EXPIRATION_VERIFICATION_INTERVAL_MS: 10 * 1000,
    SNAPSHOT_WAIT_TIME_MS: 15 * 1000,
    DEFAULT_COMPRESSION_LEVEL: 2,
    DEFAULT_LAST_VALID_EPOCH_PROGRESS: 50,
    CURRENCY_TOKEN_ID: process.env.METAGRAPH_ID
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