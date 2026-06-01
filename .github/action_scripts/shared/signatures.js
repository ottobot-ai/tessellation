const { dag4 } = require('@stardust-collective/dag4');
// Use the SDK keystore's serializeBrotli (brotli-wasm-backed) instead of the standalone `brotli`
// npm pkg. The standalone pkg's Emscripten build installs a process-level uncaughtException handler
// that crashes the harness (exit 7) on real payloads. serializeBrotli normalizes (sort keys + drop
// nulls) and brotli-compresses, matching the node's circe Printer(sortKeys, dropNullValues) +
// brotli4j-q2 signing preimage — it's the same helper the (passing) currency-tx path signs with.
const { serializeBrotli } = require('@stardust-collective/dag4-keystore');
const jsSha256 = require('js-sha256');
const { CONSTANTS } = require("./constants");

const sortAndRemoveNulls = (obj) => {
    const processValue = (value) => {
        if (value === null) return undefined;
        if (Array.isArray(value)) {
            return value.map(v => processValue(v)).filter(v => v !== undefined);
        }
        if (typeof value === 'object') {
            return sortAndRemoveNulls(value);
        }
        return value;
    };

    const cleanObj = Object.entries(obj)
        .reduce((acc, [key, value]) => {
            const processed = processValue(value);
            if (processed !== undefined) {
                acc[key] = processed;
            }
            return acc;
        }, {});

    return Object.keys(cleanObj)
        .sort()
        .reduce((acc, key) => {
            acc[key] = cleanObj[key];
            return acc;
        }, {});
};

const sortedJsonStringify = (obj) => JSON.stringify(sortAndRemoveNulls(obj));

const SerializerType = {
    STANDARD: 'standard',
    BROTLI: 'brotli'
};

// Returns the brotli-compressed signing preimage as a HEX string (same return shape as the
// standard serializer below), so the shared generateProof can uniformly `Buffer.from(_, 'hex')`.
// `serializeBrotli` performs its own normalization (sort keys + drop nulls, identical to
// sortAndRemoveNulls) and returns a Uint8Array, which we hex-encode.
const brotliSerialize = async (value, compressionLevel = CONSTANTS.DEFAULT_COMPRESSION_LEVEL) => {
    const compressed = await serializeBrotli(value, compressionLevel);
    return Buffer.from(compressed).toString('hex');
};

const createSerializer = (type = SerializerType.STANDARD) => {
    const standardSerializer = {
        serialize: (value) =>
            Buffer.from(sortedJsonStringify(value), 'utf8').toString('hex')
    };

    const brotliSerializer = {
        serialize: async (value) =>
            await brotliSerialize(value)
    };

    return type === SerializerType.BROTLI ? brotliSerializer : standardSerializer;
};

const generateProof = async (message, walletPrivateKey, account, serializerType = SerializerType.STANDARD) => {
    const serializer = createSerializer(serializerType);
    const serializedTx = await serializer.serialize(message);
    const hash = jsSha256.sha256(Buffer.from(serializedTx, 'hex'));

    const signature = await dag4.keyStore.sign(walletPrivateKey, hash);
    const publicKey = account.publicKey;
    const cleanPublicKey = publicKey.length === 130 ? publicKey.substring(2) : publicKey;

    return { id: cleanPublicKey, signature };
};

module.exports = {
    sortedJsonStringify,
    SerializerType,
    createSerializer,
    generateProof,
};