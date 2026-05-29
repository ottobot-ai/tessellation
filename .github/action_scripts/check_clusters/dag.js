const { clusterCheck, checkGlobalL0Node } = require("./shared")
const { parseSharedArgs } = require('../shared');

const createConfig = () => {
  const args = process.argv.slice(2);

  if (args.length < 5) {
    throw new Error(
        "Usage: node script.js <dagl0-port-prefix> <dagl1-port-prefix> <ml0-port-prefix> <cl1-port-prefix> <datal1-port-prefix> <extended-dag-l1>"
    );
  }

  const sharedArgs = parseSharedArgs(args.slice(0, 5));
  const extendedDAGL1 = args[5] === 'true';
  return { ...sharedArgs, extendedDAGL1 };
};

const checkDAGL1Node = async (config) => {
  const { dagL1PortPrefix } = config
  const host = process.env.TEST_HOST || 'http://localhost';
  const isRemote = host && host !== 'http://localhost';
  // gl1 EXTERNAL host port = GL1_EXT_PORT_BASE + i*10 (single source of truth,
  // exported by set-env.sh; docker-env-setup.sh binds the same band). The gl1
  // INTERNAL/container ports stay in the dagL1PortPrefix (9100) band — do NOT
  // address those from the host. GL1_URL already points at node 0's external
  // port, so honor it for the genesis entry.
  const gl1ExtPortBase = parseInt(
    process.env.GL1_EXT_PORT_BASE || `${Number(dagL1PortPrefix) * 100}`,
    10
  );
  const gl1Url = process.env.GL1_URL || `${host}:${gl1ExtPortBase}`;

  if (isRemote) {
    const infos = [{ name: 'DAG L1', baseUrl: gl1Url }];
    await clusterCheck(infos, false, 'DAG L1', 1, true);
  } else {
    const numNodes = config.extendedDAGL1 ? 6 : 3;
    const infos = [];
    for (let i = 0; i < numNodes; i++) {
      const baseUrl = i === 0 ? gl1Url : `${host}:${gl1ExtPortBase + i * 10}`;
      infos.push({ name: `DAG L1 - ${i + 1}`, baseUrl });
    }
    await clusterCheck(infos, false, 'DAG L1', numNodes, true);
  }
};

const main = async () => {
  const config = createConfig()

  await checkGlobalL0Node(config);
  await checkDAGL1Node(config);
};

main();