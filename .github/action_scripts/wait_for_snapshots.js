const axios = require('axios');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const parseArgs = () => {
  const args = {};
  for (const arg of process.argv.slice(2)) {
    const eqIndex = arg.indexOf('=');
    if (eqIndex !== -1) {
      const key = arg.slice(0, eqIndex).replace(/^-+/, '');
      const value = arg.slice(eqIndex + 1);
      args[key] = value;
    }
  }
  return args;
};

const main = async () => {
  const args = parseArgs();
  const baseUrl = args.url;
  const minOrdinal = parseInt(args.min_ordinal || '2', 10);
  const maxAttempts = parseInt(args.max_attempts || '60', 10);
  const intervalSeconds = parseInt(args.interval || '10', 10);
  const clusterName = args.cluster_name || 'Genesis';

  if (!baseUrl) {
    throw Error('Base URL required via -url=<http://host:port>');
  }

  const snapshotPath = args.snapshot_path || '/global-snapshots/latest';
  const snapshotUrl = `${baseUrl}${snapshotPath}`;
  console.log(`Waiting for ${clusterName} to reach ordinal >= ${minOrdinal} (max ${maxAttempts * intervalSeconds}s)`);
  console.log(`Polling: ${snapshotUrl}`);

  for (let idx = 0; idx < maxAttempts; idx++) {
    try {
      const response = await axios.get(snapshotUrl, {
        headers: { Accept: 'application/json' },
        timeout: 5000
      });

      if (response.status === 200 && response.data) {
        const ordinal = response.data?.value?.ordinal ?? response.data?.ordinal;
        if (ordinal !== undefined) {
          console.log(`${clusterName} at ordinal ${ordinal} (target: ${minOrdinal})`);
          if (ordinal >= minOrdinal) {
            console.log(`${clusterName} reached ordinal ${ordinal} — validators can now join safely`);
            return;
          }
        } else {
          console.log(`${clusterName} responded but no ordinal found in response (${idx + 1}/${maxAttempts})`);
        }
      }
    } catch (e) {
      const errorMsg = e.code === 'ECONNREFUSED' ? 'connection refused' :
                       e.response?.status === 404 ? '404 (no snapshots yet)' :
                       e.message;
      console.log(`${clusterName} not ready (${errorMsg})... (${idx + 1}/${maxAttempts})`);
    }

    if (idx === maxAttempts - 1) {
      throw Error(`${clusterName} failed to reach ordinal ${minOrdinal} after ${maxAttempts * intervalSeconds}s`);
    }

    await sleep(intervalSeconds * 1000);
  }
};

main();
