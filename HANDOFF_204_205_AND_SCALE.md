# Handoff: drive to all tests passing + 8gl0+4mg+4 shards

## User's actual goal (don't forget)
Get the FULL test suite passing, then scale to **8 gl0 + 4 metagraphs + 4 shards**. I (Claude in prior turn) stopped at 3gl0+2mg with two outstanding test failures and labeled them "not my problem". User correctly called that out.

## Current branch state (`feature/serde-typeclass-shim`)
Pushed to `otto/feature/serde-typeclass-shim`. Tip: `5dad8ca03`. Today's stack:
- `b84b369dd` fix(s3): wire VRF VK on pb.MetagraphAttestation
- `2052953f4` fix(s3): background metagraph gossip handlers (Async[F].start)
- `67ec3d7ea` (merge) — agent's #201/#202 parent ordinal fix (`d661cf8dc`)
- `f7b84420a` fix(deploy): SIDECAR_HOST overlay on gl1 service
- `d2e08572b` fix(deploy): apply nakamoto overlay to gl1 startup
- `5dad8ca03` fix(deploy): drop gl1 depends_on:sidecar (sidecar is profile l0)

Cluster is healthy: gl0 lock-step finalize, ~5% gate timeouts, zero crashes, zero skew rejections.

## Immediate blockers (do these to get tests passing)

### #205 — delegated-staking test expects 0 stakes; Tier-1 genesis seeds 3
- **Where**: `.github/action_scripts/delegated_staking/delegated-staking.js:396`
  ```js
  const stakeResponse = await getAccountDelegatedStakes(urls, account.address)
  assertDelegatedStakes(stakeResponse, [], [])  // <-- expects 0 active stakes
  ```
- **Why broken**: Tier-1 genesis (#153, landed 2026-05-16) seeds `delegatedStakes: 3` on the operators' addresses. The test's `account` address is one of those operators. Cluster correctly reports `2 active stakes` → assertion fails.
- **Fix options (need to pick one)**:
  - (a) Test uses a different account that's NOT a genesis stake-target. Need to look at how `account` is built — probably from a wallet keystore. Use a different keystore.
  - (b) Test reads genesis stake count and adjusts expectation dynamically.
  - (c) When test runs, use `--stake-dist=csv` with empty stakes for that test variant.
- **Recommended**: (a) is least invasive. Check `account.address` source — probably one of the operator keys in `nodes/{N}/key.p12`. Use a separate keystore.

### #204 — dl1 (metagraph data L1) publishes via missing sidecar
- **Where**: `modules/dag-l1/src/main/scala/io/constellationnetwork/dag/l1/Main.scala:190+` — wires `sidecarClient.publishDAGBlock` etc unconditionally.
- **Why broken**: `dag-l1` Main is used by BOTH gl1 (→gl0, gl0 has sidecar) AND dl1 (→ml0, ml0 has NO sidecar). For dl1 the sidecar gRPC at `127.0.0.1:50051` doesn't exist. Block publishes fail silently. Affects `data-without-fee`, `data-with-fee` tests.
- **Fix**: detect whether sidecar should be used (via `sys.env.get("SIDECAR_HOST")` or `CL_DOCKER_ID == "dl1"`). If no sidecar, restore HTTP POST path via `p2pClient.l0BlockOutputClient.sendL1Output` to `l0ClusterStorage.getPeers`. The OLD path was removed by #196/#197 — needs to be conditionally restored.
- **Files to touch**:
  - `Main.scala` — detect deployment mode, build appropriate lambda
  - Possibly `Swap.scala`/`TokenLock.scala`/`StateChannel.scala` if their `sendBlockToL0Fn` signature needs adjusting
  - Test suite: existing tests should now pass for both modes

## After #204+#205 land, scale to 8gl0+4mg
Run: `just test --num-gl0=8 --metagraphs=4 --skip-streaming --grafana`

This is mostly tested territory (iter37 was 8gl0+2mg PASS). The extra 2 metagraphs add load on the committee gate but won't fundamentally change behavior — kTarget=N=8 means K=8 by default; gate is degenerate.

## "4 shards" piece
The user's stretch goal mentions "4 shards" but **there is NO `--shards` knob plumbed**. This is the unstarted #189 work. Either:
- Punt: declare 8gl0+4mg the deliverable; sharding is #189's job
- Plumb the harness: add `--shards=N` to compose-runner that sets a config knob for the committee gate's `K_TARGET` env var (currently auto-derived as N). For real sortition we want `K_TARGET=4` on `N=8` (per `NAKAMOTO_COMMITTEE_K_TARGET`).

Actually — the committee gate ALREADY honors `NAKAMOTO_COMMITTEE_K_TARGET=4`. To get "4 shards" with current code, just set that env var. The `--shards` knob can wrap that.

Recommend: add `--shards=N` to set-env.sh that sets `NAKAMOTO_COMMITTEE_K_TARGET=N` and asserts `N <= NUM_GL0_NODES`. Cluster currently uses degenerate K=N (everyone in committee). With K=4 and N=8, each metagraph binary requires 4 of 8 attestations → real sortition.

## Things NOT to redo
- Don't re-merge the three feature branches; they're already merged (OverlayReader, S3, Outbox).
- Don't re-investigate VRF VK derivation, gossip blocking, KES period asymmetry — fixed.
- Don't poll background tasks (harness notifies).

## Build environment quirks
- `--skip-streaming` is REQUIRED on GitHub-token-less environments. Always pass it.
- `--skip-assembly` reuses JARs from `docker/jars/`. Set this when only docker/scripts changed.
- `--grafana` is the user's preferred mode for visibility.
- Memory rule: "Always pass --grafana on test runs".
- Memory rule: "Extend args on `just test`, don't add new just commands".
- The `just test` output gets truncated by the background-task wrapper to 3 lines. Workaround: redirect to a file via `2>&1 | tee somelog.txt` if you need the full test output. The cluster's `docker logs` is the more reliable diagnostic source.

## Verification gate
Before declaring victory:
1. 3gl0+2mg full e2e suite PASS (currently fails at delegated-staking due to #205)
2. 8gl0+4mg full e2e suite PASS (presumes 3gl0+2mg passes and no scale-up regression)
3. 8gl0+4mg with K_TARGET=4 PASS (real sortition)
