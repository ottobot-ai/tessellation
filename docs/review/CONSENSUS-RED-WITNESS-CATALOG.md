# Consensus RED Witness Catalog

**Status:** TRACKED, INTENTIONALLY FAILING, EXCLUDED FROM ORDINARY `Test`

**Measured:** 2026-07-29 on `feature/committee-state-diff`

This catalog is the custody record for consensus exploit witnesses that must
remain executable without making the ordinary test task permanently red. The
build defines `RedTest = config("red").extend(Test)` only for `nodeShared` and
`dagL0`.

- `sbt redCompile` must pass.
- `sbt redWitnesses` must return nonzero while any listed defect remains open.
- `sbt test` and the ordinary per-project `Test` configurations do not discover
  `src/red/scala`.
- A witness assertion may not be weakened to make this source set green. A
  runtime fix either makes the existing behavioral assertion pass or replaces a
  temporary source anchor with a stronger typed/behavioral assertion.

SHA-256 values cover the exact UTF-8 Scala source file bytes. Update a row in the
same change as any intentional witness edit.

| FQN | Finding / invariant | Expected result at this revision | SHA-256 | Stale-anchor notes | Promotion condition |
| --- | --- | --- | --- | --- | --- |
| `io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.GlobalTipAttestationEmitterSuite` | `RTA-RED-001/003/004/005/006/007/017`: GL0 optimistic authority requires opaque authenticated execution plus current-tip preference; receipt, raw storage, restored heads, and buffered-parent arrival cannot mint it. | 7 cases: 4 failed (`RTA-RED-001` opaque declarations, `003`, `005/006`, `007`), 2 errors (`004`, `017`), 1 passed. | `2d3450608d1d22f2ce0c34975f0d8d8bba32eff71f4c5f35abefa450c88fedfb` | `004` and `017` currently error because their source-slice end anchors moved. This is recorded evidence, not a semantic pass. All source spelling checks are temporary. | Replace missing anchors with typed API/behavior tests while implementing replay receipts, validated storage, preference authority, recovery-only seeds, and buffered-child full replay. Promote only when every case passes with no direct capability construction or raw-store bypass. |
| `io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.RestartAuthorityRedSuite` | `RA-01/02`: a persisted GL0 head is a recovery seed, never selected/finality/production authority; unauthenticated rebootstrap remains `RecoveryRequired`. | 4 cases: 2 failed (restored-head selection and slot-production fallback), 1 error (consensus-resource startup source anchor), 1 passed. | `cf27b99a30088856061b098a30de649797d7568cb65235ed66527d3992c4545d` | The consensus-resource startup slice uses a stale comment anchor. The other source slices still resolve but are temporary. | Land physically separate recovery storage plus exact forward replay and selected executed authority. Replace source slices with restart/fault tests, then promote when all four pass. |
| `io.constellationnetwork.node.shared.domain.nakamoto.O22FinalLaunchWireRedSuite` | `O22-01/02/03`, `WT-000`: ordinal-zero ScodecV1 carries bounded canonical `InvalidStateProofEvidenceV1`; JSON has no digest authority; adjudication is five-result and candidate-atomic; field 34 is integrated; local HOCON cannot change consensus effects. | 5 cases: 4 failed (`O22-WIRE-RED-001`, `003`, `004`, `005`), 0 errors, 1 passed (`002`, known provisional/trailing Scodec bytes reject). | `6b21e9f7829eb55cf33030803132d42768e0c70e1cbe506688ef8c3daefad502` | This is the final-launch target, not the deleted pre-activation-absence premise. `001`, `003`, `004`, and `005` use temporary source inventory checks; they do not freeze implementation identifiers beyond ratified wire/verdict names. | Complete the Scodec digest cutover, bounded evidence/parameter grammar, portable five-result adjudication, atomic field-34/O23 sink, and multi-node local-config differential tests. Replace source checks with codecs/oracles/integration tests and promote when all launch paths pass. |
| `io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashPrincipalRedSuite` | `ECO-06`: an invalid-state-proof slash must debit the exact active or pending offense-time backing principal before bounty/burn, remain slashable through withdrawal, deduplicate only after debit, and prevent maturity refund. | 5 cases: 5 failed, 0 errors, 0 passed. | `96f568291a4eee8e636debd814b8d8e4a3d5eebc6c3c2e06a5a752f4b3c04ca7` | Behavioral witness with no stale source anchors. It intentionally exercises the current broken active-only API, which is not the target interface. | Implement O23 `BondId`, complete E-2 liability, pending/active exact lock debit, tombstones, slash-before-release ordering, actual-debit bounty/burn conservation, restart, and reorg. Adapt to the final API without weakening outcomes, then promote. |
| `io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashCooldownAxisMismatchRedSuite` | `SLASH-07`, `O20-02`, `WT-010B`: exclusion is one rooted half-open artifact-`EtaPeriod` interval; event ordinals and `EpochProgress` cannot be mixed. | 1 case: 1 failed, 0 errors, 0 passed. | `7fd927373ece8ccc221344eed83c0b085c59003f7cbe2ea68ceaba4b0948a766` | Behavioral witness with no stale source anchors. It preserves the concrete ordinal-599 versus epoch-expiry-142 escape. | Replace the live writer/reader with the O20 interval, migrate every draw to the same exact-parent period axis, add both boundary cases, then promote when the exploit trace passes. |
| `io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency.CurrencyExactGlobalReplayAuthorityRedSuite` | `LINEAGE-RED-004..007`: CL1 framework replay uses one bounded exact-hash Phase-2 candidate view; ambient receiver head/GSI, same-ordinal siblings, raw callbacks, ancestry fan-out, and retries cannot become authority. Native GL1 universal GL0 execution is unchanged. | 8 cases: 2 failed (`LINEAGE-RED-006`, `007A`), 0 errors, 6 passed. | `d4efe27aa61bbed9191bea205ef46ef5ad003cad64b5a6ecc0c2ae2d8c1474f2` | `004`, `006`, `007A`, `007B`, and `007D` are explicitly temporary source boundaries. Their anchors resolve at this revision; only `006` and `007A` expose live defects. | Freeze and implement the exact signed replay reference and `CandidateCurrencyReplayView`, hash-bound Phase-2 gate, root-verified image, field-32 witness/removal, and lease/CAS lifecycle. Replace source checks with sibling/reorg/concurrency behavior tests before promotion. |

## Measured Aggregate

`sbt --error redWitnesses` currently reports:

| Project | Total | Failed | Errors | Passed |
| --- | ---: | ---: | ---: | ---: |
| `nodeShared / RedTest` | 19 | 12 | 0 | 7 |
| `dagL0 / RedTest` | 11 | 6 | 3 | 2 |
| **Total** | **30** | **18** | **3** | **9** |

The three errors are stale source anchors called out above. They must be
converted to assertions, not silently treated as expected success. The
`redWitnesses` alias runs both projects with `all`, so failure in one project
does not hide the other project's result.
