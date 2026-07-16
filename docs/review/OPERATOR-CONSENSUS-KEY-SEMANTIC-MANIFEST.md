# Operator consensus key semantic manifest

The canonical machine-readable inventory is
[`OPERATOR-CONSENSUS-KEY-SEMANTIC-MANIFEST.tsv`](OPERATOR-CONSENSUS-KEY-SEMANTIC-MANIFEST.tsv).
`OperatorConsensusKeySemanticManifestSuite` parses that file and compares its reviewed lexical call patterns with every production Scala
source. It removes comments and string/character literals before matching, so a copied spelling in non-code cannot preserve a stale row.

This manifest is a review tripwire, not a correctness proof. A row means that the named higher-level lexical call shape has been found,
classified, and attached to an uncommented/unquoted `test`/`pureTest` call shape in a test source. The lexer does not resolve that call to a
Weaver symbol or prove the named vector exercises the row. It also does not prove the call site has the exact branch-historical registry,
roster, stake, eta, or Phase-2 witness required by `KEYREG-006..010`.

The `qualification_source`, `qualification_anchor`, and `required_zero_effects` columns add a bounded K7a obligation inventory. Every
non-`BLOCKED` row names an existing adapter-specific negative test and the authority effects that a completed qualification must keep zero
after that adapter rejects an unqualified identity. The checked obligation vocabulary is `Draw`, `EtaLookup`, `PossessionProof`, `Replay`,
`KesSign`, `EdSign`, `AggregatorRecord`, `TrackerRecord`, `Store`, `Adopt`, `Publish`, and `Slash`. The additional
`direct_zero_effects` and `dominated_zero_effects` columns record only reviewed K7b evidence metadata. A direct effect has a counter or
queried sink in the named test. A dominated effect is zero because a directly observed earlier gate was zero and the production source
orders that gate before the dominated effect. The semantic suite checks this evidence against a hard-coded reviewed
`(row ID, qualification source, qualification anchor, direct effects, dominated effects)` tuple; neither an unreviewed row nor a TSV-only
source/anchor substitution can acquire or preserve an evidence claim. Rows with `-` in both evidence columns retain unproved obligations.
The suite binds metadata to the exact test anchor but does not parse its assertions. This remains bounded reviewed evidence, not inferred
control flow for the whole adapter.

## Status meanings

- `FROZEN_GENESIS`: the live path accepts only a committed period-zero atomic pair or otherwise fails closed. It is not runtime-rotation
  support.
- `FAIL_CLOSED_SCAFFOLD`: the API shape exists, but missing historical authority prevents a positive verdict. Tower verification and
  slashing are in this state.
- `BLOCKED`: the target consumer does not exist and must remain absent until its policy and historical inputs are specified and implemented.

## Inventory result

The checked inventory covers higher-level calls for:

- GL0 leadership draw, local signing capability, snapshot VRF/KES validation, and tip-attestation KES intake;
- metagraph admission draw, sender signing, receiver verification, and admission-equivocation evidence;
- execution membership, staircase duty, producer and replay-attester possession proofs/signatures, certificate intake, and checkpoint
  equivocation evidence;
- NiPoPoW tower pair lookup and proof verification.

`OperatorConsensusKeyQualificationMatrix` adds executable frozen-view identity vectors generated through
`CanonicalOperatorConsensusFixture.makePopulation(2)`. Operator A supplies the authority view. Operator B is a separately signed,
loader-validated period-zero control that exists in the combined fixture population but is absent from A's authority view. The real
genesis-only resolver accepts A's exact atomic pair and rejects B as an absent operator, B's complete registered KES+VRF pair substituted
for A, an unrooted runtime-shaped record with a dummy proof presented to the frozen resolver, and a malformed carried VRF key. The runtime-
shaped negative is not loader- or cryptographically validated. This is fixture and frozen-resolver qualification only. It is not a common
adapter layered over the distinct production consumers.

K7b-1 additionally qualifies the frozen-genesis `ShardCheckpointAttestationEmitter` boundary. For the fixture's one included metagraph, the
concrete acceptance manager invokes its injected re-execution hook once, receives the fixture-selected matching root, and mints the
otherwise-unconstructible verified capability. This exercises the production capability-construction order; the injected hook is a stub
and does not prove CL1 replay semantics or economic correctness. Against that same capability, the emitter separately rejects missing
authority, loader-validated B's VRF substituted for A, loader-validated B's long-term key paired with A's VRF, an explicitly unrooted dummy
runtime-shaped record, and malformed 31-byte local VRF evidence. Each rejection directly observes zero eta lookup, possession proof, KES
signing, tracker record, and publication; Ed25519 signing is source-order dominated by the zero KES call. The positive loader-validated A
control proves all emitter counters and sinks are live. Separate staged negatives prove missing KES capability and failed local certificate
verification cannot reach tracker record or publication. Production source orders `reExecPath` before private capability construction and
the emitter accepts only that capability; this cut exercises that ordering and the emitter-owned frozen identity boundary without claiming
the stub proves replay. It does not prove zero draw, store, adopt, slash, or a historical runtime-key path.

Two target roles are deliberately recorded as absent:

- deterministic assigned-watchtower selection and its positive replay-coverage identity;
- the VRF/registry-bound sampler for the ratified optimistic K/alpha/beta finality rail.

The current tip-attestation intake is authenticated telemetry while optimistic authority remains dark. It is not the missing optimistic
sampler. The current watchtower replay/fraud scaffold is not an assigned-watchtower protocol.

## CI contract

The semantic suite fails when:

1. a higher-level KES/VRF draw, proof, signing, duty, certificate, tower, or evidence call matching a reviewed spelling appears outside the
   manifest;
2. a reviewed call moves between files, is renamed, or changes occurrence count without a manifest review;
3. an explicitly blocked selector matching a reviewed spelling silently appears;
4. an uncommented/unquoted `test`/`pureTest` call shape is removed or its exact test name changes without updating the row;
5. a non-blocked row lacks a real adapter-negative source/anchor, uses an unknown or duplicated zero-effect obligation, claims evidence
   outside its required obligation, changes a reviewed K7b source/anchor/effect tuple, or the manifest no longer covers the complete
   obligation vocabulary;
6. a blocked row claims an adapter anchor or obligation that cannot exist while the consumer remains absent;
7. a row omits the explicit `not qualified` limitation.

The inventory is keyed by `(kind, source file, occurrence count)`: moving a call between lines in the same file, or replacing one same-kind
call with another in that file while preserving the count, is not detected. It is not a Scala AST, symbol-resolution, or call-graph proof;
aliases, new wrapper APIs, and new spellings require an explicit scanner and manifest review. Ordinary literals are ignored, while a
separate fail-closed tripwire rejects reviewed call spellings anywhere inside a lexically recognized interpolated literal.

The existing raw-primitive guard remains independent. Both guards must pass: the raw guard catches reviewed direct primitive spellings,
while this manifest catches reviewed higher-level API spellings.

The generated matrix and adapter mappings are intentionally separate. The generated matrix executes the shared frozen atomic-pair
boundary. Snapshot intake, admission, producer, attester, checkpoint adoption, tower, and slashing retain their own concrete negative
anchors because those adapters have different inputs and side effects. The `required_zero_effects` column is the remaining instrumentation
contract, not evidence that the named anchor observes every effect. Only `KSEM-EXEC-008..011` currently carry reviewed direct or dominated
evidence, and only for the effects listed in their evidence columns. Treating the mappings as one executable runtime abstraction would hide
those differences and would be a false proof.

## Remaining activation blockers

`KEYREG-006`, `KEYREG-010`, and `KEYREG-011` remain open. K7a qualifies frozen-genesis resolver rejection and inventories the current
adapter anchors and zero-effect obligations; K7b-1 qualifies only the frozen execution-attester effects described above. Closing those
gates still requires executable instrumentation for the remaining adapter/effect rows, the generated
historical branch/era/operator cross-consumer matrix, exact N-2 pair plus
roster/stake resolution, exact N-1 eta, exact Phase-2 shard references, runtime local secret-bundle selection, assigned watchtower coverage,
the optimistic sampler, portable nonempty-tower membership evidence, and positive exact-offence-parent slashing/runtime consumers. No row
in this manifest activates those paths or supplies a selection policy.
