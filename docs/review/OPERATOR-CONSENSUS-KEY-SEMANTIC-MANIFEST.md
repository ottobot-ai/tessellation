# Operator consensus key semantic manifest

The canonical machine-readable inventory is
[`OPERATOR-CONSENSUS-KEY-SEMANTIC-MANIFEST.tsv`](OPERATOR-CONSENSUS-KEY-SEMANTIC-MANIFEST.tsv).
`OperatorConsensusKeySemanticManifestSuite` parses that file and compares its reviewed lexical call patterns with every production Scala
source. It removes comments and string/character literals before matching, so a copied spelling in non-code cannot preserve a stale row.

This manifest is a review tripwire, not a correctness proof. A row means that the named higher-level lexical call shape has been found,
classified, and attached to an uncommented/unquoted `test`/`pureTest` call shape in a test source. The lexer does not resolve that call to a
Weaver symbol or prove the named vector exercises the row. It also does not prove the call site has the exact branch-historical registry,
roster, stake, eta, or Phase-2 witness required by `KEYREG-006..010`.

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
5. a row omits the explicit `not qualified` limitation.

The inventory is keyed by `(kind, source file, occurrence count)`: moving a call between lines in the same file, or replacing one same-kind
call with another in that file while preserving the count, is not detected. It is not a Scala AST, symbol-resolution, or call-graph proof;
aliases, new wrapper APIs, and new spellings require an explicit scanner and manifest review. Ordinary literals are ignored, while a
separate fail-closed tripwire rejects reviewed call spellings anywhere inside a lexically recognized interpolated literal.

The existing raw-primitive guard remains independent. Both guards must pass: the raw guard catches reviewed direct primitive spellings,
while this manifest catches reviewed higher-level API spellings.

## Remaining activation blockers

`KEYREG-011` remains open. Closing it still requires the generated branch/era/operator cross-consumer matrix, exact N-2 pair plus
roster/stake resolution, exact N-1 eta, exact Phase-2 shard references, runtime local secret-bundle selection, assigned watchtower coverage,
the optimistic sampler, portable tower membership evidence, and exact offence-parent slashing resolution. No row in this manifest activates
those paths or supplies a selection policy.
