# Sparse Merkle Tree (SMT) for Historical State Proofs — Survey + Tessellation Fit + Scope

**Status:** research / design, 2026-05-29. Owner: orchestrator. Read-only survey + scope recommendation; **no implementation in this doc**. Parallel to the in-flight gl1 follow work — see [GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md](./GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md) (Axis 2).

## TL;DR — feasibility verdict

Adding an SMT **algebra + reference impl as a parallel, additive proof/history primitive is feasible and low-risk** — it slots cleanly alongside the existing `MerklePatriciaTrie` family (same `Hasher[F]` / cats-effect / sealed-ADT idiom) and **does not touch the consensus MPT**. Replacing the consensus MPT *substrate* with an SMT is a multi-month, consensus-critical hard-fork-class change and is **not recommended** unless an explicit goal (ZK proofs, or wanting native absence in the live consensus root) forces it.

**Confirmed JMT/SMT-for-historical-proofs adopters:** **Diem/Libra** (origin of the Jellyfish Merkle Tree, versioned), **Aptos** (production JMT, version-indexed state, light-client `SparseMerkleProof` at arbitrary versions), **Penumbra** (`penumbra-zone/jmt`, async port of Diem's JMT, ICS-23 proofs for IBC). **Cosmos SDK** has an *approved-but-not-fully-shipped* migration (ADR-040 / Store v2) from versioned IAVL+ to a Diem-lineage SMT (`celestiaorg/smt`). Sui is **not** a JMT user (different accumulator model). **The single most decision-relevant precedent is Aptos: it is the production system that does exactly "prove key K had value V at version N" with a versioned sparse Merkle tree.**

**Recommended scope:** parallel/additive primitive (Part C), sliced trait → in-memory impl + property tests → versioned/historical layer → proof+verify → use-case wiring.

**Open question that gates the design (Part C.3):** the precise historical-proof **use case** is not yet pinned. A *full versioned JMT* (every key independently provable at every past version) and a much cheaper *history-of-roots index over the existing MPT* (prove "this field root / this key was V as of finalized ordinal N", using the proof machinery we already have) solve different problems at very different cost. This needs the requester to confirm before committing to JMT.

---

## PART A — Survey (cited)

### A.1 Jellyfish Merkle Tree (JMT) — the versioned SMT

The **Jellyfish Merkle Tree** is a "space-and-computation-efficient sparse Merkle tree optimized for LSM-tree-based key-value storage, designed specially for the Diem Blockchain." Logically it is a **256-bit sparse Merkle tree** with the standard SMT optimization: *any subtree containing 0 or 1 leaf is collapsed to a placeholder (default-hash) node or to that single leaf*, so the cost of the (otherwise 256-deep) sparse tree is avoided. Structurally it is a **radix-16 (nibble-path) addressable Merkle tree** — physically similar to Ethereum's modified Patricia Merkle trie, but with JMT-specific node layout. ([Diem JMT paper](https://developers.diem.com/papers/jellyfish-merkle-tree/2021-01-14.pdf), [Olshansky 5P;1R JMT](https://olshansky.medium.com/5p-1r-jellyfish-merkle-tree-d2d7dc7dfa8e), [diem_jellyfish_merkle rustdoc](https://diem.github.io/diem/diem_jellyfish_merkle/index.html))

**How versioning works (the load-bearing part for historical proofs).** JMT uses a **version-based node key**: the physical KV key is `NodeKey = (version | nibble_path)`, where `version` is a monotonically increasing counter (≈ the number of transactions / state updates applied). Each commit writes a fresh set of nodes prefixed by the new version, so **commits never overwrite prior versions' nodes** — old versions remain on disk and addressable. The version prefix also sorts node writes lexicographically, which is what makes JMT cheap on LSM-tree/RocksDB compaction. ([Diem JMT paper](https://developers.diem.com/papers/jellyfish-merkle-tree/2021-01-14.pdf), [aptos_jellyfish_merkle](https://docs.rs/aptos-jellyfish-merkle/0.1.3/aptos_jellyfish_merkle/), [RISE versioned merkle tree](https://docs.risechain.com/rise-stack/version-merkle-tree.html))

**Historical inclusion / absence proofs.** Because every version's nodes persist, you can **generate a proof against any past version**: a `SparseMerkleProof` for version N is a path of sibling hashes from the leaf for key K up to the version-N root. The structure yields three proof kinds — **inclusion** (path to a leaf with the queried key exists), and **exclusion / non-inclusion** (the path terminates at a *placeholder* (default) node, or at a *different* leaf occupying the position). Average Merkle proof size is **Θ(log(number of non-empty leaves))** rather than the naive 256, thanks to the empty-subtree collapse. ([JMT design summary, Olshansky](https://olshansky.medium.com/5p-1r-jellyfish-merkle-tree-d2d7dc7dfa8e), [Prism JMT proofs](https://docs.prism.rs/jellyfish-merkle-proofs.html))

**Confirmed adopters (network claims):**

| Network | Uses JMT? | How / evidence |
|---|---|---|
| **Diem / Libra** | Yes — **origin** | JMT was designed for Diem; the paper and `diem_jellyfish_merkle` crate are the reference. ([paper](https://developers.diem.com/papers/jellyfish-merkle-tree/2021-01-14.pdf), [crate](https://diem.github.io/diem/diem_jellyfish_merkle/index.html)) |
| **Aptos** | Yes — production | "The JMT is a popular Versioned Merkle Tree designed for Aptos." Aptos uses the version-based key schema to optimize write-amplification on RocksDB; the light-client protocol works with `SparseMerkleProof` (value inclusion at a block height) + `LedgerInfoWithSignatures` certifying the SMT root, and must generate a `SparseMerkleProof` for an *arbitrary version* plus fetch the matching signed ledger info. ([aptos_jellyfish_merkle](https://docs.rs/aptos-jellyfish-merkle/0.1.3/aptos_jellyfish_merkle/), [aptos-core #10747 light client](https://github.com/aptos-labs/aptos-core/issues/10747), [Merklized Data on disk](https://hackmd.io/@bvrooman/merklized_data_on_disk)) |
| **Penumbra** | Yes — production | `penumbra-zone/jmt` is "an async-friendly sparse merkle tree implementation based on Diem's Jellyfish Merkle Tree"; they wrote an **ICS-23** proof spec for it so IBC counterparties can verify Penumbra state. Originally tracked as "Integrate Libra's Jellyfish Merkle Tree" (penumbra #249). ([penumbra-zone/jmt](https://github.com/penumbra-zone/jmt), [Penumbra summer 2022 update](https://penumbra.zone/blog/2022-summer-update), [penumbra #249](https://github.com/penumbra-zone/penumbra/issues/249)) |
| **Sui** | **No** (do not claim JMT) | Survey found no evidence Sui uses JMT; Sui's object/accumulator model differs. Treat "Sui uses JMT" as **unconfirmed/false**. |

> Note: the `jmt` Rust crate ([docs.rs/jmt](https://docs.rs/jmt/latest/jmt/), [jmt-blake3](https://lib.rs/crates/jmt-blake3)) is a generalized, hash-pluggable descendant of the Penumbra port and is the de-facto reusable reference impl in the ecosystem. Relevant to us because **it parameterizes over the hash function** — i.e. a Blake2b/Blake3 JMT (not just Poseidon/Keccak) is normal.

### A.2 Other SMT adopters + variants

- **Cosmos SDK — IAVL+ → SMT (ADR-040 / Store v2).** Cosmos historically uses the **IAVL+ tree**: a versioned, snapshottable, self-balancing AVL+ Merkle tree giving inclusion **and** non-inclusion proofs at any version in `log₂(n)` hashes. ADR-040 separates **state commitment (SC)** from **state storage (SS)** and replaces IAVL with **Celestia's SMT** (`celestiaorg/smt`), explicitly described as "based on Diem (called jellyfish) design — a compute-optimized SMT replacing all-default subtrees with a single node (same approach as Ethereum 2), with compact proofs." **Status: approved ADR, planning/partial — not a completed migration.** Takeaway: even a mature versioned-tree ecosystem is migrating *toward* the Diem SMT lineage. ([cosmos/iavl overview](https://github.com/cosmos/iavl/blob/master/docs/overview.md), [IAVL proof spec](https://github.com/cosmos/iavl/blob/master/docs/proof/proof.md), [ADR-040](https://docs.cosmos.network/v0.50/build/architecture/adr-040-storage-and-smt-state-commitments), [celestiaorg/smt](https://github.com/celestiaorg/smt))
- **`celestiaorg/smt`** — Go SMT for a key-value map, with the Libra/Diem empty-subtree optimization reducing hashing to O(k) for k non-empty elements; **non-membership proof** carries `NonMembershipLeafData` (the unrelated leaf occupying the queried slot), `nil` for membership. ([pkg.go.dev celestiaorg/smt](https://pkg.go.dev/github.com/celestiaorg/smt), [smt proofs_test](https://github.com/celestiaorg/smt/blob/master/proofs_test.go))
- **Mina** — succinct-blockchain; the recursive zk-SNARK certifies consensus + a Merkle root to a recent ledger, and verifiers request **Merkle paths (witnesses)** to the accounts they care about; block producers only need to store the account record + Merkle path for accounts they can propose. This is a Merkle *ledger* with inclusion witnesses rather than a sparse-key-space SMT, but it is the canonical "SNARK-certified root + on-demand inclusion path" model. ([Mina 22kB technical reference](https://minaprotocol.com/blog/22kb-sized-blockchain-a-technical-reference), [Mina Merkle tree docs](https://docs.minaprotocol.com/zkapps/o1js/merkle-tree))
- **ZK-rollup SMTs (Poseidon-hashed):** **Polygon zkEVM** stores state as key-value in a **binary, fully-updatable Sparse Merkle Trie** built with the **Poseidon** hash (chosen for STARK-friendliness — minimizes ZK constraints). This is the ZK-specific motivation: the hash must be cheap *inside a circuit*. **zkSync** and **Starknet** similarly use ZK-friendly hashed SMT/trie variants (zkSync specifics not confirmed in this survey; Starknet uses a Pedersen/Poseidon-hashed Merkle-Patricia variant). ([Polygon SMT](https://docs.polygon.technology/zkEVM/concepts/sparse-merkle-trees/sparse-merkle-tree/), [Polygon L2 state tree keys/values](https://docs.polygon.technology/zkEVM/architecture/proving-system/l2statetree-keys-and-values/), [SoK ZK-friendly hashes](https://cdn.prod.website-files.com/63970f25aa7b42e284492d52/68419a894a80434f16e0f580_sok_zk_friendly_hashes.pdf))
- **Aztec — Indexed Merkle Tree (nullifier tree).** A plain depth-254 SMT costs 254 hashes/membership proof and ~254×2 hashes/insertion (non-membership check + insert), which blows up ZK circuit constraints. Aztec's **Indexed Merkle Tree** stores at each leaf a "next-larger" pointer so a non-membership proof is a single inclusion of the "low nullifier" whose range brackets the queried value — turning a depth-32 tree's insertion into 1 non-membership check + 1 pointer update. Relevant as the **cost-optimized absence-proof** design if we ever need cheap set-membership/absence at scale. ([Aztec indexed merkle tree](https://docs.aztec.network/developers/docs/foundational-topics/advanced/storage/indexed_merkle_tree), [Nomos: sparse vs indexed](https://blog.nomos.tech/designing-nullifier-sets-for-nomos-zones-sparse-vs-indexed-merkle-trees/))
- **Ethereum Verkle (contrast — NOT an SMT).** Verkle trees replace the hexary Patricia trie with a **vector-commitment** tree (portmanteau "Vector commitment" + "Merkle"). They are motivated by the same *stateless-client* problem (MPT witnesses are huge: ~3.5 MB for 1000 leaves at 7 levels, vs ~150 kB Verkle at 4 levels — ~23×) but solve it with polynomial/vector commitments, not a sparse hash tree. Cited here only to **disambiguate**: the "stateless / small-witness" motivation is shared, but Verkle is a different primitive and out of scope. ([ethereum.org Verkle](https://ethereum.org/roadmap/verkle-trees), [Verkle integration EIP](https://notes.ethereum.org/@vbuterin/verkle_tree_eip))

### A.3 SMT vs Merkle-Patricia-trie tradeoffs (concrete)

| Property | Sparse Merkle Tree (incl. JMT) | Merkle Patricia Trie (our `MerklePatriciaTrie`) |
|---|---|---|
| **Absence / non-membership** | **Native + uniform** — prove the slot holds the *default* value (or a different leaf). Same shape as inclusion. ([Dahlberg–Pulls](https://eprint.iacr.org/2016/683.pdf), [celestiaorg/smt](https://github.com/celestiaorg/smt)) | **Awkward** — must prove the path terminates without the key (branch slot empty / divergent extension). Doable but a *distinct* proof shape; **our verifier has none today** (`MerklePatriciaInclusionVerifier` is inclusion-only; range proofs bolt on exclusion *boundaries* but the range verifier is "unsound for sets" per the gl1 doc). |
| **Determinism / insertion-order independence** | A unique key-set yields a deterministic root **regardless of insert/remove order**. ([Cartesian/SMT analyses](https://ouvrard-pierre-alain.medium.com/sparse-merkle-tree-86e6e2fc26da)) | Also fully deterministic *as a final structure* — "tries with the same (key,value) bindings are identical down to the last byte" ([Cardano MPT deep-dive](https://cardanofoundation.org/blog/merkle-patricia-tries-deep-dive)). **But** our *incremental* path (`IncrementalTrieOps` + `MptOverlay.withChanges`) is where order/branch-merge subtleties bite — see #116 (MultiBranch reorg / mptRoot-only divergence). SMT's positional addressing sidesteps incremental-merge structural drift. |
| **Proof size** | Θ(log non-empty-leaves) with empty-subtree collapse; binary SMT proofs are simple sibling-lists, compressible with a bitmap of non-default siblings. | Hexary tries give **~4× larger** proofs than binary (15 siblings/level vs 1) — a known stateless-Ethereum pain point. ([EIP-3102 binary trie](https://eips.ethereum.org/EIPS/eip-3102)) |
| **ZK-friendliness** | The structure ZK-rollups standardize on — *if* paired with a ZK hash (Poseidon). With Blake2b it is **not** ZK-friendly. | Hexary + RLP/JSON-ish encoding is ZK-hostile. |
| **Versioning / history** | **First-class in JMT** (version-prefixed node keys; every past version queryable). IAVL+ also versioned. | **Not native.** We get history structurally via `MptOverlay` branch composition over an on-disk base — and crucially the overlay **cannot read value-bytes at an arbitrary historical ordinal** (see B.2 / #287). |
| **Storage overhead** | JMT keeps **all versions** on disk (the cost of historical queryability) — needs pruning/retention. The empty-subtree collapse keeps per-version node count proportional to live leaves, not 2²⁵⁶. | We store the finalized base + in-memory pending branch deltas; we do **not** retain per-ordinal historical value-bytes (that's the gap). |

### A.4 "Historical proofs" — what they're for, and which structure each network uses

| Use case | What it needs | Who does it with what |
|---|---|---|
| **Light-client historical queries** ("what was my balance at block N?") | Inclusion proof at a *past version* + signed root for that version | Aptos: `SparseMerkleProof` @ version N + `LedgerInfoWithSignatures` ([#10747](https://github.com/aptos-labs/aptos-core/issues/10747)). Mina: SNARK-certified root + Merkle witness ([ref](https://minaprotocol.com/blog/22kb-sized-blockchain-a-technical-reference)). |
| **Cross-chain / cross-shard historical reads** ("chain B verifies chain A's state as-of height H") | Inclusion (and sometimes absence) proof against a *committed, signed* root the verifier trusts | Penumbra/Cosmos IBC: **ICS-23** proofs against the JMT/IAVL root ([Penumbra ICS-23](https://penumbra.zone/blog/2022-summer-update)). **Tessellation today:** `ShardSubtreeProofService` proves a key vs a committee-signed per-MG MPT root (point reads only; no absence). |
| **Audit / dispute / fraud-proof evidence** | A proof that some past state did/didn't contain X, checkable by anyone with the signed root | Optimistic-rollup fraud proofs / Aztec nullifier non-membership ([indexed MT](https://docs.aztec.network/developers/docs/foundational-topics/advanced/storage/indexed_merkle_tree)). Tessellation slashing wants "verifiable evidence carried in the tx" (memory: slashing safety bar) — historical-state evidence is a candidate consumer. |

---

## PART B — Tessellation fit

### B.1 The current crypto layer (read)

**MPT core** — `modules/shared/src/main/scala/io/constellationnetwork/security/mpt/`:
- `MerklePatriciaTrie.scala` — immutable trie; `rootHash: MptRoot` is O(1) (nodes compute their digest at construction). `withChanges(upserts, removes)` is the incremental delta path (sort by `CompactNibblePath`, share unchanged subtrees). Hexary.
- `MerklePatriciaNode.scala` — sealed `Leaf | Branch | Extension`, digests via `Hasher[F].prefixedHash(commitment.asJson, prefix)` with per-node-type prefix bytes. **Hash = Blake2b through `Hasher[F]`; commitments are Circe-`asJson`-encoded** (not a ZK hash, not scodec at the node level).
- `prover/` — `MerklePatriciaSingleInclusionProver` (path walk → `List[MerklePatriciaCommitment]`), `MerklePatriciaRangeProver`, `MerklePatriciaBatchInclusionProver`, `MerklePatriciaPrefixProver`, `prover/attestation/` proof types (`MerklePatriciaInclusionProof`, `…RangeProof`, `…BatchInclusionProof`).
- `verifier/` — `MerklePatriciaInclusionVerifier` (**inclusion only**), `MerklePatriciaRangeVerifier` (inclusion proofs + ordering + exclusion *boundaries* — note the gl1 doc flags the range verifier as "unsound for sets today"), `MerklePatriciaBatchInclusionVerifier`.
- `producer/` — `StatelessMerklePatriciaProducer`, `ParallelMerklePatriciaProducer`, `InMemoryMerklePatriciaProducer` (stateful: `stateRef: Ref[F, Map[Hex, Array[Byte]]]` + pending insert/remove refs + per-ordinal root-hash cache), `FileSystemMerklePatriciaProducer`.

**Schema / store** — `modules/shared/.../schema/mpt/`:
- `GlobalStateKey.scala` — the 4-slot key `(networkNamespace, fieldId, contractNamespace, userNamespace)`; `toHex` serializes to the nibble path; `fieldIdFromHex` is the partial inverse; **25 `GlobalStateFieldId` partitions**. Per-field subtree roots are how `stateProof` is structured.
- `MptStore.scala` — content-addressable KV with MPT commitment; values via `ImmutableCodec[V]` (scodec). Key methods: `get/insert/remove/update`, `build(ordinal)`, `sync/syncFull`, `savepoint`/`withTransaction` (rollback bracket), and **`allEntriesAsBytes: F[Map[Hex, Array[Byte]]]`** = `producer.entries` (the *current* in-memory state — see B.2).
- `GlobalStateConverter.scala` — `GlobalSnapshotInfo` ⇄ `Map[GlobalStateKey, bytes]`; `StateChangesAccumulator` is the per-ordinal delta GSAM produces (the natural source for accept-time delta-capture, #287); `fieldRootFromBytes` recomputes a field's subtree root.

**Overlay + historical proof** — `modules/node-shared/.../domain/nakamoto/`:
- `overlay/MptOverlay.scala` — `Passthrough` and `MultiBranch` modes; per-`BranchId` `ChangeSet` accumulation over a finalized base, fold-forward on `finalizeBranch`, undo-journal for reorg-replace (#121), eviction with ancestor protection (#56.9/#113/#115). This is the bug-heavy surface (#54/#70/#113/#115/#116).
- `HistoricalMptProofService.scala` — `proofAtBranch(branch, ordinal, key)` = `overlay.buildRoot(branch, ordinal)` → `MerklePatriciaSingleInclusionProver`. **This is the closest thing we have to a "historical proof" today** — but it reconstructs the trie *structurally from branch composition*, and depends on `buildRoot` which depends on `allEntriesAsBytes` (B.2).
- `sharding/ShardSubtreeProofService.scala` + `ShardSubtreeProofClient.scala` — cross-shard inclusion proofs vs a committee-signed per-MG root (#274/#275 scaffold). **Point reads only; no absence** ("the underlying `MerklePatriciaInclusionVerifier` has no absence-witness support; generate returns `None` for absent keys").
- `schema/nakamoto/follow/FollowVerifyCore.scala` — the **already-built generic verify core**: `Verified[A]` sealed-abstract gate, `ConsumedFieldState`, `FollowVerificationError` (sealed ADT incl. `FieldRootMismatch`, `ValueBindingFailed`). The gl1 follow path uses recompute-and-match (field-root equality) for holders + range/inclusion for stateless consumers.

### B.2 The historical-value-read gap (#287) — and how a versioned SMT would close it

**The gap, concretely.** `MptStore.allEntriesAsBytes` returns `producer.entries` = `stateRef.get`, the **current** in-memory key→bytes map. `MptOverlay.allEntriesAsBytes(branch)` composes that base with the branch's `ChangeSet`. **Neither is parameterized by a historical ordinal for value-bytes** — `buildRoot(branch, ordinal)` passes `ordinal` to `underlying.build(ordinal)` (which affects the *root-hash cache / persist bookkeeping*), but the *entries* it folds the branch delta onto are still "base now," not "base as-of ordinal N." The gl1 doc states this directly (S2a finding): *"the `MptOverlay` can't read value-bytes at a historical ordinal (`allEntriesAsBytes` returns the branch tip, ignoring the ordinal), so per-ordinal historical-diff deltas aren't available from the overlay."* Consequently:
- `HistoricalMptProofService.proofAtBranch(branch, N, key)` can faithfully reconstruct a *past* state only while that ordinal is still reachable as a **pending branch in memory**; once folded into the finalized base, the *historical value at an intermediate ordinal* is gone (only the latest finalized value-bytes for each key remain). It is "historical across pending branches," not "historical across finalized time."
- gl1 worked around this by fetching the **latest-finalized full slice** and recompute-matching it (no per-ordinal replay), with accept-time delta-capture (#287) tracked as the O(changes) optimization — but that still only gives the *latest* finalized state, not arbitrary-ordinal history.

**Why a versioned (JMT-style) SMT solves this more cleanly.** JMT's **version-prefixed node keys** mean every finalized ordinal's nodes persist on disk and the root for ordinal N is reconstructable directly: a proof for `(key, N)` is generated against the version-N subtree with **no in-memory branch replay and no "is this ordinal still pending" caveat**. "Prove key K had value V at finalized ordinal N" becomes a first-class, O(log live-leaves) operation for any N under retention — exactly the Aptos light-client primitive. The overlay's job (pending-branch isolation for *consensus*) and the history primitive's job (durable per-version proofs) cleanly separate; today they're conflated in `proofAtBranch`.

> **Important nuance for the scope decision (Part C):** closing #287 does **not** require a JMT. There are two strictly cheaper points on the curve:
> 1. **Accept-time delta-capture (#287 as already scoped):** persist each ordinal's `StateChangesAccumulator` delta; reconstruct historical value-bytes by replaying deltas. O(changes) transfer, reuses existing proof machinery. **No new tree.**
> 2. **History-of-roots index:** persist `(ordinal → per-field subtree roots)` (we already compute these for `stateProof`), and serve inclusion proofs against a *retained* past root using the **existing** `MerklePatriciaSingleInclusionProver` over a rebuilt-at-N trie (from #287 deltas). Gives "prove K=V as-of finalized N" without JMT's full versioned node store.
>
> A **full versioned JMT** is the right tool only if we want (a) *every* key independently provable at *every* retained version with on-disk node persistence (no replay), and/or (b) a path toward ZK or native absence in the same primitive. Otherwise it's heavier than the problem.

### B.3 What an SMT algebra would look like alongside the MPT (idiom match)

The cleanest shape mirrors the existing `MerklePatriciaProducer` / prover / verifier split and the `FollowVerifyCore` `Verified[A]` discipline. A **trait/typeclass algebra** parameterized over `F[_]: Async: Hasher` and value bytes, with a sealed-ADT proof type and a `Verified`-gated verify:

```scala
// PARALLEL primitive — does NOT replace MerklePatriciaTrie. Hash = Blake2b via Hasher[F]
// (ZK-hash a.k.a. Poseidon only if/when ZK becomes a goal — see Part C, out of scope now).

/** Immutable sparse Merkle tree over 256-bit key positions (key = Hasher.hash(GlobalStateKey.toHex)).
  * Empty-subtree collapse (Diem/JMT style) keeps node count ∝ live leaves. */
trait SparseMerkleTree[F[_]] {
  def get(key: Hex): F[Option[Array[Byte]]]
  def root: F[SmtRoot]
  def insert(key: Hex, value: Array[Byte]): F[SparseMerkleTree[F]]   // returns new tree (structural sharing)
  def remove(key: Hex): F[SparseMerkleTree[F]]
  def withChanges(upserts: Map[Hex, Array[Byte]], removes: Set[Hex]): F[SparseMerkleTree[F]]
}

/** Proof family — sealed, value-binding mandatory (mirror FollowVerifyCore contract bar #2/#3). */
sealed trait SmtProof
object SmtProof {
  final case class Inclusion(key: Hex, value: Array[Byte], siblings: List[SmtSibling]) extends SmtProof
  final case class Absence(key: Hex, witness: AbsenceWitness, siblings: List[SmtSibling]) extends SmtProof
  // AbsenceWitness = Default (slot empty) | OtherLeaf(occupyingKey, occupyingDataDigest)  -- native to SMT
}

trait SmtProver[F[_]]   { def prove(key: Hex): F[Either[SmtProofError, SmtProof]] }      // inclusion OR absence
trait SmtVerifier[F[_]] { def verify(root: SmtRoot, proof: SmtProof): F[Either[SmtProofError, Verified[SmtEntry]]] }

/** Versioned / historical layer — the JMT-defining piece. Optional second slice. */
trait VersionedSmt[F[_]] {
  def commit(version: SnapshotOrdinal, upserts: Map[Hex, Array[Byte]], removes: Set[Hex]): F[SmtRoot]
  def rootAt(version: SnapshotOrdinal): F[Option[SmtRoot]]
  def proveAt(version: SnapshotOrdinal, key: Hex): F[Either[SmtProofError, SmtProof]]   // inclusion or absence @ N
}
```

Notes on idiom fit:
- **Hash:** Blake2b via `Hasher[F].prefixedHash(commitment.asJson, prefix)`, exactly as `MerklePatriciaNode` does — reuse `MerklePatriciaCommitment`-style domain-separation prefixes (per memory rule: route all consensus-bytes hashing through `Hasher[F]` + Circe case class, never hand-rolled Blake2b). **ZK would force Poseidon and is out of scope** unless ZK proofs become a stated goal.
- **Keys:** position = `Hasher.hash(GlobalStateKey.toHex(key))` (a fixed 256-bit position), which also *uniformizes* key distribution (random leaf positions) — sidestepping the structural-merge subtleties the hexary `MptOverlay` hits under MultiBranch (#116).
- **`Verified[A]` gate + mandatory value-binding:** copy `FollowVerifyCore`'s discipline so absence/inclusion proofs can't be consumed unverified, and `Hasher.hashBytes(value) == leaf.dataDigest` is checked *inside* verify (the gl1 doc's contract bar #2).
- **Reference impl:** an in-memory `Ref[F, ...]`-backed `SparseMerkleTree` mirroring `InMemoryMerklePatriciaProducer`; a `FileSystem…`/version-keyed store later if/when durability is needed.

### B.4 Connections to the open issues

- **#286 (prover extension needs absence proofs):** the existing range verifier is "unsound for sets"; cross-shard *set* queries and light clients need genuine absence. **SMT gives absence natively + uniformly** (B.3 `SmtProof.Absence`) — this is the strongest single argument for the parallel primitive. (Aztec's indexed-tree trick is the cost-optimized variant if absence-at-scale becomes hot — A.2.)
- **#116 (MPT MultiBranch insertion-order / reorg `mptRoot`-only divergence):** SMT positional addressing is order-independent at the *structural* level, removing a class of incremental-merge drift. **Caveat:** this only helps if the SMT were the *consensus* root (substrate replacement) — as a parallel primitive it doesn't fix #116, it just demonstrates a structure that wouldn't have it. Do **not** oversell the parallel primitive as a #116 fix.
- **Cross-shard inclusion-proof reads (`SpendActionValidator` / `ShardSubtreeProofClient`, #274/#275):** these are point-read inclusion today; an SMT would let cross-shard *set* / *absence* queries become sound, and (versioned) lets a shard prove "MG X had no entry for key K as of ordinal N." Aligns with the gl1 doc's "cross-shard = explicit proof-carrying txs" direction.
- **Light client (metakit-sdk TS verifier):** an SMT inclusion/absence proof is simpler to re-implement in TS than the hexary MPT witness (binary sibling list + bitmap vs Branch/Extension/Leaf commitment chain). Lower porting cost for the external verifier (memory: adapt metakit-sdk's TS MPT inclusion verifier — an SMT verifier is *less* code than the MPT one).

---

## PART C — Scope + feasibility verdict

### C.1 The key decision: parallel primitive vs substrate replacement

| | **(A) Parallel proof/history primitive (RECOMMENDED)** | **(B) Substrate replacement of the consensus MPT** |
|---|---|---|
| **What** | New `security/smt/` algebra + reference impl, used for historical/absence/cross-shard *proofs* and (optionally) a versioned history store. Consensus MPT untouched. | Replace `MerklePatriciaTrie` as the thing `stateProof.mptRoot` commits to; every node re-derives state into an SMT root. |
| **Consensus impact** | **None** — additive. `mptRoot` unchanged; no fork. | **Total** — changes the consensus state root ⇒ **hard fork**, byte-exact re-derivation across all node types, re-do every prover/verifier, GSAM, sharding, gl1 follow, KES/NIPoPoW fields keyed off field roots. |
| **Cost** | Weeks (algebra + tests + one use-case wiring). Bounded, parallelizable, behind its own package. | Multi-month, consensus-critical, high-coordination; touches the most bug-heavy surface (`MptOverlay`, #54/#70/#113/#115/#116). |
| **Risk** | Low — read-only against existing state; failure is contained to the new feature. | Very high — a root mismatch is a cluster split; the project just spent many iterations stabilizing the MPT/overlay path. |
| **Wins** | Native absence (#286), clean historical proofs (#287 if versioned), easier light-client/cross-shard proofs, future ZK option. | Order-independence fixes #116-class drift; ZK-readiness *if* Poseidon. |
| **When (B) is justified** | — | Only if a stated goal **forces** it: (i) ZK proofs of state become a roadmap item (need Poseidon SMT in the consensus root), or (ii) we decide native absence + order-independence must live in the *live* consensus root, not a sidecar. The survey does **not** show this is forced today. |

**Verdict: pursue (A), the parallel/additive primitive.** It captures the concrete near-term wins (#286 absence; cleaner #287 history; light-client/cross-shard proofs) without the fork-class risk of (B). The greenfield rule (no wire-format back-compat) *lowers* (B)'s cost relative to a legacy chain, but (B) is still gated on a goal the survey doesn't establish. Revisit (B) only if ZK becomes a goal or the requester wants absence/order-independence in the consensus root itself.

### C.2 Concrete slicing if we proceed with the parallel primitive

Mirror the gl1-follow slicing discipline (additive-and-safe first; wiring last). Each slice is independently reviewable; the bug-relevant payoff (absence proofs / historical proofs) lands at S4.

- **S1 — algebra trait (additive, no impl).** `security/smt/`: `SparseMerkleTree[F]`, `SmtProof` (sealed, incl. `Absence`), `SmtProver`/`SmtVerifier`, `SmtRoot`, `SmtProofError` (sealed). Reuse `Hasher[F]` + `MerklePatriciaCommitment`-style prefixed Circe commitments. No wiring. *Defines the shape; compiles; zero runtime effect.*
- **S2 — in-memory reference impl + property tests.** `InMemorySparseMerkleTree` (`Ref[F, ...]`, empty-subtree collapse, structural sharing) mirroring `InMemoryMerklePatriciaProducer`. **Property tests (Weaver + Checkers):** (a) **order-independence** — same key-set inserted in any order ⇒ same root; (b) **inclusion round-trip**; (c) **absence round-trip** — proven-absent key ⇒ verify accepts, then inserting it changes the root; (d) **value-tamper ⇒ `ValueBindingFailed`**; (e) **omitted/extra leaf detectable**. This is the slice that *proves the absence story* (#286).
- **S3 — versioned / historical layer.** `VersionedSmt[F]` with version-prefixed node keys (JMT model): `commit(version, …)`, `rootAt(version)`, `proveAt(version, key)` for inclusion **and** absence. Retention policy (bounded versions, like the NIPoPoW historical-stake retention=4 pattern). Tests: prove `(key, V)` after later commits mutate the key; absence-at-version. *This slice is what makes "prove K=V at finalized ordinal N" first-class — only build it if the use case (C.3) needs per-version history rather than latest-only.*
- **S4 — proof + verify wiring for the actual use case.** Pick **one** consumer (gated on C.3) and wire end-to-end:
  - *if cross-shard absence:* extend `ShardSubtreeProofService` to emit `SmtProof.Absence` for absent keys (replaces today's "return `None`"), verified by `SmtVerifier` against a per-MG SMT root.
  - *if light-client historical:* an HTTP route `proveAt(version, key)` + the metakit-sdk TS verifier counterpart.
  - *if audit/dispute:* an `SmtProof` carried as slashing/dispute tx evidence, verified deterministically by all nodes.
- **S5 — parity / e2e.** Property-parity between SMT roots and an independent recompute; if S4 touched the cross-shard path, a 3gl0+2mg → 8gl0+4mg+4shards e2e to confirm no regression on the existing (untouched) consensus MPT path.

### C.3 OPEN QUESTION for the requester (gates the design)

**The precise historical-proof use case is not yet pinned, and it changes the design materially. Please confirm which of these is the target:**

1. **Light-client historical queries** — "external/TS client proves balance (or any field) at a *past finalized ordinal N*." ⇒ needs the **full versioned layer (S3)** + an HTTP `proveAt` route + TS verifier. Closest to the Aptos model. Full JMT-style versioning justified.
2. **Cross-shard / cross-metagraph historical reads** — "shard B verifies shard A's state as-of a named ordinal, including *absence*." ⇒ needs **absence (S2)** + likely **per-version roots (S3)**; integrates with the gl1 doc's proof-carrying-tx direction (#274/#275). This is the use case most aligned with the in-flight follow work.
3. **Audit / dispute / fraud-proof evidence** — "a tx carries a proof that past state did/didn't contain X, checkable deterministically." ⇒ needs **absence (S2)** + a stable proof encoding as tx evidence; per-version history only if disputes reference arbitrary past ordinals. Aligns with the slashing "verifiable-evidence-in-the-tx" bar.
4. **None of the above / just want native absence now** — ⇒ **S1–S2 only**, no versioned layer; defer S3 until a per-version consumer is real (don't build scaffolding ahead of the blocker — memory rule).

**How the answer changes scope:**
- **Latest-state absence only (4, or 2-point-reads):** a **full JMT is overkill** — S1–S2 (unversioned SMT with native absence) is enough, OR even cheaper: extend the existing MPT verifier with an absence-witness shape (no new tree at all). Recommend SMT only if absence is wanted *uniformly* across many consumers.
- **Arbitrary-ordinal history (1, 2-historical, 3-historical):** the **versioned layer (S3)** is the point — and here a JMT-style versioned SMT is genuinely the right tool, *or* the lighter **history-of-roots index over the existing MPT** (B.2 note) if "prove against a retained past root" suffices and we don't need on-disk per-version nodes for *every* key.

**Recommendation pending the answer:** default to **S1+S2 (algebra + in-memory impl + absence property tests)** regardless — it's the additive, low-risk foundation, directly retires #286's absence gap, and is reusable by any of the four use cases. Hold S3 (versioning) and S4 (wiring) until the requester names the use case, so we don't build a full versioned JMT for a problem that a roots-index or a plain absence-witness solves.

---

## Appendix — source list

- Diem Jellyfish Merkle Tree paper — https://developers.diem.com/papers/jellyfish-merkle-tree/2021-01-14.pdf
- `diem_jellyfish_merkle` (Rust) — https://diem.github.io/diem/diem_jellyfish_merkle/index.html
- Diem authenticated data structures spec — https://github.com/diem/diem/blob/main/specifications/common/authenticated_data_structures.md
- Aptos `aptos-jellyfish-merkle` — https://docs.rs/aptos-jellyfish-merkle/0.1.3/aptos_jellyfish_merkle/
- Aptos light-client feature request (`SparseMerkleProof` @ version + `LedgerInfoWithSignatures`) — https://github.com/aptos-labs/aptos-core/issues/10747
- Penumbra `jmt` (async JMT, Diem-based) — https://github.com/penumbra-zone/jmt
- Penumbra "Integrate Libra's Jellyfish Merkle Tree" (#249) — https://github.com/penumbra-zone/penumbra/issues/249
- Penumbra summer 2022 update (ICS-23 for JMT / IBC) — https://penumbra.zone/blog/2022-summer-update
- `jmt` generalized crate / `jmt-blake3` — https://docs.rs/jmt/latest/jmt/ , https://lib.rs/crates/jmt-blake3
- Olshansky "5P;1R Jellyfish Merkle Tree" — https://olshansky.medium.com/5p-1r-jellyfish-merkle-tree-d2d7dc7dfa8e
- "Merklized Data on disk" (JMT analysis) — https://hackmd.io/@bvrooman/merklized_data_on_disk
- Cosmos ADR-040 (Storage + SMT state commitments) — https://docs.cosmos.network/v0.50/build/architecture/adr-040-storage-and-smt-state-commitments
- Cosmos IAVL overview + proof spec — https://github.com/cosmos/iavl/blob/master/docs/overview.md , https://github.com/cosmos/iavl/blob/master/docs/proof/proof.md
- `celestiaorg/smt` — https://pkg.go.dev/github.com/celestiaorg/smt , https://github.com/celestiaorg/smt
- Dahlberg–Pulls "Efficient Sparse Merkle Trees" (non-membership) — https://eprint.iacr.org/2016/683.pdf
- Haider "Compact Sparse Merkle Trees" (efficient non-membership) — https://eprint.iacr.org/2018/955.pdf
- Mina 22kB technical reference / Merkle docs — https://minaprotocol.com/blog/22kb-sized-blockchain-a-technical-reference , https://docs.minaprotocol.com/zkapps/o1js/merkle-tree
- Polygon zkEVM Sparse Merkle Tree (Poseidon) — https://docs.polygon.technology/zkEVM/concepts/sparse-merkle-trees/sparse-merkle-tree/ , https://docs.polygon.technology/zkEVM/architecture/proving-system/l2statetree-keys-and-values/
- SoK: ZK-friendly hashes — https://cdn.prod.website-files.com/63970f25aa7b42e284492d52/68419a894a80434f16e0f580_sok_zk_friendly_hashes.pdf
- Aztec Indexed Merkle Tree (nullifier non-membership) — https://docs.aztec.network/developers/docs/foundational-topics/advanced/storage/indexed_merkle_tree
- Nomos "Sparse vs Indexed Merkle Trees" — https://blog.nomos.tech/designing-nullifier-sets-for-nomos-zones-sparse-vs-indexed-merkle-trees/
- Ethereum Verkle trees (contrast) — https://ethereum.org/roadmap/verkle-trees , https://notes.ethereum.org/@vbuterin/verkle_tree_eip
- EIP-3102 binary trie (proof-size 4× argument) — https://eips.ethereum.org/EIPS/eip-3102
- Cardano Foundation MPT deep-dive (determinism) — https://cardanofoundation.org/blog/merkle-patricia-tries-deep-dive
