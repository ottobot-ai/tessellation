# Unified `EraRegistry` — ordinal-routed protocol evolution (serde · crypto · schema)

**Status:** research / design, 2026-05-29. Owner: orchestrator. Read-only survey + proposal; **no implementation in this doc**.
**Relation to committed work:** builds directly on the already-landed serde-era scaffold (`serde/era/`, `serde/legacy/`, the `ImmutableCodec`/`Transmittable`/`Persistable` byte-kinds) and the `.workspace/serde-design-notes.md` rationale. Aligns vocabulary with [SMT-HISTORICAL-PROOFS-DESIGN.md](./SMT-HISTORICAL-PROOFS-DESIGN.md) (Part B.1 crypto-layer read) and mirrors the slicing discipline of [GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md](./GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md).

---

## TL;DR

Today there are **four** independent "do X differently before/after ordinal N" mechanisms, three of which the owner named (serde, crypto-hash, schema) plus one more this survey found (state-proof format). They share the same shape — a per-ordinal dispatch — but only one of them (`serde/era/EraCodecRegistry`) has the disciplined, validated, sealed-ADT form; the other three are ad-hoc inline `if (ordinal <= boundary)` checks scattered across wiring code.

**Proposal:** a single `EraRegistry` keyed by `OrdinalRange → Era`, where `Era` bundles the active **serde codec kind**, **crypto hash transform**, and **schema/fields shape** for that ordinal range. It is the `EraCodecRegistry` generalized: keep its range-validation and sealed-era discipline, widen the value from `SerdeEra` to a record of policies, and make every per-ordinal call site (snapshot decode, **MPT/SMT node hashing**, field-presence checks, **disk reads**) consult it.

**First concrete use (already approved):** migrate the MPT/SMT node digest off **Brotli-JSON** onto a hash of scodec `immutable`-bytes. That is a *crypto* era boundary — exactly the kind of typed, validated boundary the registry is built to express, instead of bolting a third `HashLogic` case onto the ad-hoc `HashSelect`. Pre-boundary ordinals keep hashing via the old (Brotli/JSON) transform through the era lookup (the read-only-historical pattern `LegacyBridgeSerde` already establishes for serde). This is consensus-root-changing ⇒ greenfield re-derivation ⇒ full e2e; the registry is what makes the flip clean and reviewable rather than ad-hoc.

---

## PART A — Survey: the four existing per-ordinal mechanisms

All four answer the same question — *"which behaviour is active at snapshot ordinal N?"* — but with four different shapes and four different config keys. The unification target is to collapse them onto one validated registry.

### A.1 Serde era — the disciplined seed (`serde/era/`)

This is the one mechanism that already has the right shape. It is the template the others should fold into.

- **`SerdeEra`** — `modules/shared/src/main/scala/io/constellationnetwork/serde/era/SerdeEra.scala:10-32`. Sealed ADT `Kryo | Json | Scodec` (each with a `name: String` for config/telemetry only — never matched on). The scaladoc is explicit: *"Sealed ADT so additions are reviewed (no stringly-typed eras, no open extension in consumer code). … No other code should pattern-match on `SerdeEra` — all dispatch goes through the registry."* (`SerdeEra.scala:3-8`). `Kryo`/`Json` are read-only-historical; `Scodec` is "the only writable era" and the content-addressing era (`SerdeEra.scala:16-26`).
- **`OrdinalRange`** — `serde/era/OrdinalRange.scala:8-31`. Half-open `[from, until)` with `until: Option[Long]` (open-ended upper bound). `contains`, `overlaps`, and smart constructors `OrdinalRange.from(start)` (open) / `OrdinalRange(from, until)` (closed-open).
- **`EraCodecRegistry`** — `serde/era/EraCodecRegistry.scala:17-91`. Private-constructor wrapper over a sorted `List[(OrdinalRange, SerdeEra)]`:
  - `eraFor(ordinal): Option[SerdeEra]` (`:21-22`) / `eraForOrDie(ordinal): SerdeEra` (`:26-31`).
  - `fromRanges(raw): Either[String, EraCodecRegistry]` (`:45-83`) validates **(1)** non-empty, **(2)** no overlaps, **(3)** contiguous coverage starting at ordinal 0, **(4)** last range open-ended. This is the load-bearing safety property and the part worth preserving verbatim.
  - `defaultScodec` (`:88-90`) — one era `[0,∞) → Scodec`; "useful for tests or for a fresh chain with no historical data." Greenfield default.
- **`LegacyBridgeSerde[T]`** — `serde/legacy/LegacyBridgeSerde.scala:18-28`. Deliberately **no encode method** — *"The write path is locked to the current era (`SerdeEra.Scodec`); historical eras exist to sync from chain tail."* Crucially (`:14-16`): *"As consensus types migrate to scodec (era boundary moves forward), their legacy bridge instances REMAIN registered so re-sync from historical ordinals still works."* This read-only-historical-decoder pattern is the exact template for the crypto-era's "old ordinals still hash the old way" requirement.
- **The byte-KINDS** (the typeclasses the era selects *between*, per type):
  - `ImmutableCodec[T]` — `serde/ImmutableCodec.scala:26-29`. Canonical, frozen-forever bytes; **both the signing input and the content-address input** (`:8-17`: `Hash.fromBytes(ImmutableCodec[T].immutableBytes(t).toArray)` is "the single path for producing a consensus hash"). Built from a scodec `Codec` via `fromScodecCodec`/`derivedFromScodec` (`:36-52`).
  - `Transmittable[T]` — `serde/Transmittable.scala:19-22`. Node-to-node wire bytes; **never hashed, never signed** (`:11-14`), no implicit to `ImmutableCodec`. "Wire codec choice (protobuf vs scodec) is per type" (`:16-17`) — the seam where the protobuf north star plugs in.
  - `Persistable[T]` — `serde/Persistable.scala:20-23`. Disk/MPT-leaf bytes, *may* be compressed, evolves via reindex. Default delegates to `ImmutableCodec` uncompressed (`:31-34`); `BrotliPersistable.fromImmutableCodec` (`serde/storage/BrotliPersistable.scala:34`) is the opt-in compressed variant. **Key invariant** (`Persistable.scala:11-14`, `BrotliPersistable.scala:14-18`): the leaf *hash* is NOT of the persisted (compressed) bytes — content-address is always `ImmutableCodec.immutableBytes`. Compression is "a storage optimisation, never consensus-critical."
  - Package doc `serde/package.scala:3-22`: "Three typeclasses, one per purpose (do NOT mix). … Typeclass layer is intentionally not a subtype hierarchy" — the compiler refuses to pass wire bytes to a signing function. This is the "byte-KINDS where the kind names how the bytestring is used" the owner described.
- **`SerdeError.NoEraCodec`** — `serde/SerdeError.scala:32-35`: *"No codec era registered for type '$typeName' at ordinal $ordinalValue — check hash-eras config."* (sealed ADT, never string-matched, `:5,11`).

> **Status nuance (important):** `EraCodecRegistry` is built and tested (`serde/SerdeShimSuite.scala` exercises `fromRanges`/`OrdinalRange`) but is **not yet wired into production dispatch**. `fromRanges`/`defaultScodec` are referenced only inside the era package and its tests. The *live* serde/crypto dispatch in production is still A.2's ad-hoc `HashSelect`. So the registry is genuinely a **seed/scaffold awaiting adoption** — unification is "grow the seed into the load-bearing path," not "rewrite a working system."

### A.2 Crypto era — ad-hoc `HashSelect` (NOT the registry; owner flagged this)

This is the mechanism the owner explicitly said is *not* what they mean by an EraRegistry — it is the imperative `if`-on-ordinal that should move onto the registry.

- **`HashLogic`** — `modules/shared/src/main/scala/io/constellationnetwork/security/Hasher.scala:15-17`. Sealed `JsonHash | KryoHash`.
- **`HashSelect`** — `Hasher.scala:19-21`: `def select(ordinal: SnapshotOrdinal): HashLogic`. The only production impl is **inline** in wiring: `TessellationIOApp.scala:135-138`:
  ```scala
  val _hashSelect = new HashSelect {
    def select(ordinal: SnapshotOrdinal): HashLogic =
      if (ordinal <= cfg.lastKryoHashOrdinal.getOrElse(cfg.environment, SnapshotOrdinal.MinValue)) KryoHash else JsonHash
  }
  ```
- **`HasherSelector[F]`** — `Hasher.scala:31-60`. `forSync(hasherJson, hasherKryo, hashSelect)` (`:46-54`) returns `getForOrdinal(ordinal)` = `hashSelect.select(ordinal) match { JsonHash ⇒ hasherJson; KryoHash ⇒ hasherKryo }`. Also `getCurrent`/`withCurrent` (always `hasherJson`) and `forOrdinal`/`alwaysCurrent`. Wired at `TessellationIOApp.scala:167`: `HasherSelector.forSync[IO](Hasher.forJson, Hasher.forKryo, _hashSelect)`.
- **`Hasher.forJson`** — `Hasher.scala:108-133`. `hash`/`prefixedHash` route through `JsonSerializer[F]`. And **`JsonSerializer` IS Brotli-compressed JSON**: `json/JsonSerializer.scala:16-27` builds it from `JsonBrotliBinarySerializer.forAsync` (a Circe `Printer(dropNullValues, sortKeys)` piped through `BrotliOutputStream`, `json/JsonBrotliBinarySerializer.scala:37-53,62-65`). So `forJson.prefixedHash(data, prefix) = Hash.fromBytes(prefix ++ brotli(json(data)))`.

**The gap, precisely (the thing slice 1 fixes).** The MPT node digest is computed in `security/mpt/MerklePatriciaNode.scala` for every node type:
- `Leaf.apply` — `MerklePatriciaNode.scala:92-96`: `Hasher[F].prefixedHash(commitment.asJson, LeafPrefix)`.
- `Branch.apply` — `:131-136`: `Hasher[F].prefixedHash(commitment.asJson, BranchPrefix)`.
- `Extension.apply` — `:174-178`: `Hasher[F].prefixedHash(commitment.asJson, ExtensionPrefix)`.

The `commitment` is a `MerklePatriciaCommitment` (`security/mpt/MerklePatriciaCommitment.scala:8-13`) serialized via **Circe `.asJson`**. The `Hasher[F]` instance at the MPT call sites is the ordinal-selected one — at acceptance, `GlobalSnapshotAcceptanceManager.scala:1258`: `implicit val hasher: Hasher[F] = HasherSelector[F].getForOrdinal(ordinal)`. Since the live `HashSelect` only ever returns `JsonHash` for current ordinals (Kryo is purely pre-mainnet-cutover history), **the MPT node hash is `Blake2b(prefix ++ brotli(json(commitment)))`** — Brotli-JSON, for current ordinals.

This was **never migrated to scodec** even though the serde *write path* is already `SerdeEra.Scodec` (MPT *leaf values* go through `ImmutableCodec`/scodec via `MptStore`, but the *node digest* still hashes a Brotli-JSON commitment). The SMT design doc states the same finding independently (B.1, `MerklePatriciaNode.scala` line): *"Hash = Blake2b through `Hasher[F]`; commitments are Circe-`asJson`-encoded (not a ZK hash, not scodec at the node level)."* The crypto era today has **no `ScodecHash` case** — `HashLogic` is `JsonHash | KryoHash` only.

### A.3 Schema era — `Era` / `FieldsAddedOrdinals` (`config/types.scala`)

- **`FieldsAddedOrdinals`** — `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala:27-35`. A record of `Map[AppEnvironment, SnapshotOrdinal]` gates, one per surviving schema migration (`tessellation3Migration`, `tessellation301Migration`, `metagraphSyncData`, `updatingCombineFunctionSpendActions`, `setSumFix`, …). Populated from HOCON `fields-added-ordinals { … }` (`application.conf:165-214`), per-env.
- **`Era`** — `types.scala:41-59`. *"Typed predicate over per-environment migration gates. Resolves a `FieldsAddedOrdinals` field for the active `AppEnvironment` once at construction, then collapses repeated `ordinal < <gate>StartingOrdinal` checks into named methods."* Methods: `atOrAfterTess3` / `beforeTess3` / `atOrAfterTess301` / `atOrAfterMetagraphSync`, and the field-shaping combinators `postTess3[A](ordinal)(value): Option[A]` etc. (`:46-58`) — "the shape behind the 12 nullable post-tess3 fields in the GSI."
- **`Era.fromConfig(env, fieldsAddedOrdinals)`** — `types.scala:61-67`. Resolves the per-env ordinals once. Consumed at e.g. `GlobalSnapshotAcceptanceManager.scala:1260`: `val era = Era.fromConfig(environment, fieldsAddedOrdinals)`.

This is *already* a per-ordinal "what shape is the data at N" — i.e. a schema era. It is just keyed by named boolean predicates rather than a range list, and is a separate type from `SerdeEra`.

### A.4 State-proof-format era — `StateProofSelector` (the fourth, un-named mechanism)

Found during survey; the owner named three but this is the same pattern again and belongs in the inventory.

- **`SnapshotFormat`** — `modules/shared/src/main/scala/io/constellationnetwork/schema/StateProofSelector.scala:8-10`. Sealed `LegacyFormat | MerklePatriciaFormat` — i.e. "16 individual hash fields" vs "single MPT root" (`:3-6`).
- **`StateProofSelector.select(ordinal): SnapshotFormat`** — `:17-19`. `GlobalStateProofSelector(lastLegacyStateProofOrdinal)` does `if (ordinal <= boundary) LegacyFormat else MerklePatriciaFormat` (`:22-25`); `CurrencyStateProofSelector` is always `LegacyFormat` (`:33-34`). Wired inline in `TessellationIOApp.scala:140-144` from `cfg.lastLegacyStateProofOrdinal`.
- The doc comment even notes *why* it is separate from A.2: *"used to decouple the snapshot format selection from the hash scheme (Kryo vs JSON), since these transitions happened at different ordinals on mainnet"* (`:12-16`). **This is the single best piece of evidence for the open question of independent vs shared boundaries** (§Open Questions Q2): the codebase already needed crypto-hash and schema-format boundaries to move at *different* ordinals.

### A.5 Disk-read routing — the owner's specific concern (`SnapshotDownloadStorage`)

When reading a snapshot persisted at a historical ordinal, the code already branches on the per-ordinal era to pick the right *backend + format*:
- `SnapshotDownloadStorage.scala:77-84` (`hasCorrectSnapshotInfo`): `hashSelect.select(ordinal) match { JsonHash ⇒ snapshotInfoStorage.read(ordinal)…; KryoHash ⇒ snapshotInfoKryoStorage.read(ordinal)… }` — two *different storage objects*, chosen by ordinal.
- `SnapshotDownloadStorage.scala:97-100` (`readCombined`): same branch picks `GlobalSnapshotInfo` (JSON era) vs `GlobalSnapshotInfoV2` (Kryo era) as the decoded type.

So "which codec/hash to use when reading a file persisted at ordinal N" is **already** an ad-hoc per-ordinal switch — exactly what the owner wants the registry to own.

### A.6 Inventory summary

| Mechanism | Dispatch type | Selector | Config key | Shape today | Where wired |
|---|---|---|---|---|---|
| **Serde codec** | `SerdeEra` (sealed: Kryo/Json/Scodec) | `EraCodecRegistry.eraFor` | `hash-eras` (planned) | **Range-list + validated** ✅ | *scaffold; not yet live* |
| **Crypto hash** | `HashLogic` (sealed: Json/Kryo) | `HashSelect.select` | `last-kryo-hash-ordinal` | inline `if (ord ≤ b)` ❌ | `TessellationIOApp:135`, `HasherSelector.forSync:46` |
| **Schema fields** | `Era` (named predicates) | `Era.atOrAfter*` | `fields-added-ordinals.*` | per-gate booleans ❌ | `Era.fromConfig`, GSAM:1260 |
| **State-proof format** | `SnapshotFormat` (sealed: Legacy/Mpt) | `StateProofSelector.select` | `last-legacy-state-proof-ordinal` | inline `if (ord ≤ b)` ❌ | `TessellationIOApp:140` |
| **Disk read** | (derived from the above) | `hashSelect.select` branch | (reuses crypto key) | inline backend switch ❌ | `SnapshotDownloadStorage:77,97` |

Four selectors, four config keys, three different value-shapes, one of them validated. The proposal unifies the *dispatch*, not necessarily the *boundaries* (see Q2).

---

## PART B — Proposal: the unified `EraRegistry`

### B.1 Core shape

Generalize `EraCodecRegistry` from `List[(OrdinalRange, SerdeEra)]` to `List[(OrdinalRange, Era)]`, where `Era` is a sealed record bundling the active policy for each evolution axis. Keep the constructor validation verbatim.

```scala
package io.constellationnetwork.era   // promoted out of serde/era (it now governs crypto + schema too)

/** The bundle of protocol-evolution policies active for one ordinal range.
  *
  * Each axis is a SEALED ADT — no string-keyed control flow, no open extension in consumers
  * (the SerdeEra discipline, widened). Adding an axis or a case is a reviewed change here +
  * a registry-loader change + an application.conf range; NOTHING else pattern-matches on these.
  */
final case class Era(
  serde:  SerdeKind,    // which codec family writes/reads canonical bytes        (was: SerdeEra)
  crypto: HashKind,     // which transform produces consensus digests (MPT/SMT node hash, content-address)
  schema: SchemaShape   // which field-shape / format the snapshot carries         (folds in config.types.Era + SnapshotFormat)
) {
  def name: String = s"${serde.name}/${crypto.name}/${schema.name}"
}

/** Serde axis — the existing SerdeEra, renamed for symmetry. Kryo/Json read-only-historical; Scodec writable. */
sealed trait SerdeKind  extends Product with Serializable { def name: String }
object SerdeKind { case object Kryo extends … ; case object Json extends … ; case object Scodec extends … }

/** Crypto axis — the NEW thing. Replaces the ad-hoc HashLogic(JsonHash|KryoHash). */
sealed trait HashKind extends Product with Serializable { def name: String }
object HashKind {
  case object KryoHash    extends HashKind { val name = "kryo-hash"    }  // historical
  case object JsonHash    extends HashKind { val name = "json-hash"    }  // historical = Blake2b(prefix ++ brotli(json(commitment)))
  case object ScodecHash  extends HashKind { val name = "scodec-hash"  }  // NEW = Blake2b(prefix ++ ImmutableCodec[Commitment].immutableBytes)
}

/** Schema axis — subsumes config.types.Era predicates AND StateProofSelector's SnapshotFormat. */
sealed trait SchemaShape extends Product with Serializable { def name: String /* + field-presence predicates */ }
```

The registry itself is `EraCodecRegistry` with the value type widened. Everything else — `OrdinalRange`, `fromRanges` with its overlap/coverage/open-ended validation, `eraFor`/`eraForOrDie`, `defaultScodec` — carries over unchanged in spirit:

```scala
final class EraRegistry private (ranges: List[(OrdinalRange, Era)]) {
  def eraFor(ordinal: Long): Option[Era]
  def eraForOrDie(ordinal: Long): Era                 // throws iff coverage broken (validated at startup)
  def serdeAt(ordinal: SnapshotOrdinal):  SerdeKind   = eraForOrDie(ordinal.value.value).serde
  def hashKindAt(ordinal: SnapshotOrdinal): HashKind  = eraForOrDie(ordinal.value.value).crypto
  def schemaAt(ordinal: SnapshotOrdinal):  SchemaShape = eraForOrDie(ordinal.value.value).schema
}
object EraRegistry {
  def fromRanges(raw: List[(OrdinalRange, Era)]): Either[String, EraRegistry]   // SAME validation as EraCodecRegistry.fromRanges
  val defaultScodec: EraRegistry                                                 // [0,∞) → Era(Scodec, ScodecHash, Latest)
}
```

### B.2 How it subsumes the four mechanisms (without rewrite-for-its-own-sake)

The point of the survey's "the seed already exists, but only one of four has the shape" framing: we are **growing `EraCodecRegistry`**, not reinventing anything.

1. **Serde (A.1):** `EraCodecRegistry` *is* the seed. `SerdeEra → SerdeKind` is a rename; `eraFor` becomes `serdeAt`. The validated `fromRanges` is the kernel everything else reuses. `LegacyBridgeSerde`'s read-only-historical contract is preserved (the registry tells you *which* era's bytes you're reading; the bridge decodes them; the write path stays locked to the current era).
2. **Crypto (A.2):** the ad-hoc `HashSelect`/`HashLogic` is **deleted** in favour of `registry.hashKindAt(ordinal)`. `HasherSelector.forSync(forJson, forKryo, hashSelect)` becomes `forEra(registry, Map(HashKind → Hasher))` — i.e. the selector consults the registry and dispatches to the matching `Hasher[F]`. The MPT call sites (`MerklePatriciaNode.{Leaf,Branch,Extension}.apply`) are unchanged in *signature* — they still take `Hasher[F]` implicitly — but the implicit they receive at `GlobalSnapshotAcceptanceManager:1258` comes from `registry.hashKindAt(ordinal)` instead of `HasherSelector.getForOrdinal`. **All consensus-bytes hashing still flows through `Hasher[F]`** (memory hard-rule) — we are only changing *which* `Hasher[F]` the era hands back, and adding a third (`ScodecHash`) implementation.
3. **Schema (A.3):** `config.types.Era` is *already* a per-ordinal predicate object; it folds in as the `SchemaShape` axis. `Era.fromConfig(env, fieldsAddedOrdinals)` becomes the *loader* that builds the `SchemaShape` cases that get attached to ranges. The named predicates (`atOrAfterTess3`, `postTess3`, …) become methods on the resolved `SchemaShape` (or stay as a thin facade over `registry.schemaAt(ordinal)` to avoid churning ~dozens of call sites in one go — see slicing).
4. **State-proof format (A.4):** `SnapshotFormat` (`LegacyFormat | MerklePatriciaFormat`) is another facet of `SchemaShape` (or a sibling sealed field on it). `StateProofSelector.select` becomes `registry.schemaAt(ordinal).stateProofFormat`.
5. **Disk reads (A.5):** `SnapshotDownloadStorage`'s `hashSelect.select(ordinal) match { … }` becomes `registry.eraForOrDie(ordinal)` and branches on the bundled axes — pick the storage backend by `serde`/`schema`, decode/derive with the `crypto` transform. This is the owner's specific concern and falls out for free once the registry is the single source.

The net code-shape win: **one validated range list, one sealed `Era`, one lookup**, replacing four selectors and four `if (ordinal ≤ boundary)` sites.

### B.3 Disk-read routing in detail (owner's specific concern)

Reading a snapshot/MPT node persisted at historical ordinal N is a three-question lookup, all answered by `registry.eraForOrDie(N)`:

1. **Which on-disk codec decodes the file?** `era.serde` → pick `LegacyBridgeSerde` (Kryo/Json) vs current `ImmutableCodec`/`Persistable` (Scodec). For snapshot *info* this is exactly the `snapshotInfoKryoStorage` vs `snapshotInfoStorage` choice at `SnapshotDownloadStorage:97-100`.
2. **Which value-shape did it have?** `era.schema` → `GlobalSnapshotInfoV2` (legacy) vs `GlobalSnapshotInfo` (MPT), and the field-presence predicates for nullable post-migration fields.
3. **Which transform re-derives / re-checks its digest?** `era.crypto` → for an MPT node persisted under `JsonHash`, re-derive the node digest with the Brotli-JSON transform; under `ScodecHash`, with `ImmutableCodec`-bytes. This is the part that makes the Brotli→scodec migration *safe on read*: a node syncing history past the boundary derives pre-boundary node hashes the old way and post-boundary the new way, purely from the ordinal — no in-band format flag needed.

Because reads consult the *same* registry as writes, "what was written at N" and "how to read N" cannot drift — the bug class the `serde-design-notes.md` §1.3 calls out ("tweaking the API response shape becomes a consensus change") stays closed, and the historical-decode discipline of `LegacyBridgeSerde` extends uniformly to crypto and schema.

### B.4 The protobuf-versioned-schema-kinds north star (what the registry *enables*, not v1)

`serde-design-notes.md` (§3, §8) and the `Transmittable` doc (`Transmittable.scala:16-17`) already set the direction: **protobuf is the external/wire interface (`WireSerde`/`Transmittable`); scodec `ImmutableCodec` is the signing+hashing interface.** The owner's framing — "protobuf-ecosystem VERSIONED schema defs ↔ scodec byte-KINDS (immutable/transmittable/…) where the kind names how the bytestring is used" — maps onto the existing typeclass split:

- **byte-KINDS** = `ImmutableCodec` (hash/sign), `Transmittable` (wire), `Persistable` (disk) — already built, already "named by use" (`serde/package.scala:3-22`).
- **versioned schema defs** = `.proto` files governed by `buf` breaking-change CI (`serde-design-notes.md` §8.2), generating `Transmittable` instances via scalapb for the wire axis.
- **The bridge between them** is exactly the discipline already enforced: no implicit converts `Transmittable` → `ImmutableCodec`; a node decodes a protobuf wire payload then re-encodes with `ImmutableCodec` before hashing/verifying (`serde-design-notes.md` §2.2). Protobuf's non-canonicality is fine *because it never touches the hash path*.

**Where the EraRegistry is the enabler:** versioned schemas evolve per ordinal too (a `.proto` v2 adds a field at boundary N). The `SchemaShape` axis is where "which message version is canonical at N" lives, and `era.serde = Scodec` guarantees the *hashed* bytes are the scodec `immutable` encoding of whichever schema-version `era.schema` selects. **External-verifier reproducibility for free:** because the hashed bytes are `ImmutableCodec.immutableBytes` (a deterministic scodec encoding of a `.proto`-versioned message), metakit's Python/JS/Go/Rust/Solidity verifiers can reproduce the exact hashed bytes from the published `.proto` schema + the scodec layout for that era — they don't need to reimplement Brotli-JSON canonicalization (which is the current barrier; Brotli + Circe `dropNullValues`/`sortKeys` is JVM-specific and effectively un-portable). This is the deepest reason the Brotli→scodec hash migration (slice 1) matters beyond cleanliness: **it is the precondition for cross-language proof verification**, and the registry is what lets the boundary be introduced without a flag-day.

Position: **north star, not v1.** v1 is slice 1 (crypto boundary). The protobuf/`Transmittable` wire axis and `buf` governance land later, independently, on the same registry.

### B.5 Constraints honored

- **Typed sealed eras, no stringly-typed control flow** — every axis (`SerdeKind`, `HashKind`, `SchemaShape`) is a sealed ADT; `name: String` exists only for config/telemetry, never matched (the `SerdeEra.scala:3-8` rule, widened). The four ad-hoc `if (ordinal ≤ boundary)` sites are deleted.
- **Greenfield, no wire back-compat** — the registry governs *on-disk* + *chain-store* readability (the only back-compat that matters per the greenfield rule). It introduces no network protocol versioning ceremony; `defaultScodec` is the fresh-chain default (`EraCodecRegistry.scala:88`).
- **Build on `EraCodecRegistry`, don't reinvent** — the proposal is literally "widen its value type and promote it"; `fromRanges` validation is preserved verbatim.
- **Route ALL consensus-bytes hashing through `Hasher[F]`** — the crypto axis only changes *which* `Hasher[F]` the era yields; MPT/SMT node hashing keeps calling `Hasher[F].prefixedHash(commitment, prefix)` with a Circe-encoded (slice 1: scodec-encoded) commitment case class. No hand-rolled Blake2b.

---

## PART C — Slicing

Mirror the gl1/SMT discipline: **additive-and-safe first; consensus-flip last; each slice independently reviewable.** The consensus-root-changing flip (the Brotli→scodec hash) is isolated to its own slice behind a registry boundary so it is reviewable and e2e-gated on its own.

- **S1 — promote + widen the registry (additive, zero runtime effect).** Move `serde/era/` → `era/`; widen `EraCodecRegistry` value `SerdeEra → Era(serde, crypto, schema)`; add `HashKind` (incl. `ScodecHash`) and `SchemaShape` sealed ADTs; keep `fromRanges` validation verbatim; `defaultScodec = Era(Scodec, ScodecHash, Latest)` *but do not wire it yet*. Compiles; nothing consults it. **No behavior change.**
- **S2 — adopt the registry for the *existing* crypto/serde/schema selectors (behavior-preserving).** Replace the inline `HashSelect` (`TessellationIOApp:135`), `StateProofSelector` (`:140`), and route `config.types.Era` through `registry.schemaAt`. Construct the registry from the *current* config keys (`last-kryo-hash-ordinal`, `last-legacy-state-proof-ordinal`, `fields-added-ordinals`) so the live boundaries are **identical** to today — this is a pure refactor, validated by the existing e2e passing unchanged. `HasherSelector.forEra(registry, …)` replaces `forSync(…, hashSelect)`. **No consensus change; the registry just becomes the single source of dispatch.** This is the slice that retires the four ad-hoc sites.
- **S3 — add the `ScodecHash` `Hasher[F]` + commitment scodec codec (additive, still not active).** Implement `Hasher.forScodec` whose `prefixedHash(commitment, prefix) = Hash.fromBytes(prefix ++ ImmutableCodec[MerklePatriciaCommitment].immutableBytes(commitment))` (route through `Hash.fromBytesForSync`, `Hash.scala:54-57`). Add the hand-written scodec `Codec[MerklePatriciaCommitment]` (sealed Leaf/Branch/Extension, deterministic; the `JsonScodecParitySuite` pattern — property round-trip + canonicality, `serde-design-notes.md` §5.1). Register `HashKind.ScodecHash → Hasher.forScodec` in the selector map. **Still inactive** (no era range selects it yet). Unit + property tests only.
- **S4 — the consensus flip (Brotli→scodec MPT/SMT node hash), behind a boundary ordinal.** Add an era boundary at ordinal `B` where `crypto` switches `JsonHash → ScodecHash`. For dev/greenfield, `B = 0` (`defaultScodec` becomes the real default). Pre-`B` ordinals still hash via the Brotli-JSON transform through the era lookup (the `LegacyBridgeSerde` read-only-historical pattern, applied to crypto). **This is consensus-root-changing** ⇒ greenfield re-derivation ⇒ requires the full 3gl0+2mg → 8gl0+4mg+4shards e2e. Isolated here so the review is "does the boundary flip correctly and do historical reads still derive the old way," nothing else.
- **S5 — disk-read unification + parity.** Route `SnapshotDownloadStorage`'s backend/format/derive switches through `registry.eraForOrDie(ordinal)` (B.3). Parity test: read a corpus of pre-`B` (Brotli-JSON) and post-`B` (scodec) persisted snapshots/MPT nodes and assert each derives its stored digest under the era-selected transform. e2e confirms history-sync across the boundary.
- **S6+ (north star, deferred — do NOT build ahead of the blocker):** `.proto`-versioned `Transmittable` wire axis + `buf` governance + scalapb generation (`serde-design-notes.md` §8); metakit cross-language verifier reproducing scodec-`immutable` hashed bytes from the published schema. Lands on the same registry; gated on the wire/ecosystem workstream being prioritized.

Bug-relevant payoff (portable, cross-language-verifiable consensus hash) lands at S4–S5. S1–S3 are all additive and independently mergeable.

---

## PART D — Open questions (for the owner)

1. **Boundary-ordinal selection for S4.** Greenfield/dev is `B = 0` (everything scodec-hashed from genesis), which is the clean case. Is there *any* historical chain state that must remain readable under the Brotli-JSON node hash, or is this a fresh-chain cutover where the legacy `HashKind.JsonHash`/`KryoHash` cases exist only as a safety net and could even be dropped? (`serde-design-notes.md` Q5 asks the parallel question for serde: "does `HashLogic` need three variants or replace with scodec outright?")

2. **Do the three axes share one boundary, or move independently?** This is the load-bearing design question. **Evidence says independent:** `StateProofSelector`'s own doc (`StateProofSelector.scala:12-16`) exists *specifically because* the hash-scheme and state-proof-format transitions "happened at different ordinals on mainnet," and `last-kryo-hash-ordinal` ≠ `last-legacy-state-proof-ordinal` in `application.conf` (e.g. mainnet 2,572,384 vs 5,960,000). So `Era` should bundle three *independently-set* axes — the registry is a list of ranges where each range carries a full `Era`, and a "boundary" is wherever *any* axis changes. (Implication: the range list is the *join* of all three axes' breakpoints; validation stays per-the-whole-`Era`.) Confirm this is the intended model vs a single shared boundary for new (post-greenfield) eras.

3. **Retention of legacy decoders.** `LegacyBridgeSerde` says historical bridges "REMAIN registered as the era boundary moves forward" (`LegacyBridgeSerde.scala:14-16`). For crypto, do we keep `Hasher.forJson`/`forKryo` (and the Brotli/Circe deps) registered forever for history-sync, or is there a retention horizon (à la the NIPoPoW historical-stake retention=4) past which old-era ordinals are no longer independently re-derivable and we rely on checkpoint trust instead? Affects whether `chill`/brotli4j can ever be dropped (`serde-design-notes.md` §7.2 step 6).

4. **Test-vector / golden-file strategy across eras.** `serde-design-notes.md` §5.1 mandates golden files ("never regenerated — the historical-compatibility canary") and `JsonScodecParitySuite` already validates scodec vs real Brotli-JSON snapshots. For the unified registry: (a) one golden corpus *per era* (pre-`B` Brotli-JSON node-hash vectors + post-`B` scodec node-hash vectors)? (b) Should the cross-language reproducibility test (metakit) be a gating golden-file consumer from S4, or deferred to S6? (c) Who publishes the era-tagged vector library for external integrators (`serde-design-notes.md` Q7)?

5. **`config.types.Era` migration depth in S2.** `Era`'s named predicates (`atOrAfterTess3`, `postTess3`, …) have ~dozens of call sites. Fold them into `SchemaShape` methods directly (bigger S2 diff, fully unified), or keep `config.types.Era` as a thin facade over `registry.schemaAt(ordinal)` (smaller diff, two names for one concept transitionally)? Recommend the facade for S2, full fold in a later cleanup slice — but confirm the owner's appetite for churn.

6. **Package home + naming.** Promote to a top-level `io.constellationnetwork.era` (governs crypto + schema, not just serde), or keep under `serde/era` and accept the slightly-off package name? And `HashKind` case naming: `ScodecHash` parallels the existing `JsonHash`/`KryoHash`, but the owner's "byte-KINDS" language suggests `ImmutableHash` (named by the *kind* of bytes it hashes) — which reads better against `ImmutableCodec`?

---

## Appendix — source file:line references

**Serde era (seed):**
- `modules/shared/src/main/scala/io/constellationnetwork/serde/era/SerdeEra.scala:3-32`
- `modules/shared/src/main/scala/io/constellationnetwork/serde/era/EraCodecRegistry.scala:17-91` (validation `:45-83`, `defaultScodec` `:88-90`)
- `modules/shared/src/main/scala/io/constellationnetwork/serde/era/OrdinalRange.scala:8-31`
- `modules/shared/src/main/scala/io/constellationnetwork/serde/legacy/LegacyBridgeSerde.scala:14-28`
- `modules/shared/src/main/scala/io/constellationnetwork/serde/{ImmutableCodec,Transmittable,Persistable}.scala`, `serde/storage/BrotliPersistable.scala:24-66`, `serde/package.scala:3-29`, `serde/SerdeError.scala:11-44`

**Crypto era (ad-hoc):**
- `modules/shared/src/main/scala/io/constellationnetwork/security/Hasher.scala:15-21` (HashLogic/HashSelect), `:31-60` (HasherSelector), `:108-133` (forJson)
- `modules/shared/src/main/scala/io/constellationnetwork/json/JsonSerializer.scala:16-27`, `json/JsonBrotliBinarySerializer.scala:37-65`
- `modules/shared/src/main/scala/io/constellationnetwork/security/mpt/MerklePatriciaNode.scala:92-96,131-136,174-178` (the `prefixedHash(commitment.asJson, prefix)` gap)
- `modules/shared/src/main/scala/io/constellationnetwork/security/mpt/MerklePatriciaCommitment.scala:8-13`
- `modules/shared/src/main/scala/io/constellationnetwork/security/hash/Hash.scala:54-60` (`fromBytes`/`fromBytesForSync`/`empty`)
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/app/TessellationIOApp.scala:135-138,167` (live HashSelect wiring)
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala:1258,1260` (ordinal-selected hasher + Era at MPT accept)

**Schema era:**
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala:27-35` (FieldsAddedOrdinals), `:41-67` (Era / fromConfig)
- `modules/node-shared/src/main/resources/application.conf:165-214` (`fields-added-ordinals`), `:57-69` (`last-kryo-hash-ordinal`, `last-legacy-state-proof-ordinal`)

**State-proof-format era (the fourth mechanism):**
- `modules/shared/src/main/scala/io/constellationnetwork/schema/StateProofSelector.scala:8-39` (esp. `:12-16` — why it is separate from the hash scheme)

**Disk reads:**
- `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/SnapshotDownloadStorage.scala:77-84,97-100`

**Foundational rationale + aligned docs:**
- `.workspace/serde-design-notes.md` (typeclass-per-purpose; §2.2 the load-bearing move; §3 codec choice; §4.3 era-keyed dispatch precedent; §7 kryo retirement; §8 protobuf rollout; §10 open questions)
- `docs/nakamoto/SMT-HISTORICAL-PROOFS-DESIGN.md` (Part B.1 — independent confirmation of the MPT-node-hash crypto situation; `Hasher[F]` + `FollowVerifyCore` idiom)
- `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md` (slicing discipline; `Verified[A]`/`FollowVerifyCore` contract bar)
