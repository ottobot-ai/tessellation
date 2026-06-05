# BLS Aggregate Signatures + Unified Validator-Key Registry — Design

**Status:** DRAFT — awaiting ratification. **Date:** 2026-06-02.

## 1. Goal & scope

Introduce BLS12-381 aggregate signatures so that the N individual validator
signatures over a single consensus artifact collapse into **one** signature
verified in **one** operation (`FastAggregateVerify` = 2 pairings regardless of
N). First target: **global / currency snapshot certificates**.

This requires a small enabling refactor: **generalize the per-scheme key
registries (KES, VRF) into one rotatable validator-key registry** that also
carries the new BLS verification key. That generalization is something the
codebase already trends toward — `VrfRegistry` declares its intent to ride the
KES registration cert for runtime rotation but hasn't been wired yet (it is
still genesis-frozen). Adding BLS is the moment to complete it.

Out of scope for this doc: TipAttestation gossip aggregation and committee /
shard-checkpoint attestation aggregation (later targets; same primitives).

## 2. Dependency — PROVEN (spike 2026-06-02)

BouncyCastle **1.85** ships native BLS12-381 in `org.bouncycastle.crypto.bls`.
An isolated worktree + ephemeral-docker spike (JDK 21 Temurin) confirmed:

- Same-message aggregate **sign → aggregate → FastAggregateVerify** round-trips
  (positive TRUE, tampered negative FALSE).
- A canonical **Eth2 KAT matches py_ecc byte-for-byte** — BC is on the standard
  ciphersuite `BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_`, G2 sigs / G1 pubkeys,
  ZCash point compression (48B G1 / 96B G2).

**Exact API** (static scheme classes — note: not the names we first assumed):

| Need | Call |
|------|------|
| sk from IKM | `BLS12_381BasicScheme.keyGen(byte[] ikm≥32, byte[] keyInfo): BigInteger` |
| sk → pubkey (G1) | `BLS12_381BasicScheme.skToPk(BigInteger): ECPoint` |
| validate pubkey | `BLS12_381BasicScheme.keyValidate(ECPoint): boolean` |
| sign (PoP scheme) | `BLS12_381ProofOfPossession.sign(BigInteger, byte[]): BLS12_381G2Point` |
| same-message agg verify | `BLS12_381ProofOfPossession.fastAggregateVerify(ECPoint[] pks, byte[] msg, BLS12_381G2Point aggSig): boolean` |
| proof-of-possession | `popProve(BigInteger): BLS12_381G2Point` / `popVerify(ECPoint, BLS12_381G2Point): boolean` |
| aggregate sigs | `BLS12_381Aggregation.aggregate(BLS12_381G2Point[]): BLS12_381G2Point` |
| wire bytes | `BLS12_381Serialization.compressG1/decompressG1/compressG2/decompressG2` |

Types to thread: secret = `java.math.BigInteger`; pubkey = `ECPoint` (G1, 48B);
signature = `BLS12_381G2Point` (G2, 96B).

**Production dependency path:** repo pins BC **1.83** (`project/Dependencies.scala:6`,
`bcprov-jdk18on` + `bcpkix-jdk18on`). BLS lands in **1.85**, currently a
`-SNAPSHOT` beta available only as raw jars (not Maven-resolvable). So:
production waits for **1.85 stable** → one-line `V.bouncyCastle = "1.85"` bump,
no new dependency/resolver/JNI. The spike vendors the beta jars in `lib/` for
worktree-local development only.

## 3. Current state (verified)

- **`Signed[A]`** (`shared/.../security/signature/Signed.scala:33`) =
  `(value, proofs: NonEmptySet[SignatureProof])`. Verification is a **per-proof
  traverse** (`Signed.scala:126`) — the loop BLS collapses. `SignatureProof(id, signature)`,
  `Id` = secp256k1-derived `PeerId`.
- **KES** — mature template: genesis (`L0GenesisData.kesRegistrations`) +
  runtime rotation via `KesRegistrationCert` (`shared/.../schema/kes/KesRegistrationCert.scala`)
  + `MutableKesRegistry` MPT overlay (N-2 staggering, ordinal replay-protection,
  reorg-aware). KES master keys are **independently generated** (not derived) —
  forward security comes from the SK lifecycle in `OperationalKeyMaker`.
- **VRF** — `VrfRegistry` is **genesis-frozen only** (`L0GenesisData.operators[].vrfPublicKey`),
  VK **derived from the secp256k1 identity** via `VrfKeyDeriver.deriveVrfKeyPair`
  (SHA-512 domain-tag `tessellation-vrf-v1` → Ed25519 seed). Runtime rotation
  intended (`KesRegistrationCert.vrfVK`) but **not implemented**.
- **BLS** — does not exist.

## 4. Decisions (locked with user)

1. **Derive the genesis BLS key from the secp256k1 identity**, mirroring VRF —
   bootstrap convenience for the genesis set. Independent keys arrive later via
   rotation.
2. **Generalize into one rotatable validator-key registry** (KES + VRF + BLS).
   Rotation is the reason to generalize now rather than add a third parallel store.
3. **PoP (proof-of-possession) scheme** for rogue-key safety + cheap
   same-message verify. PoP is generated at registration and verified on load.
4. **Never derive KES from identity** — keep its independent generation
   (preserves forward security). Derivation-from-identity applies to VRF and BLS only.
   *Consequence:* KES is forward-secure from genesis (t0) with **no bootstrap
   window**; only VRF/BLS carry an identity-derived bootstrap key, which rotation
   lets operators replace.
5. **Post-genesis rotation confirmed** for all three keys, via the registration
   cert (§A.3). Rotation is a clean capability here, not a required mitigation.
6. Greenfield — no wire-format back-compat; cut call sites over directly.

## 5. Part A — Unified validator-key registry

### A.1 The registration cert as the unification carrier

Generalize `KesRegistrationCert` into an operator-key cert carrying all three
VKs (and PoP for BLS). Either rename to `OperatorKeyRegistrationCert` or extend
in place; new fields:

```scala
case class KesRegistrationCert(            // (or OperatorKeyRegistrationCert)
  operatorPeerId: PeerId,
  kesMasterVK: Hex, kesMasterVKStep: Int, offset: Long,   // existing KES
  vrfVK: Hex,                                             // NEW — completes intended VRF rotation
  blsVK: Hex,                                             // NEW — 48B G1 compressed
  blsPoP: Hex,                                            // NEW — proof-of-possession (G2, 96B)
  effectiveFromEpoch: EpochProgress,
  ordinal: KesRegistrationOrdinal,
  parent: KesRegistrationReference = ….empty
)
```

One registration event carries every key, so they cannot drift out of sync —
exactly the property `VrfRegistry`'s doc already asserts. Acceptance verifies
the envelope signature under the operator's long-term key (existing) **plus**
`popVerify(blsVK, blsPoP)` (new) before the BLS VK is honored.

### A.2 The registry trait

Generalize `KesRegistry` + `VrfRegistry` into one per-peer bundle. `getKesVk` /
`getVrfVk` become typed accessors so existing call sites change minimally:

```scala
final case class ValidatorKeys(kes: KesRegistryEntry, vrf: Array[Byte], bls: Array[Byte])
trait ValidatorKeyRegistry[F[_]] {
  def get(peerId: PeerId): F[Option[ValidatorKeys]]
  def kes(peerId: PeerId): F[Option[KesRegistryEntry]]   // == old getKesVk
  def vrf(peerId: PeerId): F[Option[Array[Byte]]]        // == old getVrfVk
  def bls(peerId: PeerId): F[Option[Array[Byte]]]        // NEW (48B G1)
  def list: F[Map[PeerId, ValidatorKeys]]
}
```

`StakeRegistry` stays separate — it holds relative-stake *weights*, not keys,
with a different source (snapshot/MPT aggregation) and lifecycle.

### A.3 Rotation overlay

Generalize `MutableKesRegistry` (`.../domain/nakamoto/kes/MutableKesRegistry.scala`)
to overlay all three VKs from the same MPT-backed cert chain via the existing
`KesRegistrationStateManager` pattern. Lookup precedence is unchanged: highest-ordinal
runtime cert with `effectiveFromEpoch <= currentEpoch`, else genesis-frozen base,
else `None`. VRF and BLS rotation are thereby inherited from the KES machinery —
no new overlay engine. This also **completes the half-built VRF runtime rotation**.

### A.4 Genesis derivation

- `L0GenesisData.operators[]` gains `blsPublicKey` (+ `blsPoP`), populated by the
  genesis generator from each operator's keypair — same place it already derives
  `vrfPublicKey`.
- New `BlsKeyDeriver` (sibling of `VrfKeyDeriver`): SHA-512 domain-tag
  `tessellation-bls-v1` over the normalized 32-byte secp256k1 scalar → ≥32-byte
  IKM → `BLS12_381BasicScheme.keyGen(ikm, keyInfo)` → sk → `skToPk` → `compressG1`
  → 48B VK; `popProve(sk)` → 96B PoP. Reuse `VrfKeyDeriver.normalizeEcScalar32`
  (the determinism contract is shared).
- **Bootstrap framing:** *VRF and BLS* genesis keys are identity-derived for
  convenience; **KES is independently generated at genesis** (forward-secure
  from t0 — not derived). Post-genesis rotation lets operators replace the
  derived VRF/BLS bootstrap keys with independent ones, and re-root KES before
  expiry / on mid-life join.

## 6. Part B — BLS aggregate signatures at snapshot certificates

### B.1 `BlsSigner[F]` wrapper

Thin Cats-Effect wrapper over the BC static scheme classes, mirroring how
`KesEd25519Blake2b256` wraps `rfc8032.Ed25519`. Surface: `derive` (from keypair),
`sign(sk, msg)`, `aggregate(sigs)`, `fastAggregateVerify(pks, msg, aggSig)`,
`popProve` / `popVerify`. Encapsulates the `BigInteger` / `ECPoint` /
`BLS12_381G2Point` ↔ bytes conversions via `BLS12_381Serialization`.

### B.2 Aggregate envelope — `AggregateSigned[A]` (DECIDED 2026-06-02)

**`Signed[A]` and its `proofs: NonEmptySet[SignatureProof]` are LEFT UNTOUCHED** —
no change to `SignatureProof`, `validProofs`, `isSignedBy`, or the ~169 generic
`.proofs` consumers. The aggregate lives in a NEW, parallel envelope used only
where aggregation pays off (the snapshot certificates):

```scala
final case class AggregateSigned[A](   // A is PHANTOM — type-documents what is certified; NO value field
  hash: Hash,                 // the signed message = the certified value's hash (e.g. the snapshot hash)
  signers: NonEmptySet[Id],   // explicit signer set — count = signers.size (verifiable)
  sig: BlsSignature           // one 96B compressed-G2 aggregate
)
```

**Detached / Mithril-style certificate** (DECIDED 2026-06-02): the cert attests to
a `Hash`, NOT to an attached value. Verification only needs the *signed message*
— and the signers sign the hash — so the value is redundant in the cert and lives
where it already does (the snapshot store), keyed by the same hash. Benefits: no
`[A]`/`Codec[A]` plumbing (the codec is concrete `Hash :: NonEmptySet[Id] ::
BlsSignature`), a tiny cert, and **S4 becomes additive** (below).

The signer set is carried **explicitly** — a BLS aggregate encodes neither the
count nor the identities, so `signers` is BOTH the `FastAggregateVerify`
pubkey-lookup input AND the source of the signer count for quorum / stake-weight
logic. The count is **tamper-proof**: a wrong signer list fails verification.
`NonEmptySet` ⇒ dedup ⇒ sound distinct-count. The 48B public keys are NOT carried
— looked up per signer in `ValidatorKeyRegistry`. A concrete `ImmutableCodec`
defines the wire form.

(Rejected alternatives: a sealed `Proofs` on `Signed.proofs` — touches ~169
generic consumers; and a value-attached `AggregateSigned[A](value, …)` — needs
`Codec[A]` and bloats the cert with the payload.)

### B.3 Verification

`AggregateSigned[A]` verify: look up each `signers` member's BLS VK in
`ValidatorKeyRegistry` (fail-closed on any unresolved signer), then
`BlsSigner.fastAggregateVerify(vks, hash.getBytes, sig)` — one 2-pairing check.
No value hashing, no `Encoder[A]`. The **consumer that holds the value binds it**
with a one-liner: `Hasher.hash(value) == cert.hash`, then verifies the cert.
`Signed[A].validProofs` (the per-proof traverse at `Signed.scala:126`) is
unchanged and still used for every non-aggregate type.

**S4 is now ADDITIVE:** because the cert references the snapshot *by hash*, the
snapshot stays `Signed[GlobalSnapshot]` and the ~171 snapshot-threading sites are
**untouched** — the `AggregateSigned` cert is produced / gossiped / stored
*alongside* the snapshot. S4 adds the cert + its emit/verify wiring; it does not
swap the snapshot's envelope type.

## 7. Slices (incremental, each independently testable)

- **S1 — Primitive.** `BlsSigner[F]` + `BlsKeyDeriver` + KAT tests against the
  eth2 vectors the spike validated. No consensus change. *(Needs BC 1.85 on the
  classpath — vendored beta in dev, stable for merge.)*
- **S2 — Registry.** Generalize cert + registry + overlay to carry BLS (and
  complete VRF runtime rotation); derive BLS at genesis; `popVerify` on load.
- **S3 — Envelope.** `AggregateProof` variant + `ImmutableCodec`; `fastAggregateVerify`
  path, gated behind a flag, exercised in tests.
- **S4 — Cutover.** Switch global/currency snapshot signing to aggregate certs;
  benchmark verify cost vs. the N-traverse; e2e at target topology.

## 8. Risks / open questions

- **BC 1.85 stable timing** gates merge (S1+). Beta is fine for dev.
- **PoP ceremony:** genesis emits PoP per operator; runtime certs carry PoP.
  Confirm `popVerify` is enforced at *both* genesis load and cert acceptance.
- **Determinism:** canonical signer-set ordering for the registry pubkey vector;
  the aggregate proof must hash deterministically (`proofsHash`).
- **Cert naming:** extend `KesRegistrationCert` in place vs. rename to
  `OperatorKeyRegistrationCert` (touches validator/manager/routes/MPT-partition).
- **Coexistence:** during S3, individual and aggregate proofs both valid; the
  per-proof validators (`SignedValidator`) must dispatch on the proof variant.
- **KES genesis provenance (verification gap):** the registry/cert model keeps
  KES independent (identity-*bound*, not *derived*); `GenesisGenerator`'s KES-seed
  origin was not read — confirm it doesn't derive the seed from identity if
  certainty is required.
