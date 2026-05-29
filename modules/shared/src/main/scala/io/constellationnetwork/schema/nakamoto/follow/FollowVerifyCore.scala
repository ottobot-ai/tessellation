package io.constellationnetwork.schema.nakamoto.follow

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import cats.{Eq, Parallel}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.swap.AllowSpendReference
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MerklePatriciaCommitment
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaRangeProof
import io.constellationnetwork.security.mpt.verifier.{MerklePatriciaRangeVerifier, MerklePatriciaVerificationError}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedTokenLockSetCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

import io.circe._
import io.circe.syntax._
import scodec.bits.ByteVector

/** Failure modes of the gl0 → gl1 follow verifier. Sealed, exhaustive, no string-typed control flow (per contract bar #4 in
  * `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  *   - [[FollowVerificationError.CommittedRootMismatch]] — `proof.committedRoot` is not the trusted `attestedRoot`. The whole payload is
  *     rejected before any range proof is touched.
  *   - [[FollowVerificationError.RangeProofInvalid]] — a field's [[MerklePatriciaRangeProof]] failed `confirmRange` against the trusted
  *     root (bad witness, out-of-range path, broken exclusion boundary, omitted-but-detectable leaf, …). Wraps the underlying
  *     [[MerklePatriciaVerificationError]] structurally — the cause is carried, never re-derived from a message string.
  *   - [[FollowVerificationError.ValueBindingFailed]] — a `(keyHex → valueHex)` pair in `proof.values` does not bind to the corresponding
  *     leaf's `dataDigest` (no leaf at that key, leaf is not a value leaf, or `Hasher.hashBytes(value) =!= dataDigest`). The value is
  *     mandatory; there is no verify variant that skips this step (contract bar #2).
  *   - [[FollowVerificationError.FieldRootMismatch]] — the '''own-slice mirror''' (gl1) verify path: after applying a producer's claimed slice
  *     to a consumed field, forward-hashing each `(Address, value)` to its MPT leaf the way gl0's writer does, and recomputing the subtree's
  *     root via gl0's exact `fieldRootFromBytes`, the recomputed root did not equal the signed `stateProof.<field>Proof`. Catches any wrong /
  *     missing / extra entry — a different entry set yields a different subtree root. This is the completeness mechanism for a full-content
  *     holder (correct-by-design via field-root equality), distinct from the per-key [[RangeProofInvalid]] / [[ValueBindingFailed]] path used
  *     by cross-shard / light-client consumers. See `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`, "Locked decisions" → recompute-and-
  *     match (field-root equality).
  */
sealed trait FollowVerificationError extends Product with Serializable

object FollowVerificationError {
  final case class CommittedRootMismatch(expected: Hash, got: Hash) extends FollowVerificationError
  final case class RangeProofInvalid(field: GlobalStateFieldId, cause: MerklePatriciaVerificationError) extends FollowVerificationError
  final case class ValueBindingFailed(field: GlobalStateFieldId, key: Hex) extends FollowVerificationError
  final case class FieldRootMismatch(field: GlobalStateFieldId, expected: Hash, got: Hash) extends FollowVerificationError

  // MerklePatriciaVerificationError has no Eq; it's `extends Throwable` with string-carrying subtypes.
  // Compare the cause by reference-class + message so RangeProofInvalid gets an Eq without inventing an
  // Eq on the foreign error ADT. This Eq is for test assertions / dedup, never for control flow.
  private implicit val mpVerificationErrorEq: Eq[MerklePatriciaVerificationError] =
    Eq.instance { (a, b) =>
      a.getClass == b.getClass && Option(a.getMessage) == Option(b.getMessage)
    }

  // GlobalStateFieldId is a closed set of case objects with only an Ordering instance in scope; universal
  // equality is exact for it. Local Eq so the FollowVerificationError Eq below can use `===`.
  private implicit val fieldIdEq: Eq[GlobalStateFieldId] = Eq.fromUniversalEquals

  implicit val eq: Eq[FollowVerificationError] = Eq.instance {
    case (CommittedRootMismatch(e1, g1), CommittedRootMismatch(e2, g2)) => e1 === e2 && g1 === g2
    case (RangeProofInvalid(f1, c1), RangeProofInvalid(f2, c2))         => f1 === f2 && c1 === c2
    case (ValueBindingFailed(f1, k1), ValueBindingFailed(f2, k2))       => f1 === f2 && k1 === k2
    case (FieldRootMismatch(f1, e1, g1), FieldRootMismatch(f2, e2, g2)) => f1 === f2 && e1 === e2 && g1 === g2
    case _                                                              => false
  }
}

/** The verified slice of global state a gl1 follower consumes for DAG-tx validation — and nothing else. Decoded from the proof's value
  * bytes only AFTER [[FollowVerifyCore.verifyFieldRoots]] has matched every consumed field's recomputed subtree root against gl0's signed
  * `stateProof.<field>Proof`.
  *
  * Private constructor: the only way to obtain a `ConsumedFieldState` wrapped in [[Verified]] is through the verifier. This is the
  * "no path to use unproven state" half of contract bar #1.
  *
  * '''Address-keyed (own-slice rework, 2026-05-28).''' Keyed by plaintext [[Address]] — the SAME key type gl1's downstream consumers use
  * (`TransactionService.balances`, `TokenLockService.getActiveTokenLocks`, `Collateral` / `CollateralDaemon`,
  * `mptStore.syncFromGlobalSnapshotInfo`, `getLatestBalances`). The own-
  * slice producer ([[io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowSliceService]]) now reads these maps directly from
  * gl0's finalized `GlobalSnapshotInfo` (`gsi.balances`, `gsi.lastTxRefs`, `gsi.lastAllowSpendRefs`, `gsi.lastTokenLockRefs`,
  * `gsi.activeTokenLocks`), so the address
  * is in hand; the verifier forward-hashes `(Address, value)` to the MPT leaf (`toHex(hypergraph(field, address))` + `immutableBytes(value)`)
  * to recompute the field roots. Keeping the address keeps gl1's downstream unchanged (no leaf-path-`Hex` → `Address` reverse map needed).
  * The per-key inclusion path (S1, light clients / cross-shard point reads) is unchanged and stays leaf-path-`Hex`-keyed in
  * [[ConsumedFieldLeaves]].
  */
final class ConsumedFieldState private (
  val balances: SortedMap[Address, Balance],
  val lastTxRefs: SortedMap[Address, TransactionReference],
  val lastAllowSpendRefs: SortedMap[Address, AllowSpendReference],
  val lastTokenLockRefs: SortedMap[Address, TokenLockReference],
  val activeTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
) {
  override def toString: String =
    s"ConsumedFieldState(balances=${balances.size}, lastTxRefs=${lastTxRefs.size}, " +
      s"lastAllowSpendRefs=${lastAllowSpendRefs.size}, lastTokenLockRefs=${lastTokenLockRefs.size}, " +
      s"activeTokenLocks=${activeTokenLocks.size})"
}

object ConsumedFieldState {

  val empty: ConsumedFieldState =
    new ConsumedFieldState(
      SortedMap.empty[Address, Balance],
      SortedMap.empty[Address, TransactionReference],
      SortedMap.empty[Address, AllowSpendReference],
      SortedMap.empty[Address, TokenLockReference],
      SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
    )

  /** Sole builder — `private[follow]` so only the verifier in this compilation unit can call it. Mirrors the `Verified` private-constructor
    * gate one level down. All maps are keyed by plaintext [[Address]] (see the class scaladoc).
    */
  private[follow] def make(
    balances: SortedMap[Address, Balance],
    lastTxRefs: SortedMap[Address, TransactionReference],
    lastAllowSpendRefs: SortedMap[Address, AllowSpendReference],
    lastTokenLockRefs: SortedMap[Address, TokenLockReference],
    activeTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  ): ConsumedFieldState =
    new ConsumedFieldState(balances, lastTxRefs, lastAllowSpendRefs, lastTokenLockRefs, activeTokenLocks)
}

/** The per-key-inclusion (S1) view of the consumed-field slice, keyed by the MPT '''leaf path''' (`Hex`). Produced by
  * [[FollowVerifyCore.verifyConsumedFields]] for cross-shard / light-client consumers that hold NONE of the field content and prove individual
  * keys via [[MerklePatriciaRangeProof]] + value-binding.
  *
  * '''Why leaf-path `Hex`, not `Address`, for THIS path.''' The MPT key for these hypergraph fields hashes the `Address` into the
  * `userNamespace` slot (`GlobalStateKey.toHex` → SHA over `addr.value.value`), which is '''one-way'''. A per-key inclusion proof
  * cryptographically commits only the MPT leaf path, not the plaintext address, so the only key a stateless verifier can soundly attach to a
  * proven value is that `Hex` path. The own-slice path ([[ConsumedFieldState]]) recovers the address instead because its producer reads the
  * Address-keyed `GlobalSnapshotInfo` directly — it doesn't reverse a hash. See the design doc "Locked decisions" #5 (one trust model, two
  * access patterns).
  */
final class ConsumedFieldLeaves private (
  val balances: SortedMap[Hex, Balance],
  val lastTxRefs: SortedMap[Hex, TransactionReference],
  val lastAllowSpendRefs: SortedMap[Hex, AllowSpendReference],
  val lastTokenLockRefs: SortedMap[Hex, TokenLockReference],
  val activeTokenLocks: SortedMap[Hex, SortedSet[Signed[TokenLock]]]
) {
  override def toString: String =
    s"ConsumedFieldLeaves(balances=${balances.size}, lastTxRefs=${lastTxRefs.size}, " +
      s"lastAllowSpendRefs=${lastAllowSpendRefs.size}, lastTokenLockRefs=${lastTokenLockRefs.size}, " +
      s"activeTokenLocks=${activeTokenLocks.size})"
}

object ConsumedFieldLeaves {

  /** Sole builder — `private[follow]` so only [[FollowVerifyCore.verifyConsumedFields]] can call it. */
  private[follow] def make(
    balances: SortedMap[Hex, Balance],
    lastTxRefs: SortedMap[Hex, TransactionReference],
    lastAllowSpendRefs: SortedMap[Hex, AllowSpendReference],
    lastTokenLockRefs: SortedMap[Hex, TokenLockReference],
    activeTokenLocks: SortedMap[Hex, SortedSet[Signed[TokenLock]]]
  ): ConsumedFieldLeaves =
    new ConsumedFieldLeaves(balances, lastTxRefs, lastAllowSpendRefs, lastTokenLockRefs, activeTokenLocks)
}

/** A producer's claimed consumed-field slice for the '''own-slice mirror''' (gl1) follow path, as consumed by
  * [[FollowVerifyCore.verifyFieldRoots]] (see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * '''Address-keyed, typed-per-field (own-slice rework, 2026-05-28).''' The producer
  * ([[io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowSliceService]]) reads the five consumed fields directly from gl0's
  * finalized `GlobalSnapshotInfo` (`gsi.balances` etc.), so they arrive as the SAME typed Address-keyed maps gl1's downstream uses. The
  * payload carries them as typed maps (not value-bytes-as-`Hex`); the verifier forward-encodes each value with the EXACT `immutableBytes`
  * gl0's MPT writer uses (`GlobalStateConverter.toAllStateKeyValueBytes` → `enc[V]` → `ImmutableCodec[V].immutableBytes`) when recomputing
  * the field root, so re-encoding reproduces gl0's leaf `dataDigest` byte-identically.
  *
  *   - `balances` / `lastTxRefs` / `lastAllowSpendRefs` / `lastTokenLockRefs` / `activeTokenLocks` — the upserted entries per consumed field,
  *     Address-keyed and typed (`activeTokenLocks` is read by the token-lock-replacement validator, hence in-slice). Under the locked Transfer
  *     model (latest-finalized full slice) every consumed-field entry is an upsert and there are no
  *     removals; the typed-per-field shape and the removals map below still support an incremental delta (accept-time delta-capture, #287).
  *   - `removals` — per [[GlobalStateFieldId]], the `Address` set gl0 deleted. Applied before upserts so a key removed-then-reupserted ends
  *     with the new value, matching `MerklePatriciaTrie.withChanges`.
  */
final case class ConsumedFieldDelta(
  balances: SortedMap[Address, Balance],
  lastTxRefs: SortedMap[Address, TransactionReference],
  lastAllowSpendRefs: SortedMap[Address, AllowSpendReference],
  lastTokenLockRefs: SortedMap[Address, TokenLockReference],
  activeTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
  removals: SortedMap[GlobalStateFieldId, Set[Address]]
)

object ConsumedFieldDelta {

  val empty: ConsumedFieldDelta =
    ConsumedFieldDelta(
      SortedMap.empty[Address, Balance],
      SortedMap.empty[Address, TransactionReference],
      SortedMap.empty[Address, AllowSpendReference],
      SortedMap.empty[Address, TokenLockReference],
      SortedMap.empty[Address, SortedSet[Signed[TokenLock]]],
      SortedMap.empty[GlobalStateFieldId, Set[Address]]
    )

  // `GlobalStateFieldId` has no circe KeyEncoder/KeyDecoder (only Encoder[Int]) — encode `removals` as an
  // association list keyed by the fieldId's Int code, exactly like `GlobalFollowProof` does. `Address` has
  // KeyEncoder/KeyDecoder, so the typed upsert maps stay plain JSON objects.
  private implicit val removalsEncoder: Encoder[SortedMap[GlobalStateFieldId, Set[Address]]] =
    Encoder.encodeList[(GlobalStateFieldId, Set[Address])].contramap(_.toList)
  private implicit val removalsDecoder: Decoder[SortedMap[GlobalStateFieldId, Set[Address]]] =
    Decoder.decodeList[(GlobalStateFieldId, Set[Address])].map(SortedMap.from(_))

  implicit val encoder: Encoder[ConsumedFieldDelta] = (d: ConsumedFieldDelta) =>
    Json.obj(
      "balances" -> d.balances.asJson,
      "lastTxRefs" -> d.lastTxRefs.asJson,
      "lastAllowSpendRefs" -> d.lastAllowSpendRefs.asJson,
      "lastTokenLockRefs" -> d.lastTokenLockRefs.asJson,
      "activeTokenLocks" -> d.activeTokenLocks.asJson,
      "removals" -> d.removals.asJson
    )

  implicit val decoder: Decoder[ConsumedFieldDelta] = (c: HCursor) =>
    for {
      balances <- c.downField("balances").as[SortedMap[Address, Balance]]
      lastTxRefs <- c.downField("lastTxRefs").as[SortedMap[Address, TransactionReference]]
      lastAllowSpendRefs <- c.downField("lastAllowSpendRefs").as[SortedMap[Address, AllowSpendReference]]
      lastTokenLockRefs <- c.downField("lastTokenLockRefs").as[SortedMap[Address, TokenLockReference]]
      activeTokenLocks <- c.downField("activeTokenLocks").as[SortedMap[Address, SortedSet[Signed[TokenLock]]]]
      removals <- c.downField("removals").as[SortedMap[GlobalStateFieldId, Set[Address]]]
    } yield ConsumedFieldDelta(balances, lastTxRefs, lastAllowSpendRefs, lastTokenLockRefs, activeTokenLocks, removals)

  // Structural Eq via the canonical JSON encoding — byte-stable (sorted maps, deterministic field order),
  // same approach `GlobalFollowProof` uses. For test assertions / dedup, never control flow.
  implicit val eq: Eq[ConsumedFieldDelta] = Eq.by[ConsumedFieldDelta, Json](_.asJson)
}

/** Proof that `value: A` was produced by the verifier and only the verifier. The constructor is private and the only builder
  * ([[Verified.makeInternal]]) is `private[follow]`, so a `Verified[A]` cannot be forged outside this compilation unit. gl1's validator
  * signature (S3) consumes `Verified[ConsumedFieldState]` — there is no path to feed it unproven state (contract bar #1).
  */
sealed abstract case class Verified[A] private (value: A)

object Verified {

  /** Sole builder. `private[follow]` — callable only by [[FollowVerifyCore]] (same compilation unit / package). */
  private[follow] def makeInternal[A](value: A): Verified[A] = new Verified[A](value) {}
}

/** Correct-by-design verify core for the gl0 → gl1 follow path (Axis 2).
  *
  * Two entry points, each producing a [[Verified]] gate but via a different completeness mechanism and a different key type (see
  * `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`, "Locked decisions"):
  *
  *   - [[verifyConsumedFields]] — '''per-key inclusion''' for cross-shard / light-client consumers that do NOT hold the field content. Range
  *     proof + value-binding vs the signed `mptRoot`. Returns [[Verified]]`[`[[ConsumedFieldLeaves]]`]` (leaf-path-`Hex`-keyed — a stateless
  *     verifier cannot reverse the one-way address hash).
  *   - [[verifyFieldRoots]] — '''field-root equality''' for the own-slice mirror follower (gl1) that DOES hold the full content of the four
  *     consumed fields. Apply the producer's claimed Address-keyed slice on top of the follower's prior state, forward-hash each
  *     `(Address, value)` to its MPT leaf the way gl0's writer does, recompute the subtree root via gl0's exact
  *     `GlobalStateConverter.fieldRootFromBytes`, and assert it equals the signed `stateProof.<field>Proof`. A wrong / missing / extra entry
  *     yields a different subtree root ⇒ [[FollowVerificationError.FieldRootMismatch]]. Returns [[Verified]]`[`[[ConsumedFieldState]]`]`
  *     (Address-keyed — the producer read the address from the GSI, so gl1's downstream stays unchanged).
  *
  * [[verifyConsumedFields]] performs, '''in order''':
  *   a. committed-root check — `proof.committedRoot === attestedRoot`, else [[FollowVerificationError.CommittedRootMismatch]].
  *   a. per-field range-proof check — `MerklePatriciaRangeVerifier.make(attestedRoot).confirmRange(rangeProof)`, else
  *      [[FollowVerificationError.RangeProofInvalid]]. This is where inclusion, in-range, ordering, and exclusion-boundary completeness are
  *      enforced cryptographically — a hidden update or forged absence cannot pass.
  *   a. value-binding (mandatory, internal) — for each `(keyHex → valueHex)` in `proof.values(field)`, find the matching leaf in the field's
  *      range proof and assert `Hasher.hashBytes(valueHex.toBytes) === leaf.dataDigest`, else [[FollowVerificationError.ValueBindingFailed]].
  *   a. assemble + wrap — decode the now-bound value bytes into typed leaf-keyed maps and return `Right(Verified(ConsumedFieldLeaves(...)))`.
  */
object FollowVerifyCore {

  /** The fields a gl1 follower actually consumes (design doc "What gl1 actually consumes"). The prover is expected to populate exactly
    * these; the verifier processes whichever of them appear in the payload and ignores any extra field a (possibly buggy) prover added —
    * extra fields cannot widen the verified state because [[ConsumedFieldState]] / [[ConsumedFieldLeaves]] only expose these five.
    *
    * `ActiveTokenLocks` is consumed by gl1's token-lock-replacement validator
    * (`ContextualTokenLockValidator.validateReplaceTokenLockRef` / `getBalanceAffectedByTxs`, fed from `TokenLockService.getActiveTokenLocks`);
    * without it the follower's mirror keeps `activeTokenLocks` empty and every replacement fails `NothingToReplace`.
    */
  val consumedFields: List[GlobalStateFieldId] = List(
    GlobalStateFieldId.Balances,
    GlobalStateFieldId.LastTxRefs,
    GlobalStateFieldId.LastAllowSpendRefs,
    GlobalStateFieldId.LastTokenLockRefs,
    GlobalStateFieldId.ActiveTokenLocks
  )

  /** FIELD-ROOT-MATCH verify for an own-slice mirror follower (gl1). Correct-by-design completeness via field-root equality, for a holder of
    * the full content of the four consumed fields — Address-keyed throughout (own-slice rework, 2026-05-28).
    *
    * `prior` is the follower's current verified consumed-field state BEFORE this ordinal's writes (Address-keyed); under the locked Transfer
    * model the producer serves the latest-finalized FULL slice as a delta-from-empty, so `prior` is [[ConsumedFieldState.empty]] for the
    * bootstrap fetch (the typed-per-field delta + removals still support an incremental apply, #287). `delta` is gl0's claimed slice;
    * `signedFieldRoots` are the `stateProof.<field>Proof` values from the signed, finality-gated snapshot.
    *
    * Steps, '''in order''' (short-circuits on the first mismatch):
    *   a. '''apply''' — for each consumed field, `(prior(field) -- delta.removals(field)) ++ delta.<field>` (removals first matches
    *      `MerklePatriciaTrie.withChanges`), yielding the post-state Address-keyed map.
    *   a. '''forward-hash''' — for each `(Address, value)` in the post-state, derive the MPT leaf key `toHex(hypergraph(field, address))` and
    *      the leaf value bytes `ImmutableCodec[V].immutableBytes(value).toArray` — the EXACT key derivation and value encoding gl0's MPT
    *      writer uses (`GlobalStateConverter.toAllStateKeyValueBytes`: `GlobalStateKey.hypergraph(fieldId, addr)` + `enc[V]`), so the rebuilt
    *      leaf digests are byte-identical to gl0's.
    *   a. '''recompute''' — recompute the field's subtree root via [[GlobalStateConverter.fieldRootFromBytes]] — the SAME callable gl0 uses
    *      for `stateProof.<field>Proof` (producer + overlay paths). Byte-identity is by construction, not by re-implementation.
    *   a. '''match''' — assert the recomputed root `=== signedFieldRoots(field)` (a field absent from `signedFieldRoots` is treated as
    *      [[Hash.empty]], matching gl0's `getOrElse(_, Hash.empty)` default), else [[FollowVerificationError.FieldRootMismatch]].
    *   a. '''assemble + wrap''' — return `Right(Verified(ConsumedFieldState(<post-state maps>)))`.
    *
    * Note the [[Async]]`/`[[Parallel]]`/`[[JsonSerializer]] constraints (vs [[verifyConsumedFields]]'s `Async: Hasher`): recomputing a subtree
    * root rebuilds an MPT from bytes via `MerklePatriciaTrie.makeParallelFromBytes`, which needs them, and `GlobalStateKey.toHex` needs a
    * [[Hasher]]. This is required to reuse gl0's exact `fieldRootFromBytes` + key derivation rather than re-implementing them.
    */
  def verifyFieldRoots[F[_]: Async: Parallel: Hasher: JsonSerializer](
    prior: ConsumedFieldState,
    delta: ConsumedFieldDelta,
    signedFieldRoots: SortedMap[GlobalStateFieldId, Hash]
  ): F[Either[FollowVerificationError, Verified[ConsumedFieldState]]] = {
    // (a) apply: removals first, then upserts (upsert-wins, matches `MerklePatriciaTrie.withChanges`).
    // Explicit foldLeft (not `++`) so the upsert-wins merge is intentional, satisfying NoMapConcat.
    def applyField[V](priorMap: SortedMap[Address, V], upserts: SortedMap[Address, V], field: GlobalStateFieldId): SortedMap[Address, V] =
      upserts.foldLeft(priorMap -- delta.removals.getOrElse(field, Set.empty[Address])) {
        case (acc, (addr, value)) => acc.updated(addr, value)
      }

    val postBalances = applyField(prior.balances, delta.balances, GlobalStateFieldId.Balances)
    val postTxRefs = applyField(prior.lastTxRefs, delta.lastTxRefs, GlobalStateFieldId.LastTxRefs)
    val postAllowSpendRefs = applyField(prior.lastAllowSpendRefs, delta.lastAllowSpendRefs, GlobalStateFieldId.LastAllowSpendRefs)
    val postTokenLockRefs = applyField(prior.lastTokenLockRefs, delta.lastTokenLockRefs, GlobalStateFieldId.LastTokenLockRefs)
    val postActiveTokenLocks = applyField(prior.activeTokenLocks, delta.activeTokenLocks, GlobalStateFieldId.ActiveTokenLocks)

    // (b) forward-hash one field's Address-keyed post-state into the MPT `(leaf-path Hex → value bytes)` map,
    // using the EXACT key derivation + value encoding gl0's MPT writer uses (see `toAllStateKeyValueBytes`).
    def leafBytes[V: ImmutableCodec](field: GlobalStateFieldId, post: SortedMap[Address, V]): F[Map[Hex, Array[Byte]]] =
      post.toList.traverse {
        case (addr, value) =>
          GlobalStateKey.toHex[F](GlobalStateKey.hypergraph(field, addr)).map(_ -> ImmutableCodec[V].immutableBytes(value).toArray)
      }.map(_.toMap)

    // (c)+(d): recompute each field's subtree root via gl0's shared callable and match the signed root. Carries the
    // post-state map forward on success so assemble can wrap it without re-applying. Short-circuits on first mismatch.
    def checkField[V: ImmutableCodec](field: GlobalStateFieldId, post: SortedMap[Address, V]): EitherT[F, FollowVerificationError, Unit] =
      EitherT(
        leafBytes[V](field, post)
          .flatMap(GlobalStateConverter.fieldRootFromBytes[F])
          .map { recomputed =>
            val expected = signedFieldRoots.getOrElse(field, Hash.empty)
            if (recomputed === expected) ().asRight[FollowVerificationError]
            else (FollowVerificationError.FieldRootMismatch(field, expected, recomputed): FollowVerificationError).asLeft[Unit]
          }
      )

    val checkAll: F[Either[FollowVerificationError, Unit]] =
      (
        checkField[Balance](GlobalStateFieldId.Balances, postBalances) >>
          checkField[TransactionReference](GlobalStateFieldId.LastTxRefs, postTxRefs) >>
          checkField[AllowSpendReference](GlobalStateFieldId.LastAllowSpendRefs, postAllowSpendRefs) >>
          checkField[TokenLockReference](GlobalStateFieldId.LastTokenLockRefs, postTokenLockRefs) >>
          checkField[SortedSet[Signed[TokenLock]]](GlobalStateFieldId.ActiveTokenLocks, postActiveTokenLocks)
      ).value

    checkAll.map {
      case Left(err) => err.asLeft[Verified[ConsumedFieldState]]
      case Right(()) =>
        Verified
          .makeInternal(
            ConsumedFieldState.make(postBalances, postTxRefs, postAllowSpendRefs, postTokenLockRefs, postActiveTokenLocks)
          )
          .asRight[FollowVerificationError]
    }
  }

  def verifyConsumedFields[F[_]: Async: Hasher](
    attestedRoot: Hash,
    proof: GlobalFollowProof
  ): F[Either[FollowVerificationError, Verified[ConsumedFieldLeaves]]] =
    if (proof.committedRoot =!= attestedRoot)
      (FollowVerificationError.CommittedRootMismatch(attestedRoot, proof.committedRoot): FollowVerificationError)
        .asLeft[Verified[ConsumedFieldLeaves]]
        .pure[F]
    else {
      val verifier = MerklePatriciaRangeVerifier.make[F](attestedRoot)

      // (b) + (c): for every consumed field present in the payload, verify the range proof then bind its
      // values. Short-circuits on the first failure via EitherT.
      val verifyFields: F[Either[FollowVerificationError, Unit]] =
        consumedFields.traverse_ { field =>
          proof.fields.get(field) match {
            case None =>
              // Field carries no proof — nothing to verify for it. A field gl1 consumes but that has no
              // entries this ordinal is legitimately absent (empty map); cryptographic absence of any
              // particular key is established by the range proof when the field IS present.
              EitherT.rightT[F, FollowVerificationError](())
            case Some(rangeProof) =>
              for {
                _ <- EitherT(verifier.confirmRange(rangeProof))
                  .leftMap(cause => FollowVerificationError.RangeProofInvalid(field, cause): FollowVerificationError)
                _ <- EitherT(bindValues[F](field, rangeProof, proof.values.getOrElse(field, SortedMap.empty[Hex, Hex])))
              } yield ()
          }
        }.value

      verifyFields.flatMap {
        case Left(err) => err.asLeft[Verified[ConsumedFieldLeaves]].pure[F]
        case Right(()) => assemble[F](proof).map(state => Verified.makeInternal(state).asRight[FollowVerificationError])
      }
    }

  /** Value-binding for one field: every `(keyHex → valueHex)` must hash to the `dataDigest` of the leaf at `keyHex` in `rangeProof`. The
    * leaf commitment is the head of the inclusion proof's witness (the prover prepends root→leaf, so the deepest/leaf commitment ends up at
    * the head; the verifier walks `witness.reverse`). `rangeProof` has already been confirmed against the trusted root by the caller, so the
    * `dataDigest` we read here is cryptographically bound to `attestedRoot`; binding ties the actual value bytes to it.
    */
  private def bindValues[F[_]: Async: Hasher](
    field: GlobalStateFieldId,
    rangeProof: MerklePatriciaRangeProof,
    values: SortedMap[Hex, Hex]
  ): F[Either[FollowVerificationError, Unit]] = {
    val leafDigestByPath: Map[Hex, Hash] =
      rangeProof.inclusionProofs.flatMap { ip =>
        ip.witness.headOption.collect {
          case leaf: MerklePatriciaCommitment.Leaf => ip.path -> leaf.dataDigest
        }
      }.toMap

    values.toList.traverse_ {
      case (keyHex, valueHex) =>
        EitherT(
          leafDigestByPath.get(keyHex) match {
            case None =>
              // The payload carries a value for a key that has no committed leaf in the range proof —
              // cannot be bound. (Includes the omitted-leaf case where prover dropped the inclusion
              // proof but kept the value.)
              (FollowVerificationError.ValueBindingFailed(field, keyHex): FollowVerificationError).asLeft[Unit].pure[F]
            case Some(dataDigest) =>
              Hasher[F].hashBytes(valueHex.toBytes).map { computed =>
                if (computed === dataDigest) ().asRight[FollowVerificationError]
                else (FollowVerificationError.ValueBindingFailed(field, keyHex): FollowVerificationError).asLeft[Unit]
              }
          }
        )
    }.value
  }

  /** Decode the (now value-bound) bytes into typed maps, keyed by the committed leaf-path `Hex`. Decode failures surface as a raised error
    * inside `F` rather than a `FollowVerificationError`: a value whose `hashBytes` matched the committed `dataDigest` but that fails to
    * scodec-decode means the committed state itself is malformed for the declared field — a prover/serialization bug, not a verification
    * outcome. Slice 1 treats it as exceptional; S3 wiring can decide whether to demote it to a typed error once the field set is
    * load-bearing.
    */
  private def assemble[F[_]: Async](proof: GlobalFollowProof): F[ConsumedFieldLeaves] = {
    def decodeMap[V: ImmutableCodec](field: GlobalStateFieldId): F[SortedMap[Hex, V]] =
      proof.values.getOrElse(field, SortedMap.empty[Hex, Hex]).toList.traverse {
        case (keyHex, valueHex) =>
          ImmutableCodec[V].fromImmutableBytes(ByteVector.view(valueHex.toBytes)) match {
            case Right(v) => (keyHex -> v).pure[F]
            case Left(err) =>
              new RuntimeException(s"Follow-proof value decode failed for field $field at $keyHex: $err").raiseError[F, (Hex, V)]
          }
      }.map(SortedMap.from(_))

    for {
      balances <- decodeMap[Balance](GlobalStateFieldId.Balances)
      lastTxRefs <- decodeMap[TransactionReference](GlobalStateFieldId.LastTxRefs)
      lastAllowSpendRefs <- decodeMap[AllowSpendReference](GlobalStateFieldId.LastAllowSpendRefs)
      lastTokenLockRefs <- decodeMap[TokenLockReference](GlobalStateFieldId.LastTokenLockRefs)
      activeTokenLocks <- decodeMap[SortedSet[Signed[TokenLock]]](GlobalStateFieldId.ActiveTokenLocks)
    } yield ConsumedFieldLeaves.make(balances, lastTxRefs, lastAllowSpendRefs, lastTokenLockRefs, activeTokenLocks)
  }
}
