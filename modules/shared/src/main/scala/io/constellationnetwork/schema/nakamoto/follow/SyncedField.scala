package io.constellationnetwork.schema.nakamoto.follow

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.swap.AllowSpendReference
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.schema.{GlobalSnapshotInfo, GlobalSnapshotStateProof}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedTokenLockSetCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

/** Single-source descriptor for ONE of the five UNIFORM gl0 hypergraph state fields a follower (gl1 / cl1 / dl1) syncs along the own-slice
  * follow path (Axis 2 — see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * '''What "uniform" means here.''' Each of the five fields shares the EXACT same shape: a single signed `Hash` root over a
  * `SortedMap[Address, V]`, encoded leaf-wise by one `ImmutableCodec[V]`, keyed in gl0's MPT by `toHex(hypergraph(fieldId, address))`. This
  * descriptor captures the per-field bits that differ — the `fieldId`, the value type `V` + its codec, where the field lives on the
  * `GlobalSnapshotInfo` (read/write), where it lives on a verified [[ConsumedFieldState]] (typed read), and which `stateProof.<field>Proof`
  * slot carries its signed root — so the five places that used to hand-duplicate "which fields a follower syncs" (`consumedFields`, the two
  * processors' `signedFieldRoots`, the two processors' `consumedFieldsToGlobalSnapshotInfo`, `GlobalFollowSliceService.sliceFromGsi`) can
  * be expressed ONCE over [[SyncedField.baseRegistry]] and drift becomes a compile/CI failure (`RegistryConsistencySuite`).
  *
  * '''Scope — the five UNIFORM fields only.''' `lastCurrencySnapshots` (the 6th consumed field, cl1/dl1 only) is deliberately NOT modeled
  * here: it is `Either`-valued, splits into TWO `metagraph`-namespaced MPT partitions, carries a paired `CurrencySnapshotMptRoots` root via
  * [[GlobalStateConverter.currencySnapshotFieldRoots]], and needs a `StateProofSelector`. Forcing it into this uniform descriptor would
  * require an unsafe cast (no single `ImmutableCodec[V]`, no single signed `Hash`), so it stays a bespoke sibling handled explicitly by its
  * callers (`FollowVerifyCore.currencySnapshotsCheck`, the cl1/dl1 `consumedFieldsToGlobalSnapshotInfo` `lastCurrencySnapshots` line).
  *
  * '''Path-dependent `V` never leaks.''' `V` is an abstract type MEMBER (not a type parameter), so the existential `List[SyncedField]` in
  * [[SyncedField.baseRegistry]] holds heterogeneous value types without an unsafe cast. The per-field operations are METHODS on the
  * descriptor (`readFromGsi` / `writeToGsi` / `readFromState` / `signedRoot` / `fieldRoot`), each round-tripping through its own `V`, so a
  * caller folding over the registry (`baseRegistry.flatMap(sf => sf.signedRoot(p).map(sf.fieldId -> _))`, `baseRegistry.foldLeft(gsi)((g,
  * sf) => sf.writeToGsi(g, sf.readFromState(s)))`) never names `V` and never casts.
  */
sealed trait SyncedField {

  /** The field's value type — an abstract type MEMBER so `List[SyncedField]` can hold the five heterogeneous value types existentially. */
  type V

  /** The gl0 `GlobalStateFieldId` partition this descriptor projects. */
  def fieldId: GlobalStateFieldId

  /** The single canonical leaf codec for `V` — the EXACT `ImmutableCodec[V]` gl0's MPT writer uses (`enc[V]` in
    * `GlobalStateConverter.toAllStateKeyValueBytes`), so re-encoding reproduces gl0's leaf `dataDigest` byte-identically.
    */
  implicit def codec: ImmutableCodec[V]

  /** Read this field's Address-keyed map off a finalized [[GlobalSnapshotInfo]] (the slice-producer source). `Option`-typed GSI fields are
    * defaulted to empty here so the producer hands back a total map.
    */
  def readFromGsi(gsi: GlobalSnapshotInfo): SortedMap[Address, V]

  /** Write this field's Address-keyed map back onto a [[GlobalSnapshotInfo]] (the follower's partial-GSI assembly), preserving gl0's
    * `Some(...)`-vs-bare shape for the slot.
    */
  def writeToGsi(gsi: GlobalSnapshotInfo, m: SortedMap[Address, V]): GlobalSnapshotInfo

  /** Read this field's Address-keyed map off a verified [[ConsumedFieldState]] — a TYPED read (no cast), symmetric to [[writeToGsi]]. */
  def readFromState(s: ConsumedFieldState): SortedMap[Address, V]

  /** Write this field's Address-keyed map (all-upsert) onto a [[ConsumedFieldDelta]] — the slice-producer side
    * (`GlobalFollowSliceService.sliceFromGsi`). Both the GSI source field and the delta target field are plain Address-keyed maps, so this
    * is a straight `delta.copy(<field> = m)`; it lets the slice producer be expressed as a registry projection rather than a per-field
    * literal.
    */
  def writeToDelta(delta: ConsumedFieldDelta, m: SortedMap[Address, V]): ConsumedFieldDelta

  /** This field's SIGNED root slot on a [[GlobalSnapshotStateProof]]. Always-present roots are `Some(_)`; `Option`-typed slots pass through
    * (`None` ⇒ the field is omitted from the signed-roots map, matching gl0's `getOrElse(_, Hash.empty)` empty-field convention).
    */
  def signedRoot(proof: GlobalSnapshotStateProof): Option[Hash]

  /** Recompute this field's subtree root from an Address-keyed map the way gl0's writer does: forward-hash each `(Address, value)` to its
    * MPT leaf (`toHex(hypergraph(fieldId, address))` + `codec.immutableBytes(value)`) and route the `(leaf-path Hex → value bytes)` map
    * through gl0's EXACT [[GlobalStateConverter.fieldRootFromBytes]] — byte-identity by construction, not re-implementation. Identical math
    * to `FollowVerifyCore.verifyFieldRoots`'s `leafBytes` + `checkField`.
    */
  def fieldRoot[F[_]: Async: Parallel: Hasher: JsonSerializer](m: SortedMap[Address, V]): F[Hash] =
    m.toList.traverse {
      case (a, v) => GlobalStateKey.toHex[F](GlobalStateKey.hypergraph(fieldId, a)).map(_ -> codec.immutableBytes(v).toArray)
    }.map(_.toMap).flatMap(GlobalStateConverter.fieldRootFromBytes[F])
}

object SyncedField {

  /** `Aux[V0]` refines the abstract member so a single descriptor's `V` is recoverable at its construction site (the registry stores them
    * as the existential `SyncedField`).
    */
  type Aux[V0] = SyncedField { type V = V0 }

  /** Build a uniform-field descriptor. The five per-field bits are passed as functions; the canonical `ImmutableCodec[V0]` is captured
    * implicitly so `fieldRoot` re-encodes exactly as gl0's writer.
    */
  def apply[V0](
    fid: GlobalStateFieldId,
    rd: GlobalSnapshotInfo => SortedMap[Address, V0],
    wr: (GlobalSnapshotInfo, SortedMap[Address, V0]) => GlobalSnapshotInfo,
    rs: ConsumedFieldState => SortedMap[Address, V0],
    wd: (ConsumedFieldDelta, SortedMap[Address, V0]) => ConsumedFieldDelta,
    sr: GlobalSnapshotStateProof => Option[Hash]
  )(implicit c: ImmutableCodec[V0]): Aux[V0] =
    new SyncedField {
      type V = V0
      val fieldId: GlobalStateFieldId = fid
      implicit val codec: ImmutableCodec[V0] = c
      def readFromGsi(gsi: GlobalSnapshotInfo): SortedMap[Address, V0] = rd(gsi)
      def writeToGsi(gsi: GlobalSnapshotInfo, m: SortedMap[Address, V0]): GlobalSnapshotInfo = wr(gsi, m)
      def readFromState(s: ConsumedFieldState): SortedMap[Address, V0] = rs(s)
      def writeToDelta(delta: ConsumedFieldDelta, m: SortedMap[Address, V0]): ConsumedFieldDelta = wd(delta, m)
      def signedRoot(proof: GlobalSnapshotStateProof): Option[Hash] = sr(proof)
    }

  /** The single source of truth for the five UNIFORM hypergraph fields a follower syncs, in [[GlobalStateFieldId.ordering]]-irrelevant
    * declaration order (callers that need a `SortedMap` build one explicitly). Mirrors EXACTLY the hand-duplicated logic this refactor
    * collapses:
    *   - `readFromGsi` — `gsi.balances` / `gsi.lastTxRefs` / `gsi.lastAllowSpendRefs.getOrElse(empty)` /
    *     `gsi.lastTokenLockRefs.getOrElse(empty)` / `gsi.getActiveTokenLocks` (== `GlobalFollowSliceService.sliceFromGsi`);
    *   - `writeToGsi` — `g.copy(<field> = m)` for the always-bare slots, `g.copy(<field> = m.some)` for the `Option`-typed slots (==
    *     `consumedFieldsToGlobalSnapshotInfo`);
    *   - `readFromState` — the matching `ConsumedFieldState` accessor;
    *   - `signedRoot` — `p.balancesProof.some` / `p.lastTxRefsProof.some` / `p.lastAllowSpendRefs` / `p.lastTokenLockRefs` /
    *     `p.activeTokenLocks` (== the two processors' `signedFieldRoots`).
    */
  val baseRegistry: List[SyncedField] = List(
    SyncedField[Balance](
      GlobalStateFieldId.Balances,
      _.balances,
      (g, m) => g.copy(balances = m),
      _.balances,
      (d, m) => d.copy(balances = m),
      p => p.balancesProof.some
    ),
    SyncedField[TransactionReference](
      GlobalStateFieldId.LastTxRefs,
      _.lastTxRefs,
      (g, m) => g.copy(lastTxRefs = m),
      _.lastTxRefs,
      (d, m) => d.copy(lastTxRefs = m),
      p => p.lastTxRefsProof.some
    ),
    SyncedField[AllowSpendReference](
      GlobalStateFieldId.LastAllowSpendRefs,
      _.lastAllowSpendRefs.getOrElse(SortedMap.empty[Address, AllowSpendReference]),
      (g, m) => g.copy(lastAllowSpendRefs = m.some),
      _.lastAllowSpendRefs,
      (d, m) => d.copy(lastAllowSpendRefs = m),
      p => p.lastAllowSpendRefs
    ),
    SyncedField[TokenLockReference](
      GlobalStateFieldId.LastTokenLockRefs,
      _.lastTokenLockRefs.getOrElse(SortedMap.empty[Address, TokenLockReference]),
      (g, m) => g.copy(lastTokenLockRefs = m.some),
      _.lastTokenLockRefs,
      (d, m) => d.copy(lastTokenLockRefs = m),
      p => p.lastTokenLockRefs
    ),
    SyncedField[SortedSet[Signed[TokenLock]]](
      GlobalStateFieldId.ActiveTokenLocks,
      _.getActiveTokenLocks,
      (g, m) => g.copy(activeTokenLocks = m.some),
      _.activeTokenLocks,
      (d, m) => d.copy(activeTokenLocks = m),
      p => p.activeTokenLocks
    )
  )
}
