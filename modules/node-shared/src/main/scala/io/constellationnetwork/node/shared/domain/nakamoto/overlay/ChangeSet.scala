package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.Monoid
import cats.effect.kernel.Sync
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.sharding.ShardCurrencyStateDiff
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedAllowSpendSetCodec

import scodec.bits.ByteVector

/** A delta over an MPT base trie: keys to insert/update with their pre-serialized value bytes, plus keys to remove.
  *
  * Used by `MptOverlay` (#56.4) to accumulate per-branch mutations as in-memory diffs over the finalized base. Multiple change sets compose
  * via `merge`: applying `a.merge(b)` is semantically equivalent to applying `a` then `b` to the same base trie.
  *
  * Apply semantics: when this ChangeSet is materialized against a base trie via `MerklePatriciaTrie.withChanges(upserts, removals)`,
  * removals are applied first, then upserts. So if a key appears in both `upserts` and `removals`, the upsert wins. Direct construction may
  * leave such overlap; `merge` strips it under "later wins" semantics.
  *
  * Value bytes use the same canonical form as `MptStore.insertBytes` / `producer.entries` — pre-serialized via `ImmutableCodec`. Keep them
  * here as `Array[Byte]` for direct producer-layer consumption.
  *
  * Note on equality: `Array[Byte]` uses reference equality. `equals`/`hashCode` on this case class are NOT structural over the byte arrays.
  * Tests that compare ChangeSets must compare upserts.mapValues(_.toSeq) explicitly. The overlay never compares ChangeSets directly.
  */
final case class ChangeSet(
  upserts: Map[Hex, Array[Byte]],
  removals: Set[Hex]
) {

  /** Apply `other` on top of this — semantically `(this then other)` against the same base.
    *
    * Conflict resolution ("later wins"):
    *   - A key in `other.removals` cancels a matching `this.upserts` entry (the new state is "removed").
    *   - A key in `other.upserts` cancels a matching `this.removals` entry (the new state is "set to other.value").
    *   - Where both sets upsert the same key, `other`'s value wins.
    *
    * The result has no overlap between `upserts` keys and `removals`.
    */
  def merge(other: ChangeSet): ChangeSet = {
    val newUpserts = (upserts -- other.removals) ++ other.upserts
    val newRemovals = (removals -- other.upserts.keySet) ++ other.removals
    ChangeSet(newUpserts, newRemovals)
  }

  def isEmpty: Boolean = upserts.isEmpty && removals.isEmpty

  /** Total count of mutations — useful for memory-budget telemetry (#56.9). */
  def size: Int = upserts.size + removals.size
}

object ChangeSet {

  val empty: ChangeSet = ChangeSet(Map.empty, Set.empty)

  /** Wire form for the shard checkpoint (`ShardCurrencyStateDiff` lives in `shared`; `ChangeSet` can't). Value bytes are Hex-encoded;
    * `SortedMap`/`SortedSet` give the deterministic encoding the signing preimage needs. Exact inverse of [[fromWire]].
    */
  def toWire(cs: ChangeSet): ShardCurrencyStateDiff =
    ShardCurrencyStateDiff(
      upserts = SortedMap.from(cs.upserts.iterator.map { case (k, v) => k -> Hex.fromBytes(v) }),
      removals = SortedSet.from(cs.removals)
    )

  /** Decode the wire form back to a `ChangeSet` for `MerklePatriciaTrie.withChanges`. */
  def fromWire(d: ShardCurrencyStateDiff): ChangeSet =
    ChangeSet(
      upserts = d.upserts.iterator.map { case (k, v) => k -> v.toBytes }.toMap,
      removals = d.removals.toSet
    )

  /** The MINIMAL per-metagraph MPT byte-diff between two `CurrencySnapshotInfo`s — the committee carries this in
    * `ShardCheckpoint.derivedStateDelta.perMetagraphStateDiff` (step 6 of the unroll workstream;
    * `docs/nakamoto/COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md`).
    *
    * '''Coverage (PIN-2).''' Encodes BOTH halves of a metagraph's unrolled currency state, so the carried diff is self-sufficient and the
    * allow-spends transfer (the e2e's whole point):
    *   - the 8 `Mg*` info sub-fields via the SINGLE shared encoder [[GlobalStateConverter.infoEntryBytes]] (balances, txRefs, feeTxRefs,
    *     allow-spend refs, token-lock refs, active token-locks, messages, sync-view) — byte-identical to what gl0 writes;
    *   - the metagraph-scope `activeAllowSpends` (fieldId-7: `hypergraph(ActiveAllowSpends, Some(mgAddr), holder) -> SortedSet[Signed[
    *     AllowSpend]]`), which `infoEntryBytes` deliberately EXCLUDES (it lives in its own state-proof slot). Encoded with the SAME
    *     `signedAllowSpendSetCodec` the bytes writer ([[GlobalStateConverter.toAllStateKeyValueBytes]]) uses, so the bytes match.
    *
    * '''Minimality (PIN-3).''' `upserts` = the entries present in `next` whose value bytes DIFFER from `prior` (or are new); `removals` =
    * the keys present in `prior` but absent in `next`. Not "all entries" — small wire is the whole point of the unroll. Keys are Hex
    * (`GlobalStateKey.toHex`), so the result drops straight into `MerklePatriciaTrie.withChanges` on the apply side.
    *
    * '''Determinism.''' Pure function of `(mgAddr, prior, next)` — no live-MPT read; the producer and every gl0 verifier that recompute the
    * same `(prior, next)` (`prior = S(N)` from the adopted, chain-linked best-tip; `next` from the committee re-exec over `S(N)`) produce
    * byte-identical diffs.
    */
  def currencyInfoChangeSet[F[_]: Sync: Hasher](
    mgAddr: Address,
    prior: CurrencySnapshotInfo,
    next: CurrencySnapshotInfo
  ): F[ChangeSet] =
    (fullInfoEntriesHex[F](mgAddr, prior), fullInfoEntriesHex[F](mgAddr, next)).mapN { (priorEntries, nextEntries) =>
      // upserts = next entries that are new or whose bytes changed vs prior (PIN-3 minimality).
      val upserts: Map[Hex, Array[Byte]] =
        nextEntries.filter { case (k, v) => !priorEntries.get(k).exists(java.util.Arrays.equals(_, v)) }
      // removals = prior keys absent from next.
      val removals: Set[Hex] = priorEntries.keySet -- nextEntries.keySet
      ChangeSet(upserts, removals)
    }

  /** fieldId-7 metagraph-scope `activeAllowSpends` entries (NOT in `infoEntryBytes`): keyed exactly as the bytes writer does
    * (`hypergraph(ActiveAllowSpends, Some(mgAddr), holder)`), value via the SAME `signedAllowSpendSetCodec` the bytes writer
    * ([[GlobalStateConverter.toAllStateKeyValueBytes]]) uses.
    */
  private def allowSpendEntries(mgAddr: Address, info: CurrencySnapshotInfo): List[(GlobalStateKey, Array[Byte])] =
    info.activeAllowSpends.fold(List.empty[(GlobalStateKey, Array[Byte])])(_.toList.map {
      case (holder, set) =>
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, mgAddr.some, holder) ->
          signedAllowSpendSetCodec.immutableBytes(set).toArray
    })

  /** The full unrolled `Hex key → value bytes` map for one MG's `CurrencySnapshotInfo`: the 8 `Mg*` info sub-fields (the shared
    * [[GlobalStateConverter.infoEntryBytes]] encoder — byte-identical to what gl0 writes) ⊕ the fieldId-7 `activeAllowSpends`
    * ([[allowSpendEntries]]). This is the EXACT entry set [[currencyInfoChangeSet]] diffs over AND [[reconstructInfoFromDiff]] applies
    * onto, so producer (compute-diff) and verifier (apply-diff) stay byte-symmetric by sharing this single definition.
    */
  def fullInfoEntriesHex[F[_]: Sync: Hasher](mgAddr: Address, info: CurrencySnapshotInfo): F[Map[Hex, Array[Byte]]] =
    GlobalStateConverter.infoEntryBytes[F](mgAddr, info).flatMap { mgEntries =>
      (mgEntries ++ allowSpendEntries(mgAddr, info)).traverse {
        case (k, v) => GlobalStateKey.toHex[F](k).map(_ -> v)
      }.map(_.toMap)
    }

  /** APPLY side (step 6 — the inverse of [[currencyInfoChangeSet]]). Reconstruct `next`'s `CurrencySnapshotInfo` by applying the
    * committee's minimal `diff` onto the prior cumulative state `S(N)` WITHOUT re-executing the metagraph: rebuild `S(N)`'s full unrolled
    * entry map ([[fullInfoEntriesHex]] — the SAME bytes the producer diffed against), apply `removals` then `upserts` (upsert-wins,
    * matching [[ChangeSet.merge]] / `MerklePatriciaTrie.withChanges` semantics), then reconstruct via
    * [[GlobalStateConverter.reconstructCurrencyInfoFrom]] over an in-memory reader of the result.
    *
    * '''Round-trip invariant (the step-6 determinism core).''' `reconstructInfoFromDiff(mg, prior, currencyInfoChangeSet(mg, prior, next))
    * \=== next` for every `(prior, next)` — so every gl0 verifier that holds the SAME `S(N)` as the producer reconstructs the
    * byte-identical `next`, and its `currencySnapshotFieldRoots` infoRoot equals the committee-attested `perMetagraphMptRoots` (PIN-1). A
    * verifier whose `S(N)` DIFFERS (trim / reorg — its best-tip lags the producer's) reconstructs a DIFFERENT `next`, the root mismatches,
    * and the checkpoint is rejected (never adopt unverified) — self-healing via defer + re-pull, no double-count, no split.
    */
  def reconstructInfoFromDiff[F[_]: Sync: Hasher](
    mgAddr: Address,
    prior: CurrencySnapshotInfo,
    diff: ChangeSet
  ): F[CurrencySnapshotInfo] =
    fullInfoEntriesHex[F](mgAddr, prior).flatMap { priorEntries =>
      val nextEntries: Map[Hex, Array[Byte]] = (priorEntries -- diff.removals) ++ diff.upserts
      // In-memory `CurrencyInfoReader` over the applied entry map — the same prefix-scan + per-entry `ImmutableCodec` decode `MptStore`
      // does, so `reconstructCurrencyInfoFrom` is oblivious to whether it reads a real trie or this projection.
      val reader: GlobalStateConverter.CurrencyInfoReader[F] = new GlobalStateConverter.CurrencyInfoReader[F] {
        def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] =
          nextEntries.toList.collect {
            case (k, v) if k.value.startsWith(prefix.value) => k -> v
          }.traverse {
            case (k, v) =>
              Sync[F]
                .fromEither(ImmutableCodec[V].fromImmutableBytes(ByteVector(v)).leftMap(e => new RuntimeException(e.toString)))
                .map(k -> _)
          }.map(_.toMap)
      }
      GlobalStateConverter.reconstructCurrencyInfoFrom[F](mgAddr, reader)
    }

  /** Monoid over `merge`. NOT commutative — `combine(a, b) = a.merge(b)` applies `b` after `a`. */
  implicit val monoid: Monoid[ChangeSet] = new Monoid[ChangeSet] {
    def empty: ChangeSet = ChangeSet.empty
    def combine(a: ChangeSet, b: ChangeSet): ChangeSet = a.merge(b)
  }
}
