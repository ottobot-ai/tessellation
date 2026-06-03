package io.constellationnetwork.serde

import cats.syntax.option._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.{SystemIndexDelta, TokenLockExpiryKey}
import io.constellationnetwork.schema.nakamoto.follow.{GlobalChangeSetDelta, GlobalChangeSetResponse}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.smt.{AbsenceWitness, SmtProof, SmtSibling}
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.GlobalChangeSetResponseCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Round-trip suite (test (a) of task #12 slice 3; extended in Slice B) for the [[GlobalChangeSetResponse]] wire codec — the gl0 →
  * currency-l0 (ml0) changeset transport. Confirms the EXPLICIT, hand-written scodec codec round-trips the response envelope
  * (`latestOrdinal`, `baseOrdinal`, ordered `deltas`), including a populated per-ordinal [[GlobalChangeSetDelta]] whose accumulator binary
  * is itself scodec (the canonical [[io.constellationnetwork.serde.codecs.instances.StateChangesAccumulatorCodec]]) and whose optional SMT
  * proofs round-trip via the canonical [[io.constellationnetwork.serde.codecs.instances.SmtProofCodec]] — never Circe/auto-derived.
  */
object GlobalChangeSetResponseCodecSuite extends FunSuite {

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  private def h(n: Int): Hash = Hash("00000000000000000000000000000000000000000000000000000000000000" + f"$n%02x")

  /** A non-trivial accumulator: a populated expiry-index bucket + a removed-key set, mirroring the populated fixture in
    * `StateChangesAccumulatorCodecSuite` so the embedded delta exercises real partition + system-index + removed-key bytes.
    */
  private def sampleAccumulator(seed: Long): StateChangesAccumulator = {
    val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")
    val hash: Hash = Hash("00000000000000000000000000000000000000000000000000000000000000" + f"${seed % 256}%02x")
    StateChangesAccumulator(
      balances = SortedMap(addr -> Balance(NonNegLong.unsafeFrom(1000L + seed))),
      tokenLockExpiryIndex = SystemIndexDelta.EpochBucket[TokenLockExpiryKey](
        adds = SortedMap(EpochProgress(NonNegLong.unsafeFrom(10L + seed)) -> Set(TokenLockExpiryKey(addr, hash))),
        removes = SortedMap.empty
      ),
      removedTokenLockKeys = Set(addr)
    )
  }

  private val sampleInclusion: SmtProof =
    SmtProof.Inclusion(
      key = Hex("00000000000000ab"),
      value = Array[Byte](1, 2, 3, 4, -1, 0),
      valueDigest = h(7),
      siblings = List(SmtSibling(h(1)), SmtSibling(Hash.empty), SmtSibling(h(3)))
    )

  private val sampleAbsenceDefault: SmtProof =
    SmtProof.Absence(key = Hex("00000000000000cd"), witness = AbsenceWitness.Default, siblings = List(SmtSibling(h(9))))

  private val sampleAbsenceOtherLeaf: SmtProof =
    SmtProof.Absence(
      key = Hex("00000000000000ef"),
      witness = AbsenceWitness.OtherLeaf(occupyingKey = Hex("00000000000000aa"), occupyingDataDigest = h(42)),
      siblings = List(SmtSibling(h(5)), SmtSibling(h(6)))
    )

  test("empty changeset (no deltas, baseOrdinal Some) round-trips") {
    val r = GlobalChangeSetResponse(latestOrdinal = ord(7L), baseOrdinal = ord(7L).some, deltas = Nil)
    expect(r.immutableBytes.fromImmutableBytes[GlobalChangeSetResponse] == Right(r))
  }

  test("fallback changeset (no deltas, baseOrdinal None) round-trips") {
    val r = GlobalChangeSetResponse(latestOrdinal = ord(42L), baseOrdinal = none, deltas = Nil)
    expect(r.immutableBytes.fromImmutableBytes[GlobalChangeSetResponse] == Right(r))
  }

  test("populated changeset (contiguous deltas, mix of present/absent SMT proofs) round-trips") {
    val r = GlobalChangeSetResponse(
      latestOrdinal = ord(12L),
      baseOrdinal = ord(9L).some,
      deltas = List(
        // delta with an Inclusion inclusion-proof and an absence-at-parent (OtherLeaf)
        GlobalChangeSetDelta(ord(10L), sampleAccumulator(10L), sampleInclusion.some, sampleAbsenceOtherLeaf.some),
        // delta with an Inclusion proof and an absence-at-parent (Default witness)
        GlobalChangeSetDelta(ord(11L), sampleAccumulator(11L), sampleInclusion.some, sampleAbsenceDefault.some),
        // warmup-style delta: both proofs None
        GlobalChangeSetDelta(ord(12L), sampleAccumulator(12L), none, none)
      )
    )
    val decoded = r.immutableBytes.fromImmutableBytes[GlobalChangeSetResponse]
    // Canonicality: re-encoding the decoded value reproduces the original bytes. Compared via bytes (not `==`) because
    // `SmtProof.Inclusion.value: Array[Byte]` has reference equality, so a structural `== Right(r)` would spuriously fail.
    expect(decoded.map(_.immutableBytes) == Right(r.immutableBytes)) &&
    // the ordered-list contract is preserved (ordinals stay in the produced order)
    expect(decoded.map(_.deltas.map(_.ordinal)) == Right(List(ord(10L), ord(11L), ord(12L)))) &&
    // the SMT proofs decode to structurally-equal proofs (SmtProof.Eq compares value bytes by content)
    expect(decoded.toOption.flatMap(_.deltas.headOption).flatMap(_.smtInclusionProof).exists(p => SmtProof.eq.eqv(p, sampleInclusion)))
  }
}
