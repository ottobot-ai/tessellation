package io.constellationnetwork.serde

import cats.syntax.option._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.{SystemIndexDelta, TokenLockExpiryKey}
import io.constellationnetwork.schema.nakamoto.follow.GlobalChangeSetResponse
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.GlobalChangeSetResponseCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Round-trip suite (test (a) of task #12 slice 3) for the [[GlobalChangeSetResponse]] wire codec — the gl0 → currency-l0 (ml0) changeset
  * transport. Confirms the EXPLICIT, hand-written scodec codec round-trips the response envelope (`latestOrdinal`, `baseOrdinal`, ordered
  * `deltas`), including a populated per-ordinal [[StateChangesAccumulator]] delta whose binary is itself scodec (the canonical
  * [[io.constellationnetwork.serde.codecs.instances.StateChangesAccumulatorCodec]], reused here — never Circe/auto-derived).
  */
object GlobalChangeSetResponseCodecSuite extends FunSuite {

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

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

  test("empty changeset (no deltas, baseOrdinal Some) round-trips") {
    val r = GlobalChangeSetResponse(latestOrdinal = ord(7L), baseOrdinal = ord(7L).some, deltas = Nil)
    expect(r.immutableBytes.fromImmutableBytes[GlobalChangeSetResponse] == Right(r))
  }

  test("fallback changeset (no deltas, baseOrdinal None) round-trips") {
    val r = GlobalChangeSetResponse(latestOrdinal = ord(42L), baseOrdinal = none, deltas = Nil)
    expect(r.immutableBytes.fromImmutableBytes[GlobalChangeSetResponse] == Right(r))
  }

  test("populated changeset (contiguous deltas with real accumulators) round-trips") {
    val r = GlobalChangeSetResponse(
      latestOrdinal = ord(12L),
      baseOrdinal = ord(9L).some,
      deltas = List(
        ord(10L) -> sampleAccumulator(10L),
        ord(11L) -> sampleAccumulator(11L),
        ord(12L) -> sampleAccumulator(12L)
      )
    )
    val decoded = r.immutableBytes.fromImmutableBytes[GlobalChangeSetResponse]
    expect(decoded == Right(r)) &&
    // the ordered-list contract is preserved (ordinals stay in the produced order)
    expect(decoded.map(_.deltas.map(_._1)) == Right(List(ord(10L), ord(11L), ord(12L))))
  }
}
