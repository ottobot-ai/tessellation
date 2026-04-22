package io.constellationnetwork.serde

import java.util.UUID

import cats.data.{NonEmptyList, NonEmptySet}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{DataApplicationPart, DataApplicationPartV1}
import io.constellationnetwork.currency.schema.globalSnapshotSync.{GlobalSnapshotSync, GlobalSnapshotSyncOrdinal, GlobalSyncView}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.cluster.SessionToken
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.CurrencyRecordCodecs._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.FunSuite

/** Round-trip suite for the batch-3 currency-record codecs. */
object CurrencyRecordCodecsSuite extends FunSuite {

  private val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")
  private def proof = SignatureProof(Id(Hex("cafe")), Signature(Hex("beef")))

  // ---- GlobalSnapshotSync + View -------------------------------------------

  test("GlobalSnapshotSync round-trips") {
    val s = GlobalSnapshotSync(
      parentOrdinal = GlobalSnapshotSyncOrdinal(NonNegLong.unsafeFrom(1L)),
      globalSnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L)),
      globalSnapshotHash = Hash("a" * 64),
      session = SessionToken(Generation(PosLong.unsafeFrom(7L)))
    )
    expect(s.immutableBytes.fromImmutableBytes[GlobalSnapshotSync] == Right(s))
  }

  test("GlobalSyncView round-trips (fixed-width 48 bytes)") {
    val v = GlobalSyncView(
      ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(5L)),
      hash = Hash("b" * 64),
      epochProgress = EpochProgress(NonNegLong.unsafeFrom(10L))
    )
    val bytes = v.immutableBytes
    expect(bytes.length == 48L) and
      expect(bytes.fromImmutableBytes[GlobalSyncView] == Right(v))
  }

  // ---- DataApplicationPart ------------------------------------------------

  test("DataApplicationPartV1 round-trips with empty state + empty blocks") {
    val p = DataApplicationPartV1(Array.emptyByteArray, List.empty, Hash.empty)
    val bytes = p.immutableBytes
    val decoded = bytes.fromImmutableBytes[DataApplicationPartV1].toOption.get
    expect(decoded.onChainState.sameElements(p.onChainState)) and
      expect(decoded.blocks.zip(p.blocks).forall { case (a, b) => a.sameElements(b) }) and
      expect(decoded.calculatedStateProof == p.calculatedStateProof)
  }

  test("DataApplicationPart round-trips with non-trivial state + optional updateHashes") {
    val p = DataApplicationPart(
      onChainState = Array[Byte](1, 2, 3, 4, 5),
      blocks = List(Array[Byte](0x0a, 0x0b), Array[Byte](0x0c)),
      calculatedStateProof = Hash("d" * 64),
      updateHashes = Some(SortedSet(Hash("e" * 64)))
    )
    val decoded = p.immutableBytes.fromImmutableBytes[DataApplicationPart].toOption.get
    expect(decoded.onChainState.sameElements(p.onChainState)) and
      expect(decoded.blocks.size == p.blocks.size) and
      expect(decoded.calculatedStateProof == p.calculatedStateProof) and
      expect(decoded.updateHashes == p.updateHashes)
  }

  // ---- Tips ---------------------------------------------------------------

  private def br = BlockReference(Height(NonNegLong.unsafeFrom(1L)), ProofsHash("f" * 64))

  test("ActiveTip round-trips") {
    val t = ActiveTip(br, NonNegLong.unsafeFrom(3L), SnapshotOrdinal(NonNegLong.unsafeFrom(2L)))
    expect(t.immutableBytes.fromImmutableBytes[ActiveTip] == Right(t))
  }

  test("DeprecatedTip round-trips") {
    val t = DeprecatedTip(br, SnapshotOrdinal(NonNegLong.unsafeFrom(4L)))
    expect(t.immutableBytes.fromImmutableBytes[DeprecatedTip] == Right(t))
  }

  test("SnapshotTips round-trips") {
    val t = SnapshotTips(
      deprecated = SortedSet(DeprecatedTip(br, SnapshotOrdinal(NonNegLong.unsafeFrom(4L)))),
      remainedActive = SortedSet(ActiveTip(br, NonNegLong.unsafeFrom(1L), SnapshotOrdinal(NonNegLong.unsafeFrom(2L))))
    )
    expect(t.immutableBytes.fromImmutableBytes[SnapshotTips] == Right(t))
  }

  test("BlockAsActiveTip round-trips") {
    val block = Block(
      parent = NonEmptyList.of(br),
      transactions = NonEmptySet.of(
        Signed(
          Transaction(
            addr,
            addr,
            TransactionAmount(PosLong.unsafeFrom(1L)),
            TransactionFee(NonNegLong.unsafeFrom(0L)),
            TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(0L)), Hash("0" * 64)),
            TransactionSalt(0L)
          ),
          NonEmptySet.of(proof)
        )
      )
    )
    val t = BlockAsActiveTip(Signed(block, NonEmptySet.of(proof)), NonNegLong.unsafeFrom(2L))
    expect(t.immutableBytes.fromImmutableBytes[BlockAsActiveTip] == Right(t))
  }

  // ---- AllowSpendBlock / TokenLockBlock -----------------------------------

  test("AllowSpendBlock round-trips") {
    val asp = AllowSpend(addr, addr, None, SwapAmount(PosLong.unsafeFrom(1L)), AllowSpendFee(NonNegLong.unsafeFrom(0L)),
      AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(0L)), Hash("0" * 64)),
      EpochProgress(NonNegLong.unsafeFrom(0L)), Nil)
    val b = AllowSpendBlock(RoundId(UUID.fromString("00000000-0000-0000-0000-000000000001")),
      NonEmptySet.of(Signed(asp, NonEmptySet.of(proof))))
    expect(b.immutableBytes.fromImmutableBytes[AllowSpendBlock] == Right(b))
  }

  test("TokenLockBlock round-trips") {
    val tl = TokenLock(addr, TokenLockAmount(PosLong.unsafeFrom(1L)), TokenLockFee(NonNegLong.unsafeFrom(0L)),
      TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(0L)), Hash("0" * 64)),
      None, None, None)
    val b = TokenLockBlock(RoundId(UUID.fromString("00000000-0000-0000-0000-000000000002")),
      NonEmptySet.of(Signed(tl, NonEmptySet.of(proof))))
    expect(b.immutableBytes.fromImmutableBytes[TokenLockBlock] == Right(b))
  }

  // witness so unused-import lint doesn't strip helpers
  locally { val _ = (Hashed, SortedMap.empty[String, Int]) }
}
