package io.constellationnetwork.serde

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Round-trip suite for the currency state-proof and info codecs. */
object CurrencySnapshotInfoCodecsSuite extends FunSuite {

  private val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")

  // ---- StateProof ---------------------------------------------------------

  test("CurrencySnapshotStateProofV1 round-trips") {
    val p = CurrencySnapshotStateProofV1(Hash("a" * 64), Hash("b" * 64))
    expect(p.immutableBytes.fromImmutableBytes[CurrencySnapshotStateProofV1] == Right(p))
  }

  test("Current CurrencySnapshotStateProof round-trips with all options present") {
    val p = CurrencySnapshotStateProof(
      Hash("a" * 64),
      Hash("b" * 64),
      Some(Hash("c" * 64)),
      Some(Hash("d" * 64)),
      Some(Hash("e" * 64)),
      Some(Hash("f" * 64)),
      Some(Hash("0" * 64)),
      Some(Hash("1" * 64)),
      Some(Hash("2" * 64))
    )
    expect(p.immutableBytes.fromImmutableBytes[CurrencySnapshotStateProof] == Right(p))
  }

  test("Current CurrencySnapshotStateProof round-trips with all options absent") {
    val p = CurrencySnapshotStateProof(
      Hash("a" * 64),
      Hash("b" * 64),
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
    expect(p.immutableBytes.fromImmutableBytes[CurrencySnapshotStateProof] == Right(p))
  }

  // ---- Info ---------------------------------------------------------------

  test("CurrencySnapshotInfoV1 round-trips") {
    val info = CurrencySnapshotInfoV1(
      lastTxRefs = SortedMap(addr -> TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(1L)), Hash("a" * 64))),
      balances = SortedMap(addr -> Balance(NonNegLong.unsafeFrom(100L)))
    )
    expect(info.immutableBytes.fromImmutableBytes[CurrencySnapshotInfoV1] == Right(info))
  }

  test("Current CurrencySnapshotInfo round-trips with all optional maps absent") {
    val info = CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = SortedMap.empty,
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )
    expect(info.immutableBytes.fromImmutableBytes[CurrencySnapshotInfo] == Right(info))
  }

  test("Current CurrencySnapshotInfo round-trips with non-trivial optional maps") {
    val info = CurrencySnapshotInfo(
      lastTxRefs = SortedMap(addr -> TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(5L)), Hash("1" * 64))),
      balances = SortedMap(addr -> Balance(NonNegLong.unsafeFrom(500L))),
      lastMessages = Some(SortedMap.empty),
      lastFeeTxRefs = Some(SortedMap.empty),
      lastAllowSpendRefs = Some(SortedMap.empty),
      activeAllowSpends = Some(SortedMap.empty),
      globalSnapshotSyncView = Some(SortedMap.empty),
      lastTokenLockRefs = Some(SortedMap.empty),
      activeTokenLocks = Some(SortedMap.empty)
    )
    expect(info.immutableBytes.fromImmutableBytes[CurrencySnapshotInfo] == Right(info))
  }
}
