package io.constellationnetwork.serde

import java.util.UUID

import io.constellationnetwork.currency.dataApplication.FeeTransaction
import io.constellationnetwork.schema.artifact._
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.cluster.SessionToken
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.swap.SwapAmount
import io.constellationnetwork.schema.tokenLock.TokenLockAmount
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.CurrencyAtomCodecs._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.FunSuite

/** Round-trip suite for the batch-1 currency atom codecs. */
object CurrencyAtomCodecsSuite extends FunSuite {

  private val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")

  test("MessageType Owner/Staking round-trip") {
    val o: MessageType = MessageType.Owner
    val s: MessageType = MessageType.Staking
    expect(o.immutableBytes.fromImmutableBytes[MessageType] == Right(o))
      .and(expect(s.immutableBytes.fromImmutableBytes[MessageType] == Right(s)))
      .and(expect(o.immutableBytes != s.immutableBytes))
  }

  test("SessionToken round-trips (wraps Generation = PosLong)") {
    val tk = SessionToken(Generation(PosLong.unsafeFrom(123L)))
    expect(tk.immutableBytes.fromImmutableBytes[SessionToken] == Right(tk))
  }

  test("RoundId round-trips (UUID = 16 bytes)") {
    val r = RoundId(UUID.fromString("12345678-1234-1234-1234-123456789abc"))
    val bytes = r.immutableBytes
    expect(bytes.length == 16L).and(expect(bytes.fromImmutableBytes[RoundId] == Right(r)))
  }

  test("SnapshotVersion accepts '0.0.1' and rejects arbitrary strings") {
    val v = SnapshotVersion(Refined.unsafeApply[String, MatchesRegex["^(0\\.0\\.1|1\\.0\\.0)$"]]("0.0.1"))
    expect(v.immutableBytes.fromImmutableBytes[SnapshotVersion] == Right(v))

    // Build a string-encoded version with an invalid payload and assert decode fails.
    val badBytes = "not-a-version".getBytes("UTF-8")
    val prefix = scodec.bits.ByteVector.fromValidHex(f"${badBytes.length}%04x")
    val combined = prefix ++ scodec.bits.ByteVector.view(badBytes)
    combined.fromImmutableBytes[SnapshotVersion] match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }

  test("CurrencyMessage round-trips") {
    val m = CurrencyMessage(MessageType.Owner, addr, addr, MessageOrdinal(NonNegLong.unsafeFrom(7L)))
    expect(m.immutableBytes.fromImmutableBytes[CurrencyMessage] == Right(m))
  }

  test("FeeTransaction round-trips") {
    val ft = FeeTransaction(addr, addr, Amount(NonNegLong.unsafeFrom(100L)), Hash("1" * 64))
    expect(ft.immutableBytes.fromImmutableBytes[FeeTransaction] == Right(ft))
  }

  test("SpendTransaction round-trips with all optional fields present and absent") {
    val withRefs = SpendTransaction(Some(Hash("a" * 64)), None, SwapAmount(PosLong.unsafeFrom(10L)), addr, addr)
    val noRefs = SpendTransaction(None, None, SwapAmount(PosLong.unsafeFrom(10L)), addr, addr)
    expect(withRefs.immutableBytes.fromImmutableBytes[SpendTransaction] == Right(withRefs))
      .and(expect(noRefs.immutableBytes.fromImmutableBytes[SpendTransaction] == Right(noRefs)))
  }

  test("TokenUnlock round-trips") {
    val t = TokenUnlock(Hash("1" * 64), TokenLockAmount(PosLong.unsafeFrom(5L)), None, addr)
    expect(t.immutableBytes.fromImmutableBytes[TokenUnlock] == Right(t))
  }

  test("AllowSpendExpiration round-trips") {
    val a = AllowSpendExpiration(Hash("2" * 64))
    expect(a.immutableBytes.fromImmutableBytes[AllowSpendExpiration] == Right(a))
  }
}
