package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardBinaryBuffer]] — the per-shard raw-binary accumulator (EXECUTION-SHARDING R-1, the inversion intake).
  *
  * Coverage:
  *   - buffer then snapshot round-trips per MG in insertion order.
  *   - dedup by binary hash (idempotent re-buffer, even across MGs).
  *   - per-MG grouping with multiple MGs.
  *   - cap rejection: at cap, new distinct binaries are dropped (chain-safe overflow), already-buffered ones remain.
  *   - empty buffer ⇒ empty snapshot.
  */
object ShardBinaryBufferSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp)

  private def mkAddress(label: String): Address =
    Address.fromBytes(label.getBytes("UTF-8"))

  private def hashFromString(s: String): Hash =
    Hash.fromBytes(s.getBytes("UTF-8"))

  // Distinct content per (label, idx) ⇒ distinct canonical hash. `lastSnapshotHash` is irrelevant to the
  // buffer (it dedups + groups; chain-link ordering is the producer's job), so we vary it for realism only.
  private def mkSignedBinary(label: String, idx: Int): Signed[StateChannelSnapshotBinary] = {
    import cats.data.NonEmptySet
    import io.constellationnetwork.schema.ID.Id
    import io.constellationnetwork.security.hex.Hex
    val body = StateChannelSnapshotBinary(
      lastSnapshotHash = hashFromString(s"$label-parent-$idx"),
      content = s"$label-content-$idx".getBytes("UTF-8"),
      fee = SnapshotFee(NonNegLong.unsafeFrom(0L))
    )
    val sentinelProof = SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(body, NonEmptySet.of(sentinelProof))
  }

  private def hashOf(b: Signed[StateChannelSnapshotBinary])(implicit h: Hasher[IO]): IO[Hash] = {
    import io.constellationnetwork.security.signature.Signed.SignedOps
    SignedOps(b).toHashed[IO].map(_.hash)
  }

  test("empty buffer ⇒ empty snapshotPending") { res =>
    implicit val (h, _) = res
    val _ = h
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      pending <- buffer.snapshotPending
    } yield expect(pending.isEmpty)
  }

  test("buffer one binary ⇒ snapshotPending returns it under its MG") { res =>
    implicit val (h, _) = res
    val mg = mkAddress("mg-a")
    val b = mkSignedBinary("mg-a", 0)
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      _ <- buffer.bufferBinary(mg, b)
      pending <- buffer.snapshotPending
    } yield
      expect.all(
        pending.size == 1,
        pending.get(mg).exists(_.toList.size == 1),
        pending.get(mg).exists(_.head == b)
      )
  }

  test("multiple binaries for one MG preserve insertion order") { res =>
    implicit val (h, _) = res
    val mg = mkAddress("mg-a")
    val b0 = mkSignedBinary("mg-a", 0)
    val b1 = mkSignedBinary("mg-a", 1)
    val b2 = mkSignedBinary("mg-a", 2)
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      _ <- buffer.bufferBinary(mg, b0)
      _ <- buffer.bufferBinary(mg, b1)
      _ <- buffer.bufferBinary(mg, b2)
      pending <- buffer.snapshotPending
    } yield
      expect(pending.get(mg).map(_.toList) == Some(List(b0, b1, b2)))
  }

  test("dedup by binary hash: re-buffering the same binary is a no-op") { res =>
    implicit val (h, _) = res
    val mg = mkAddress("mg-a")
    val b = mkSignedBinary("mg-a", 0)
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      _ <- buffer.bufferBinary(mg, b)
      _ <- buffer.bufferBinary(mg, b)
      _ <- buffer.bufferBinary(mg, b)
      pending <- buffer.snapshotPending
    } yield expect(pending.get(mg).map(_.toList.size) == Some(1))
  }

  test("multiple MGs are grouped separately") { res =>
    implicit val (h, _) = res
    val mgA = mkAddress("mg-a")
    val mgB = mkAddress("mg-b")
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      _ <- buffer.bufferBinary(mgA, mkSignedBinary("mg-a", 0))
      _ <- buffer.bufferBinary(mgB, mkSignedBinary("mg-b", 0))
      _ <- buffer.bufferBinary(mgA, mkSignedBinary("mg-a", 1))
      pending <- buffer.snapshotPending
    } yield
      expect.all(
        pending.size == 2,
        pending.get(mgA).map(_.toList.size) == Some(2),
        pending.get(mgB).map(_.toList.size) == Some(1)
      )
  }

  test("cap: at cap, new distinct binaries are rejected; buffered ones remain") { res =>
    implicit val (h, _) = res
    val mg = mkAddress("mg-a")
    val b0 = mkSignedBinary("mg-a", 0)
    val b1 = mkSignedBinary("mg-a", 1)
    val b2 = mkSignedBinary("mg-a", 2) // beyond cap=2
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 2)
      _ <- buffer.bufferBinary(mg, b0)
      _ <- buffer.bufferBinary(mg, b1)
      _ <- buffer.bufferBinary(mg, b2) // rejected (chain-safe overflow)
      pending <- buffer.snapshotPending
      h0 <- hashOf(b0)
      h1 <- hashOf(b1)
      h2 <- hashOf(b2)
      bufferedHashes <- pending.get(mg).toList.flatMap(_.toList).traverse(hashOf)
    } yield
      expect.all(
        pending.get(mg).map(_.toList.size) == Some(2),
        bufferedHashes.toSet == Set(h0, h1),
        !bufferedHashes.toSet.contains(h2)
      )
  }
}
