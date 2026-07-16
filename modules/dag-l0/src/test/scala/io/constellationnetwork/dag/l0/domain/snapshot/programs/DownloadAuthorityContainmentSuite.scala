package io.constellationnetwork.dag.l0.domain.snapshot.programs

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.snapshot.{PeerSelect, PeerSelection}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.peer.{L0Peer, PeerId}
import io.constellationnetwork.schema.snapshot.SnapshotMetadata
import io.constellationnetwork.schema.{GlobalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed

import com.comcast.ip4s.{Host, Port}
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object DownloadAuthorityContainmentSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: Resource[IO, Res] = for {
    securityProvider <- SecurityProvider.forAsync[IO]
    implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    hasher = Hasher.forJson[IO]
  } yield (jsonSerializer, hasher, securityProvider)

  private val selectedPeer = L0Peer(
    PeerId(Hex("selected-peer")),
    Host.fromString("127.0.0.1").get,
    Port.fromInt(9000).get
  )

  private def ordinal(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))

  test("metadata equivocation rejects continuation past the bound-metadata helper") { _ =>
    val selectedOrdinal = ordinal(100L)
    val selectedHash = Hash("selected-hash")
    val selection = PeerSelection(selectedPeer, selectedOrdinal, selectedHash)
    val peerSelect = new PeerSelect[IO] {
      def select: IO[PeerSelection] = selection.pure[IO]
    }
    val equivocated = SnapshotMetadata(ordinal(99L), Hash("equivocated-hash"), Hash("parent"))

    for {
      destructiveCalls <- Ref.of[IO, List[String]](List.empty)
      result <- Download
        .selectBoundMetadata(peerSelect, _ => equivocated.pure[IO])
        .flatTap(_ => destructiveCalls.set(List("snapshots", "checkpoints", "mpt", "caches", "mempool")))
        .attempt
      calls <- destructiveCalls.get
    } yield
      expect(result.swap.exists(_.isInstanceOf[Download.SnapshotMetadataSelectionMismatch])) &&
        expect(calls.isEmpty)
  }

  test("invalid genesis signature rejects continuation past genesis validation") { res =>
    implicit val (jsonSerializer, hasher, securityProvider) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      signed <- Signed.forAsyncHasher[IO, GlobalSnapshot](GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue), keyPair)
      expected <- signed.toHashed[IO]
      tampered = signed.copy(value = signed.value.copy(lastSnapshotHash = Hash("tampered")))
      destructiveCalls <- Ref.of[IO, List[String]](List.empty)
      result <- Download
        .validateGenesis(tampered, expected.ordinal, expected.hash)
        .flatTap(_ => destructiveCalls.set(List("write-genesis", "install-context")))
        .attempt
      calls <- destructiveCalls.get
    } yield
      expect(result.swap.exists(_.isInstanceOf[Signed.InvalidSignatureForHash[_]])) &&
        expect(calls.isEmpty)
  }

  test("wrong signed genesis identity rejects continuation past genesis validation") { res =>
    implicit val (jsonSerializer, hasher, securityProvider) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      signed <- Signed.forAsyncHasher[IO, GlobalSnapshot](GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue), keyPair)
      hashed <- signed.toHashed[IO]
      destructiveCalls <- Ref.of[IO, List[String]](List.empty)
      result <- Download
        .validateGenesis(signed, hashed.ordinal, Hash("different-genesis"))
        .flatTap(_ => destructiveCalls.set(List("write-genesis", "install-context")))
        .attempt
      calls <- destructiveCalls.get
    } yield
      expect(result.swap.exists(_.isInstanceOf[Download.GenesisSnapshotIdentityMismatch])) &&
        expect(calls.isEmpty)
  }
}
