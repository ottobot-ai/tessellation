package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct

import weaver.SimpleIOSuite

object OperatorConsensusKeyRegistrySuite extends SimpleIOSuite {

  private def peer(byte: String): PeerId = PeerId(Hex(byte * 64))

  private def keys(operator: PeerId, kesByte: Byte, vrfByte: Byte): OperatorConsensusKeys =
    OperatorConsensusKeys(
      operatorPeerId = operator,
      kes = KesRegistryEntry(VerificationKeyKesProduct(Array.fill(32)(kesByte), step = 0), offset = 0L),
      vrfPublicKey = VrfPublicKey.fromBytes(Array.fill(32)(vrfByte)),
      effectiveFromPeriod = EtaPeriod.Zero,
      registration = None
    )

  test("one atomic record drives both immutable projections") {
    val operator = peer("01")
    val registry = OperatorConsensusKeyRegistry.make[IO](Map(operator -> keys(operator, 0x11, 0x22)))

    for {
      pair <- registry.get(operator)
      kes <- registry.kesRegistry.getKesVk(operator)
      vrf <- registry.vrfRegistry.getVrfVk(operator)
      _ <- IO {
        kes.foreach(_.vk.value(0) = 0x7f.toByte)
        vrf.foreach(_(0) = 0x7f.toByte)
      }
      pairAfterMutation <- registry.get(operator)
    } yield
      expect(pair.exists(_.kes.vk.value.head == 0x11.toByte)) &&
        expect(pair.exists(_.vrfPublicKey.toBytes.head == 0x22.toByte)) &&
        expect(pairAfterMutation.exists(_.kes.vk.value.head == 0x11.toByte)) &&
        expect(pairAfterMutation.exists(_.vrfPublicKey.toBytes.head == 0x22.toByte))
  }

  test("registry rejects a map identity that differs from the signed operator identity") {
    val mapIdentity = peer("01")
    val recordIdentity = peer("02")

    IO(
      OperatorConsensusKeyRegistry.make[IO](Map(mapIdentity -> keys(recordIdentity, 0x11, 0x22)))
    ).attempt.map(result => expect(result.swap.exists(_.getMessage.contains("Malformed atomic operator-key record"))))
  }

  test("registration=None is reserved for a period-zero committed genesis pair") {
    val operator = peer("01")
    val futureWithoutRecord = keys(operator, 0x11, 0x22).copy(
      kes = keys(operator, 0x11, 0x22).kes.copy(offset = 2L),
      effectiveFromPeriod = EtaPeriod(2L),
      registration = None
    )

    IO(OperatorConsensusKeyRegistry.make[IO](Map(operator -> futureWithoutRecord))).attempt.map { result =>
      expect(result.swap.exists(_.getMessage.contains("Malformed atomic operator-key record")))
    }
  }

  test("registry rejects KES or VRF key reuse across operators") {
    val first = peer("01")
    val second = peer("02")

    val duplicateKes = IO(
      OperatorConsensusKeyRegistry.make[IO](
        Map(first -> keys(first, 0x11, 0x21), second -> keys(second, 0x11, 0x22))
      )
    ).attempt
    val duplicateVrf = IO(
      OperatorConsensusKeyRegistry.make[IO](
        Map(first -> keys(first, 0x11, 0x22), second -> keys(second, 0x12, 0x22))
      )
    ).attempt

    (duplicateKes, duplicateVrf).mapN {
      case (kesResult, vrfResult) =>
        expect(kesResult.swap.exists(_.getMessage.contains("KES key reuse"))) &&
        expect(vrfResult.swap.exists(_.getMessage.contains("VRF key reuse")))
    }
  }
}
