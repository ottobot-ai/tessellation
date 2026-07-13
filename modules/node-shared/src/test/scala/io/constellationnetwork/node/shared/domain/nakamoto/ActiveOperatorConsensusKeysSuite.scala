package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptySet
import cats.effect.IO

import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object ActiveOperatorConsensusKeysSuite extends SimpleIOSuite {

  private val operator = PeerId(Hex("01" * 64))

  private val genesisKeys = OperatorConsensusKeys(
    operator,
    KesRegistryEntry(VerificationKeyKesProduct(Array.fill(32)(0x11.toByte), 0), 0L),
    VrfPublicKey.fromBytes(Array.fill(32)(0x22.toByte)),
    EtaPeriod.Zero,
    None
  )

  private val runtimeRecord = {
    val cert = KesRegistrationCert(
      operatorPeerId = operator,
      kesMasterVK = Hex("31" * 32),
      kesMasterVKStep = 0,
      offset = 2L,
      vrfPublicKey = Hex("41" * 32),
      effectiveFromPeriod = EtaPeriod(2L),
      registrationParentHash = Hash("aa" * 32),
      ordinal = KesRegistrationOrdinal(NonNegLong.unsafeFrom(1L)),
      parent = KesRegistrationReference.empty
    )
    val proof = SignatureProof(Id(operator.value), Signature(Hex("7f" * 64)))
    KesRegistrationRecord(
      Signed(cert, NonEmptySet.one(proof)),
      SnapshotOrdinal(NonNegLong.unsafeFrom(9L))
    )
  }

  test("current-view compatibility resolver accepts a committed genesis pair") {
    val registry = OperatorConsensusKeyRegistry.make[IO](Map(operator -> genesisKeys))

    ActiveOperatorConsensusKeys.resolve(registry, operator, EtaPeriod(10L)).map { resolved =>
      expect(resolved.exists(_.registration.isEmpty))
    }
  }

  test("current-view compatibility resolver rejects every runtime registration") {
    val runtimeKeys = OperatorConsensusKeys(
      operator,
      KesRegistryEntry(VerificationKeyKesProduct(Hex("31" * 32).toBytes, 0), 2L),
      VrfPublicKey.fromBytes(Hex("41" * 32).toBytes),
      EtaPeriod(2L),
      Some(runtimeRecord)
    )
    val registry = OperatorConsensusKeyRegistry.make[IO](Map(operator -> runtimeKeys))

    ActiveOperatorConsensusKeys.resolve(registry, operator, EtaPeriod(10L)).map { resolved =>
      expect(resolved.isEmpty)
    }
  }
}
