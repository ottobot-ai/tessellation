package io.constellationnetwork.node.shared.domain.nakamoto

import java.security.MessageDigest

import cats.data.NonEmptySet
import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** Generated frozen-genesis qualification vectors shared by the semantic manifest.
  *
  * This suite exercises the real genesis-only atomic-pair resolver. It deliberately does not pretend that snapshot intake, admission,
  * checkpoint production, tower verification, and slashing share one runtime adapter. The manifest maps those consumers to their own
  * executable negative suites and records the authority effects that each adapter must keep at zero.
  */
object OperatorConsensusKeyQualificationMatrix extends SimpleIOSuite {

  sealed abstract class EffectObligation(val manifestName: String)

  object EffectObligation {
    case object Draw extends EffectObligation("Draw")
    case object EtaLookup extends EffectObligation("EtaLookup")
    case object PossessionProof extends EffectObligation("PossessionProof")
    case object Replay extends EffectObligation("Replay")
    case object KesSign extends EffectObligation("KesSign")
    case object EdSign extends EffectObligation("EdSign")
    case object AggregatorRecord extends EffectObligation("AggregatorRecord")
    case object TrackerRecord extends EffectObligation("TrackerRecord")
    case object Store extends EffectObligation("Store")
    case object Adopt extends EffectObligation("Adopt")
    case object Publish extends EffectObligation("Publish")
    case object Slash extends EffectObligation("Slash")

    val all: List[EffectObligation] = List(
      Draw,
      EtaLookup,
      PossessionProof,
      Replay,
      KesSign,
      EdSign,
      AggregatorRecord,
      TrackerRecord,
      Store,
      Adopt,
      Publish,
      Slash
    )

    val byManifestName: Map[String, EffectObligation] = all.map(probe => probe.manifestName -> probe).toMap
  }

  sealed abstract class AttackIdentity(val name: String)

  object AttackIdentity {
    case object AbsentOperator extends AttackIdentity("AbsentOperator")
    case object RegisteredPairSubstitutedForAuthority extends AttackIdentity("RegisteredPairSubstitutedForAuthority")
    case object RuntimeShapedPairInFrozenView extends AttackIdentity("RuntimeShapedPairInFrozenView")
    case object MalformedWireVrfKey extends AttackIdentity("MalformedWireVrfKey")

    val all: List[AttackIdentity] =
      List(AbsentOperator, RegisteredPairSubstitutedForAuthority, RuntimeShapedPairInFrozenView, MalformedWireVrfKey)
  }

  private final case class Candidate(
    peerId: PeerId,
    carriedKesMasterKey: Array[Byte],
    carriedVrfKey: Array[Byte],
    authority: OperatorConsensusKeyRegistry[IO]
  )

  test("generated qualification population keeps loader-registered B outside A's frozen authority view") {
    CanonicalOperatorConsensusFixture.makePopulation(2).use { population =>
      val authorityA :: registeredControlB :: Nil = population.operators

      for {
        completePopulation <- population.operatorKeyRegistry.list
        authorityAView <- authorityA.operatorKeyRegistry.list
      } yield
        expect.all(
          completePopulation.keySet == Set(authorityA.resolvedPair.operatorPeerId, registeredControlB.resolvedPair.operatorPeerId),
          authorityAView.keySet == Set(authorityA.resolvedPair.operatorPeerId),
          !authorityAView.contains(registeredControlB.resolvedPair.operatorPeerId),
          !MessageDigest.isEqual(
            authorityA.resolvedPair.kes.vk.value,
            registeredControlB.resolvedPair.kes.vk.value
          ),
          !MessageDigest.isEqual(
            authorityA.resolvedPair.vrfPublicKey.toBytes,
            registeredControlB.resolvedPair.vrfPublicKey.toBytes
          )
        )
    }
  }

  test("generated frozen-view resolver rejects absent, substituted, unrooted runtime-shaped, and malformed identities") {
    CanonicalOperatorConsensusFixture.makePopulation(2).use { population =>
      val authorityA :: registeredControlB :: Nil = population.operators
      val activePeriod = EtaPeriod(10L)
      val runtime = runtimeShapedPair(authorityA.resolvedPair, EtaPeriod(2L))
      val runtimeAuthority = OperatorConsensusKeyRegistry.make[IO](Map(runtime.operatorPeerId -> runtime))

      val positive = Candidate(
        authorityA.resolvedPair.operatorPeerId,
        authorityA.resolvedPair.kes.vk.value,
        authorityA.resolvedPair.vrfPublicKey.toBytes,
        authorityA.operatorKeyRegistry
      )
      val attacks = List(
        AttackIdentity.AbsentOperator -> Candidate(
          registeredControlB.resolvedPair.operatorPeerId,
          registeredControlB.resolvedPair.kes.vk.value,
          registeredControlB.resolvedPair.vrfPublicKey.toBytes,
          authorityA.operatorKeyRegistry
        ),
        AttackIdentity.RegisteredPairSubstitutedForAuthority -> Candidate(
          authorityA.resolvedPair.operatorPeerId,
          registeredControlB.resolvedPair.kes.vk.value,
          registeredControlB.resolvedPair.vrfPublicKey.toBytes,
          authorityA.operatorKeyRegistry
        ),
        AttackIdentity.RuntimeShapedPairInFrozenView -> Candidate(
          runtime.operatorPeerId,
          runtime.kes.vk.value,
          runtime.vrfPublicKey.toBytes,
          runtimeAuthority
        ),
        AttackIdentity.MalformedWireVrfKey -> Candidate(
          authorityA.resolvedPair.operatorPeerId,
          authorityA.resolvedPair.kes.vk.value,
          Array.fill[Byte](31)(0x7f.toByte),
          authorityA.operatorKeyRegistry
        )
      )

      for {
        positiveResult <- qualifies(positive, activePeriod)
        attackResults <- attacks.traverse { case (attack, candidate) => qualifies(candidate, activePeriod).tupleLeft(attack) }
      } yield
        expect.all(
          positiveResult,
          attackResults.map(_._1).toSet == AttackIdentity.all.toSet,
          attackResults.forall { case (_, accepted) => !accepted }
        )
    }
  }

  pureTest("zero-effect obligation vocabulary is complete and stable") {
    expect.same(
      Set(
        "Draw",
        "EtaLookup",
        "PossessionProof",
        "Replay",
        "KesSign",
        "EdSign",
        "AggregatorRecord",
        "TrackerRecord",
        "Store",
        "Adopt",
        "Publish",
        "Slash"
      ),
      EffectObligation.byManifestName.keySet
    )
  }

  private def qualifies(candidate: Candidate, artifactPeriod: EtaPeriod): IO[Boolean] =
    ActiveOperatorConsensusKeys
      .resolve(candidate.authority, candidate.peerId, artifactPeriod)
      .map(
        _.exists { resolved =>
          MessageDigest.isEqual(resolved.kes.vk.value, candidate.carriedKesMasterKey) &&
          MessageDigest.isEqual(resolved.vrfPublicKey.toBytes, candidate.carriedVrfKey)
        }
      )

  /** Deliberately unrooted runtime-shaped negative. The dummy proof is not loader- or cryptographically validated. */
  private def runtimeShapedPair(genesis: OperatorConsensusKeys, effectiveFrom: EtaPeriod): OperatorConsensusKeys = {
    val cert = KesRegistrationCert(
      operatorPeerId = genesis.operatorPeerId,
      kesMasterVK = Hex.fromBytes(genesis.kes.vk.value),
      kesMasterVKStep = genesis.kes.vk.step,
      offset = effectiveFrom.value,
      vrfPublicKey = Hex.fromBytes(genesis.vrfPublicKey.toBytes),
      effectiveFromPeriod = effectiveFrom,
      registrationParentHash = Hash("aa" * 32),
      ordinal = KesRegistrationOrdinal(NonNegLong.unsafeFrom(1L)),
      parent = KesRegistrationReference.empty
    )
    val proof = SignatureProof(Id(genesis.operatorPeerId.value), Signature(Hex("7f" * 64)))
    val record = KesRegistrationRecord(Signed(cert, NonEmptySet.one(proof)), SnapshotOrdinal.unsafeApply(9L))

    genesis.copy(
      kes = genesis.kes.copy(offset = effectiveFrom.value),
      effectiveFromPeriod = effectiveFrom,
      registration = record.some
    )
  }
}
