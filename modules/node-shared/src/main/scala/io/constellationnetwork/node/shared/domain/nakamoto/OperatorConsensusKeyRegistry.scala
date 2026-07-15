package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.Sync
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct

/** Atomic read-only view of the preregistered KES+VRF identity for each operator.
  *
  * KES and VRF projections are deliberately derived from this object. They are compatibility adapters for consumers that have not yet
  * migrated to a branch-historical paired lookup; they are never independent registration authorities.
  *
  * `registration=None` is reserved for a committed period-zero genesis pair; a nonzero activation without an accepted signed record is
  * malformed. This active-view algebra is sufficient for the immutable genesis anchor. Runtime rotation requires the branch-aware
  * `activeKeysAt(operator, candidateParent, period)` contract tracked by E2K; a current-view instance must never stand in for that
  * historical lookup.
  */
trait OperatorConsensusKeyRegistry[F[_]] {

  def get(peerId: PeerId): F[Option[OperatorConsensusKeys]]

  def list: F[Map[PeerId, OperatorConsensusKeys]]

  def kesRegistry: KesRegistry[F]

  def vrfRegistry: VrfRegistry[F]
}

object OperatorConsensusKeyRegistry {

  /** Construct an immutable registry from already authenticated, unique atomic records. The caller remains responsible for checking the
    * signed genesis/runtime registration artifact.
    */
  def make[F[_]: Sync](registrations: Map[PeerId, OperatorConsensusKeys]): OperatorConsensusKeyRegistry[F] = {
    validate(registrations)
    new OperatorConsensusKeyRegistry[F] { registry =>
      private val entries = registrations.iterator.map { case (peerId, keys) => peerId -> copyKeys(keys) }.toMap

      override def get(peerId: PeerId): F[Option[OperatorConsensusKeys]] =
        Sync[F].pure(entries.get(peerId).map(copyKeys))

      override def list: F[Map[PeerId, OperatorConsensusKeys]] =
        Sync[F].pure(entries.iterator.map { case (peerId, keys) => peerId -> copyKeys(keys) }.toMap)

      override val kesRegistry: KesRegistry[F] = new KesRegistry[F] {
        override def getKesVk(peerId: PeerId): F[Option[KesRegistryEntry]] =
          registry.get(peerId).map(_.map(_.kes))

        override def list: F[Map[PeerId, KesRegistryEntry]] =
          Sync[F].pure(entries.iterator.map { case (peerId, keys) => peerId -> copyKes(keys.kes) }.toMap)
      }

      override val vrfRegistry: VrfRegistry[F] = new VrfRegistry[F] {
        override def getVrfVk(peerId: PeerId): F[Option[Array[Byte]]] =
          registry.get(peerId).map(_.map(_.vrfPublicKey.toBytes))

        override def list: F[Map[PeerId, Array[Byte]]] =
          entries.iterator.map { case (peerId, keys) => peerId -> keys.vrfPublicKey.toBytes }.toMap.pure[F]
      }
    }
  }

  def empty[F[_]: Sync]: OperatorConsensusKeyRegistry[F] = make(Map.empty)

  private def copyKes(entry: KesRegistryEntry): KesRegistryEntry =
    KesRegistryEntry(VerificationKeyKesProduct(entry.vk.value.clone(), entry.vk.step), entry.offset)

  private def copyKeys(keys: OperatorConsensusKeys): OperatorConsensusKeys =
    keys.copy(kes = copyKes(keys.kes), vrfPublicKey = VrfPublicKey.fromBytes(keys.vrfPublicKey.toBytes))

  private def validate(registrations: Map[PeerId, OperatorConsensusKeys]): Unit = {
    val malformed = registrations.toList.collect {
      case (peerId, keys)
          if peerId =!= keys.operatorPeerId ||
            keys.kes.vk.value.length != 32 ||
            keys.kes.vk.step != 0 ||
            keys.kes.offset < 0L ||
            keys.kes.offset != keys.effectiveFromPeriod.value ||
            (keys.registration.isEmpty && keys.effectiveFromPeriod =!= EtaPeriod.Zero) ||
            keys.vrfPublicKey.toBytes.length != VrfPublicKey.ExpectedLength =>
        peerId
    }
    require(malformed.isEmpty, s"Malformed atomic operator-key record(s): ${malformed.sorted.mkString(",")}")

    val duplicateKesOwners = duplicateOwners(registrations)(_.kes.vk.value)
    require(duplicateKesOwners.isEmpty, s"KES key reuse across operators: ${duplicateKesOwners.mkString(";")}")

    val duplicateVrfOwners = duplicateOwners(registrations)(_.vrfPublicKey.toBytes)
    require(duplicateVrfOwners.isEmpty, s"VRF key reuse across operators: ${duplicateVrfOwners.mkString(";")}")
  }

  private def duplicateOwners(
    registrations: Map[PeerId, OperatorConsensusKeys]
  )(key: OperatorConsensusKeys => Array[Byte]): List[String] =
    registrations.toList.groupBy { case (_, keys) => Hex.fromBytes(key(keys)).value }.values.collect {
      case owners if owners.sizeCompare(1) > 0 => owners.map(_._1).sorted.mkString(",")
    }.toList.sorted
}
