package io.constellationnetwork.node.shared.domain.nakamoto.kes

import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.util.Try

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistryEntry, OperatorConsensusKeyRegistry}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.kes.KesRegistrationStateManager
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.kes.VerificationKeyKesProduct

/** Runtime paired-key reader that overlays the paired genesis identity with unified KES+VRF registration certs.
  *
  * Lookup precedence (per-operator):
  *
  *   1. If the operator has at least one MPT-persisted runtime cert with `effectiveFromPeriod <= currentPeriod`, use the highest-ordinal
  *      such cert. Rotations apply because each cert overrides the prior one once its activation eta period passes.
  *   1. Otherwise fall back only when both genesis KES and VRF projections exist and are well formed.
  *   1. Otherwise `None` — the operator is unknown.
  *
  * '''Reader behavior.''' Unlike the iteration-A in-memory `Ref`-backed registry, this implementation reads records from MPT and resolves
  * the exact per-operator chain selected by its pointer. GSAM now selects signed candidates into the snapshot artifact and writes the
  * accepted histories and pointers into the same candidate-branch MPT root. This reader is restart- and best-tip-reorg-aware when supplied
  * a branch-aware view, but its API still lacks an explicit candidate-parent argument and must not be used where historical consensus
  * validation requires a sibling or offence-parent view.
  *
  * Constructed with the frozen atomic [[OperatorConsensusKeyRegistry]] as the genesis base. A canonical runtime record is held pending
  * until `effectiveFromPeriod`. Genesis anchoring and the explicit historical resolver remain separate production gates.
  *
  * '''Write path.''' This trait deliberately exposes NO mutators. The target GSAM accept pipeline is:
  * `KesRegistrationCertAcceptanceManager.accept` partitions candidates → `KesRegistrationStateManager` computes the new per-peer
  * `SortedSet` / pointer → `AcceptanceMptStateChanges.applyStateChanges` persists both atomically in the candidate branch. That end-to-end
  * canonical path is wired; the runtime registry here remains only a pure reader over the exact view its caller supplies.
  */
trait MutableKesRegistry[F[_]] {

  /** Atomic active KES+VRF lookup. Consensus consumers must migrate to this result; neither carried keys nor independent KES/VRF lookups
    * may fill in a missing half.
    */
  def getConsensusKeys(peerId: PeerId, currentPeriod: EtaPeriod)(implicit hasher: Hasher[F]): F[Option[OperatorConsensusKeys]]

  /** Transitional KES-only adapter derived from [[getConsensusKeys]]. It never resolves KES independently of VRF.
    */
  def getKesVk(peerId: PeerId, currentPeriod: EtaPeriod)(implicit hasher: Hasher[F]): F[Option[KesRegistryEntry]]

  /** Transitional VRF-only adapter derived from [[getConsensusKeys]]. It never resolves VRF independently of KES. */
  def getVrfVk(peerId: PeerId, currentPeriod: EtaPeriod)(implicit hasher: Hasher[F]): F[Option[VrfPublicKey]]

  /** The latest accepted runtime cert for `peerId`, regardless of `effectiveFromPeriod`. Used by the validator to look up `lastRef` for
    * chain-link checks (the next-cert `parent` must match `KesRegistrationReference.of(latestAccepted)`). Diagnostic / observability.
    */
  def latestRuntimeCertFor(peerId: PeerId)(implicit hasher: Hasher[F]): F[Option[KesRegistrationRecord]]

  /** Raw per-operator cert history (latest-first), recovered from MPT. This diagnostic surface includes orphan records and is not the
    * pointer-selected canonical chain used by `getKesVk`; under multi-branch it reads the view supplied at construction.
    *
    * Returns `Nil` when no runtime certs exist for the operator (genesis-only). The list is sorted by `SortedSet.ordering` reversed, so the
    * head is the most-recent record by `(acceptedAt, signed event)`.
    */
  def runtimeCertsFor(peerId: PeerId)(implicit hasher: Hasher[F]): F[List[KesRegistrationRecord]]

  /** Snapshot of every per-operator latest-accepted runtime cert. Diagnostic / observability. Implemented via
    * `KesRegistrationStateManager.materializeAllFromMpt` — full prefix scan, not a hot path; intended for debug routes and reorg- rebuild
    * tests.
    */
  def list(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationRecord]]
}

object MutableKesRegistry {

  /** Construct the MPT-backed mutable registry overlay.
    *
    * `base` is the frozen paired genesis operator-key registry; `reader` is the read-only handle into the runtime cert partition. Both come
    * from node service wiring, with the reader bound to that caller's selected MPT view.
    *
    * No mutable state is allocated here. This does not by itself prove the supplied MPT view was populated canonically.
    */
  def make[F[_]: Async](
    baseRegistry: OperatorConsensusKeyRegistry[F],
    reader: GlobalStateReader[F]
  ): F[MutableKesRegistry[F]] = {
    val manager = KesRegistrationStateManager.make[F](reader)
    Async[F].pure(new MutableKesRegistry[F] {

      override def getConsensusKeys(
        peerId: PeerId,
        currentPeriod: EtaPeriod
      )(implicit hasher: Hasher[F]): F[Option[OperatorConsensusKeys]] =
        if (currentPeriod.value < 0L) none[OperatorConsensusKeys].pure[F]
        else
          (manager.materializeFromMpt(peerId), manager.materializeChainForPeer(peerId)).tupled.flatMap {
            case (None, chain) if chain.isEmpty => basePair(peerId, baseRegistry)
            case (None, _)                      => none[OperatorConsensusKeys].pure[F]
            case (Some(latest), chain) =>
              canonicalChain(latest, chain.toList).flatMap {
                case None => none[OperatorConsensusKeys].pure[F]
                case Some(canonical) =>
                  canonical
                    .filter(_.event.value.effectiveFromPeriod <= currentPeriod)
                    .sortBy(_.event.value.ordinal)
                    .lastOption
                    .fold(basePair(peerId, baseRegistry))(toConsensusKeys(_).pure[F])
              }
          }

      override def getKesVk(peerId: PeerId, currentPeriod: EtaPeriod)(implicit hasher: Hasher[F]): F[Option[KesRegistryEntry]] =
        getConsensusKeys(peerId, currentPeriod).map(_.map(_.kes))

      override def getVrfVk(peerId: PeerId, currentPeriod: EtaPeriod)(implicit hasher: Hasher[F]): F[Option[VrfPublicKey]] =
        getConsensusKeys(peerId, currentPeriod).map(_.map(_.vrfPublicKey))

      override def latestRuntimeCertFor(peerId: PeerId)(implicit hasher: Hasher[F]): F[Option[KesRegistrationRecord]] =
        manager.materializeFromMpt(peerId)

      override def runtimeCertsFor(peerId: PeerId)(implicit hasher: Hasher[F]): F[List[KesRegistrationRecord]] =
        // MPT SortedSet is ordered head=earliest, last=latest. The legacy iteration-A API returns latest-first,
        // so reverse here to keep the wire / caller contract stable across the migration.
        manager.materializeChainForPeer(peerId).flatMap(records => records.toList.reverse.pure[F])

      override def list(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationRecord]] =
        manager.materializeAllFromMpt
    })
  }

  private def toConsensusKeys(record: KesRegistrationRecord): Option[OperatorConsensusKeys] = {
    val cert = record.event.value
    val kesBytes = Try(cert.kesMasterVK.toBytes).toOption
    val vrfBytes = Try(cert.vrfPublicKey.toBytes).toOption

    (kesBytes, vrfBytes) match {
      case (Some(kes), Some(vrf))
          if kes.length == KesRegistrationCertValidator.KesMasterVerificationKeyLength &&
            vrf.length == VrfPublicKey.ExpectedLength && cert.kesMasterVKStep == 0 && cert.offset >= 0L &&
            cert.offset == cert.effectiveFromPeriod.value =>
        OperatorConsensusKeys(
          cert.operatorPeerId,
          KesRegistryEntry(VerificationKeyKesProduct(kes, cert.kesMasterVKStep), cert.offset),
          VrfPublicKey.fromBytes(vrf),
          cert.effectiveFromPeriod,
          record.some
        ).some
      case _ => none[OperatorConsensusKeys]
    }
  }

  private def basePair[F[_]: Async](
    peerId: PeerId,
    registry: OperatorConsensusKeyRegistry[F]
  ): F[Option[OperatorConsensusKeys]] =
    registry.get(peerId).map {
      case Some(keys)
          if keys.operatorPeerId === peerId && keys.registration.isEmpty && keys.effectiveFromPeriod === EtaPeriod.Zero &&
            keys.kes.vk.value.length == KesRegistrationCertValidator.KesMasterVerificationKeyLength && keys.kes.vk.step == 0 &&
            keys.kes.offset == 0L && keys.vrfPublicKey.toBytes.length == VrfPublicKey.ExpectedLength =>
        val copiedKes = KesRegistryEntry(VerificationKeyKesProduct(keys.kes.vk.value.clone(), keys.kes.vk.step), keys.kes.offset)
        OperatorConsensusKeys(peerId, copiedKes, VrfPublicKey.fromBytes(keys.vrfPublicKey.toBytes), EtaPeriod.Zero, none).some
      case _ => none[OperatorConsensusKeys]
    }

  /** Resolve only the unique, exact reference chain ending at the pointer-selected latest record. Orphan records are ignored. A missing or
    * ambiguous parent, an operator change, an ordinal gap, or an overflow makes the runtime registry unavailable rather than selecting a
    * key from malformed consensus state.
    */
  private def canonicalChain[F[_]: Async: Hasher](
    latest: KesRegistrationRecord,
    records: List[KesRegistrationRecord]
  ): F[Option[List[KesRegistrationRecord]]] =
    records.traverse(record => KesRegistrationReference.of[F](record.event).map(_ -> record)).map { referenced =>
      val byReference = referenced.groupMap(_._1)(_._2)

      @annotation.tailrec
      def walk(current: KesRegistrationRecord, acc: List[KesRegistrationRecord]): Option[List[KesRegistrationRecord]] = {
        val cert = current.event.value
        val nextAcc = current :: acc

        if (cert.parent === KesRegistrationReference.empty)
          Option.when(cert.ordinal === KesRegistrationOrdinal.first)(nextAcc)
        else {
          val ordinalIsNext =
            Try(Math.addExact(cert.parent.ordinal.value.value, 1L)).toOption.contains(cert.ordinal.value.value)

          if (!ordinalIsNext) none
          else
            byReference.get(cert.parent) match {
              case Some(parent :: Nil) if parent.event.value.operatorPeerId === cert.operatorPeerId =>
                walk(parent, nextAcc)
              case _ => none
            }
        }
      }

      walk(latest, Nil)
    }
}

/** One operator's atomic active consensus-key identity. A runtime record rotates both keys together; `registration=None` identifies the
  * paired genesis anchor.
  */
final case class OperatorConsensusKeys(
  operatorPeerId: PeerId,
  kes: KesRegistryEntry,
  vrfPublicKey: VrfPublicKey,
  effectiveFromPeriod: EtaPeriod,
  registration: Option[KesRegistrationRecord]
)
