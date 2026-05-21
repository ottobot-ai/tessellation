package io.constellationnetwork.node.shared.domain.nakamoto.kes

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert.KesRegistrationRecord
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.kes.VerificationKeyKesProduct

/** Runtime-mutable [[KesRegistry]] that overlays genesis-frozen entries with Slice 10 registration certs.
  *
  * Lookup precedence (per-operator):
  *
  *   1. If the operator has at least one runtime cert with `effectiveFromEpoch <= currentEpoch`, use the highest-ordinal such cert.
  *      Rotations apply because each cert overrides the prior one once its activation epoch passes.
  *   1. Otherwise fall back to the genesis-frozen [[KesRegistry]] entry (Slice 3).
  *   1. Otherwise `None` — the operator is unknown.
  *
  * '''Backward compatibility.''' This overlay is constructed with the existing genesis [[KesRegistry]] as the base. Genesis lookups
  * continue to work unchanged until a runtime cert with `effectiveFromEpoch <= currentEpoch` lands for that operator, at which point the
  * runtime cert takes precedence. A cert that hasn't reached its activation epoch yet is held as "pending" and not returned from `getKesVk`
  * — it becomes effective on or after `effectiveFromEpoch`.
  *
  * '''State.''' Per-operator we keep a small chain `List[KesRegistrationRecord]` sorted by `acceptedAt` (latest first). Reads scan the head
  * for the first record whose `effectiveFromEpoch <= currentEpoch`. The chain is also the source of truth for the chain-link validation in
  * [[KesRegistrationCertValidator]] (the next-cert `parent` must match `KesRegistrationReference.of(latestAccepted)`).
  *
  * The registry is held in a `Ref` so the GSAM accept handler can swap in a new snapshot of per-operator records each ordinal. This avoids
  * the user-visible epoch parameter being baked into the constructor — callers thread the current epoch through `getKesVk(peerId,
  * currentEpoch)` instead.
  */
trait MutableKesRegistry[F[_]] {

  /** Returns the active KES master VK for `peerId` at `currentEpoch`. Runtime cert overrides genesis if and only if the cert's
    * `effectiveFromEpoch <= currentEpoch`.
    */
  def getKesVk(peerId: PeerId, currentEpoch: EpochProgress): F[Option[KesRegistryEntry]]

  /** The per-operator chain of accepted runtime certs (latest-first). Used by the validator to look up `lastRef` for chain-link checks. */
  def runtimeCertsFor(peerId: PeerId): F[List[KesRegistrationRecord]]

  /** Apply a new batch of accepted certs from the GSAM accept handler. Each accepted record is appended to the head of its operator's
    * chain. Idempotent on a record by `(operatorPeerId, ordinal)` — re-applying the same record is a no-op.
    */
  def applyAccepted(accepted: SortedMap[PeerId, KesRegistrationRecord]): F[Unit]

  /** All currently-held runtime cert chains. Diagnostic / observability. */
  def list: F[SortedMap[PeerId, List[KesRegistrationRecord]]]
}

object MutableKesRegistry {

  def make[F[_]: Sync](base: KesRegistry[F]): F[MutableKesRegistry[F]] =
    Ref.of[F, SortedMap[PeerId, List[KesRegistrationRecord]]](SortedMap.empty).map { ref =>
      new MutableKesRegistry[F] {

        override def getKesVk(peerId: PeerId, currentEpoch: EpochProgress): F[Option[KesRegistryEntry]] =
          ref.get.flatMap { state =>
            val runtimeMatch: Option[KesRegistryEntry] = state.get(peerId).flatMap { records =>
              records.find(_.event.value.effectiveFromEpoch <= currentEpoch).map(toEntry)
            }
            runtimeMatch match {
              case Some(entry) => Sync[F].pure(Some(entry))
              case None        => base.getKesVk(peerId)
            }
          }

        override def runtimeCertsFor(peerId: PeerId): F[List[KesRegistrationRecord]] =
          ref.get.map(_.getOrElse(peerId, Nil))

        override def applyAccepted(accepted: SortedMap[PeerId, KesRegistrationRecord]): F[Unit] =
          ref.update { state =>
            accepted.foldLeft(state) {
              case (acc, (peerId, record)) =>
                val existing = acc.getOrElse(peerId, Nil)
                val isDuplicate = existing.exists(_.event.value.ordinal === record.event.value.ordinal)
                if (isDuplicate) acc
                else acc.updated(peerId, record :: existing)
            }
          }

        override def list: F[SortedMap[PeerId, List[KesRegistrationRecord]]] = ref.get
      }
    }

  /** Decode the runtime cert's `kesMasterVK` hex bytes into a `VerificationKeyKesProduct` entry suitable for the read-only `KesRegistry`
    * contract. The cert's hex bytes are decoded eagerly; a malformed `kesMasterVK` would surface here as a `Hex.toBytes` exception — but
    * the validator's `MalformedVk` check already rejects empty / structurally-broken values, so by the time a record gets here the bytes
    * are non-empty and base-16 valid.
    */
  private def toEntry(record: KesRegistrationRecord): KesRegistryEntry = {
    val cert = record.event.value
    val vkBytes: Array[Byte] = scala.util.Try(cert.kesMasterVK.toBytes).getOrElse(Array.emptyByteArray)
    KesRegistryEntry(VerificationKeyKesProduct(vkBytes, cert.kesMasterVKStep), cert.offset)
  }
}
