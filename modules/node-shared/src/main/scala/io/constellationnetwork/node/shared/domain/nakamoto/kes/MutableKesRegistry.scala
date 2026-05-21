package io.constellationnetwork.node.shared.domain.nakamoto.kes

import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.kes.KesRegistrationStateManager
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert.KesRegistrationRecord
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.kes.VerificationKeyKesProduct

/** Runtime-mutable [[KesRegistry]] that overlays genesis-frozen entries with Slice 10 registration certs.
  *
  * Lookup precedence (per-operator):
  *
  *   1. If the operator has at least one MPT-persisted runtime cert with `effectiveFromEpoch <= currentEpoch`, use the highest-ordinal such
  *      cert. Rotations apply because each cert overrides the prior one once its activation epoch passes.
  *   1. Otherwise fall back to the genesis-frozen [[KesRegistry]] entry (Slice 3).
  *   1. Otherwise `None` — the operator is unknown.
  *
  * '''Durability (S10 persistence iteration B).''' Unlike the iteration-A in-memory `Ref`-backed registry, this implementation is
  * MPT-backed: every `getKesVk` call resolves through `KesRegistrationStateManager.materializeFromMpt`. After a node restart, MPT is the
  * source of truth — no in-memory state to lose. Newly-joining cluster peers reach the same registry view as live peers because the MPT is
  * byte-equivalent across honest nodes by the consensus state-proof contract.
  *
  * '''Backward compatibility.''' Constructed with the existing genesis [[KesRegistry]] as the base. Genesis lookups continue to work
  * unchanged until a runtime cert with `effectiveFromEpoch <= currentEpoch` lands in MPT for that operator, at which point the runtime cert
  * takes precedence. A cert that hasn't reached its activation epoch yet is held as "pending" and not returned from `getKesVk` — it becomes
  * effective on or after `effectiveFromEpoch`.
  *
  * '''Write path.''' This trait deliberately exposes NO mutators. The MPT writes are owned by the GSAM accept pipeline (S10-wiring-B,
  * landing in a follow-up): `KesRegistrationCertAcceptanceManager.accept` partitions candidates → `KesRegistrationStateManager` computes
  * the new per-peer SortedSet / pointer → `AcceptanceMptStateChanges.applyStateChanges` persists. The runtime registry is a pure reader
  * over that durable state.
  */
trait MutableKesRegistry[F[_]] {

  /** Returns the active KES master VK for `peerId` at `currentEpoch`. Runtime cert overrides genesis if and only if the cert's
    * `effectiveFromEpoch <= currentEpoch`. Goes through the MPT-backed [[KesRegistrationStateManager]] on every call; reorg-aware via the
    * underlying [[GlobalStateReader]] (branch-aware reads pick up pending writes on the chain's current tip).
    */
  def getKesVk(peerId: PeerId, currentEpoch: EpochProgress)(implicit hasher: Hasher[F]): F[Option[KesRegistryEntry]]

  /** The latest accepted runtime cert for `peerId`, regardless of `effectiveFromEpoch`. Used by the validator to look up `lastRef` for
    * chain-link checks (the next-cert `parent` must match `KesRegistrationReference.of(latestAccepted)`). Diagnostic / observability.
    */
  def latestRuntimeCertFor(peerId: PeerId)(implicit hasher: Hasher[F]): F[Option[KesRegistrationRecord]]

  /** Full per-operator cert history (latest-first), recovered from MPT. Replaces the iteration-A in-memory list. Routes call this when they
    * need the canonical "what certs has this operator submitted" view; under multi-branch the chain's tip-aware reader picks up pending
    * certs ahead of the finalized base.
    *
    * Returns `Nil` when no runtime certs exist for the operator (genesis-only). The list is sorted by `SortedSet.ordering` reversed, so the
    * head is the most-recent record by `(acceptedAt, ordinal)`.
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
    * `base` is the frozen genesis registry (Slice 3); `reader` is the read-only handle into the runtime cert partition. Both come from the
    * node's `Services.make` wiring — `base` from `L0GenesisData.kesRegistrations`, `reader` from the per-mode overlay factory (`pending` on
    * gl0, `finalized` on followers).
    *
    * No mutable state is allocated here. The MPT IS the state.
    */
  def make[F[_]: Async](
    base: KesRegistry[F],
    reader: GlobalStateReader[F]
  ): F[MutableKesRegistry[F]] = {
    val manager = KesRegistrationStateManager.make[F](reader)
    Async[F].pure(new MutableKesRegistry[F] {

      override def getKesVk(peerId: PeerId, currentEpoch: EpochProgress)(implicit hasher: Hasher[F]): F[Option[KesRegistryEntry]] =
        manager.materializeFromMpt(peerId).flatMap {
          case Some(record) if record.event.value.effectiveFromEpoch <= currentEpoch =>
            (Some(toEntry(record)): Option[KesRegistryEntry]).pure[F]
          case _ =>
            // Pointer record either absent, or present but not yet active at `currentEpoch` (held as "pending").
            // Fall through to the genesis-frozen base in both cases.
            base.getKesVk(peerId)
        }

      override def latestRuntimeCertFor(peerId: PeerId)(implicit hasher: Hasher[F]): F[Option[KesRegistrationRecord]] =
        manager.materializeFromMpt(peerId)

      override def runtimeCertsFor(peerId: PeerId)(implicit hasher: Hasher[F]): F[List[KesRegistrationRecord]] =
        // MPT SortedSet is ordered head=earliest, last=latest. The legacy iteration-A API returns latest-first,
        // so reverse here to keep the wire / caller contract stable across the migration.
        manager.materializeChainForPeer(peerId).map(_.toList.reverse)

      override def list(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationRecord]] =
        manager.materializeAllFromMpt
    })
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
