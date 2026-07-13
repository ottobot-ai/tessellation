package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import java.nio.charset.StandardCharsets

import cats.effect.Sync
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashManager.SlashedRegistryEntry
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

import io.circe.parser.{decode => circeDecode}
import io.circe.syntax._
import io.circe.{Printer => CircePrinter}
import scodec.bits.ByteVector

/** Double-slash MPT guard for the WATCHTOWER invalid-state-proof tier — the analog of [[SlashedSeenReader]] one layer up, keyed on the
  * checkpoint identity `(shardId, disputedCheckpointHash)` instead of the metagraph-equivocation triple.
  *
  * '''Why a typed reader, not direct MPT access.''' Keeps the verdict ([[InvalidStateProofValidator]]) testable against a stub before the
  * GSAM `Slashings/` MPT partition is wired, exactly as `SlashedSeenReader` does for [[SlashableEvidenceValidator]]. The concrete impl
  * ([[InvalidStateProofSlashedReader.fromMptStore]]) reads the [[io.constellationnetwork.schema.mpt.GlobalStateFieldId.Slashings]]
  * partition (fieldId 34) written by the GSAM accept path when an invalid-state-proof is upheld.
  *
  * '''Honest-node byte-equivalence (slashing safety bar).''' `wasSlashed` MUST be deterministic over the chosen branch view — every honest
  * node reading the same finalized MPT bytes returns the same answer, so the verdict is cluster-uniform. The concrete impl reads the
  * FINALIZED base store; the validator's caller-contract gates the dispute to land below depth-k₁, where the base `S(N)` is
  * cluster-uniform.
  */
trait InvalidStateProofSlashedReader[F[_]] {

  /** Returns `true` iff a slash record already exists for `(shardId, disputedCheckpointHash)` — the load-bearing double-slash key (only the
    * first upheld invalid-state-proof for a given wrong checkpoint slashes; subsequent submissions are rejected).
    */
  def wasSlashed(shardId: ShardId, disputedCheckpointHash: Hash): F[Boolean]
}

object InvalidStateProofSlashedReader {

  /** Always-false stub for isolated validator unit tests only. Production validation must bind an exact rooted MPT view; otherwise an
    * already-applied slash can be processed again.
    */
  def neverSlashed[F[_]](implicit F: cats.Applicative[F]): InvalidStateProofSlashedReader[F] =
    (_: ShardId, _: Hash) => F.pure(false)

  /** In-memory deterministic stub for negative-path tests — every node builds the same `seen` set from the same MPT bytes. */
  def fromSet[F[_]](seen: Set[(ShardId, Hash)])(implicit F: cats.Applicative[F]): InvalidStateProofSlashedReader[F] =
    (shardId: ShardId, disputedCheckpointHash: Hash) => F.pure(seen.contains((shardId, disputedCheckpointHash)))

  /** Canonical, deterministic `ImmutableCodec[SlashedRegistryEntry]` for the MPT value — UTF-8 of Circe's `noSpaces` printer over the
    * derived `Encoder` (the SAME canonical-JSON discipline as `CompatCodecs.jsonImmutableCodec`). Circe object-key order is the case-class
    * field declaration order, so every honest node encodes a given record to byte-identical bytes (the slashing-safety determinism bar).
    * Kept `private` to this object: the partition is written by GSAM via the value's bytes and read back here; no other site needs the
    * codec.
    *
    * GSAM's writer side encodes via this SAME helper so the producer's `postBytes` and any reader/replay agree byte-for-byte.
    */
  val entryCodec: io.constellationnetwork.serde.ImmutableCodec[SlashedRegistryEntry] =
    new io.constellationnetwork.serde.ImmutableCodec[SlashedRegistryEntry] {
      private val printer: CircePrinter = CircePrinter.noSpaces
      def immutableBytes(value: SlashedRegistryEntry): ByteVector =
        ByteVector.view(printer.print(value.asJson).getBytes(StandardCharsets.UTF_8))
      def fromImmutableBytes(bytes: ByteVector): Either[io.constellationnetwork.serde.SerdeError, SlashedRegistryEntry] =
        circeDecode[SlashedRegistryEntry](new String(bytes.toArray, StandardCharsets.UTF_8))
          .leftMap(e => io.constellationnetwork.serde.SerdeError.ScodecFailure(e.getMessage))
    }

  /** Production reader backed by the global MPT store. Prefix-scans the [[io.constellationnetwork.schema.mpt.GlobalStateFieldId.Slashings]]
    * partition and upholds the double-slash guard iff ANY record matches `(shardId, disputedCheckpointHash)`.
    *
    * '''Determinism.''' The scan reads the store's canonical byte map and decodes each value via [[entryCodec]]; the membership test is a
    * pure predicate over the decoded records. Two honest nodes over the same finalized MPT bytes return the same answer. The partition is
    * tiny (≤ `numShards × committeeSize` per slashed checkpoint, slashes rare), so the full scan is cheap — and it serves BOTH the
    * double-slash dedup here and a future per-operator cooldown gate from the same single partition (the value carries `peerId` +
    * `cooldownUntilEpoch`).
    *
    * Reads the FINALIZED base store (not a branch-aware view): the double-slash decision is only consulted for disputes gated below
    * depth-k₁, where the base is cluster-uniform (see the validator scaladoc), so reading the base is the correct, byte-equivalent source.
    */
  def fromMptStore[F[_]: Sync: Hasher](
    mptStore: MptStore[F, GlobalStateKey]
  ): InvalidStateProofSlashedReader[F] =
    new InvalidStateProofSlashedReader[F] {
      private implicit val codec: io.constellationnetwork.serde.ImmutableCodec[SlashedRegistryEntry] = entryCodec

      def wasSlashed(shardId: ShardId, disputedCheckpointHash: Hash): F[Boolean] =
        GlobalStateKey.slashingsFieldPrefix[F].flatMap { prefix =>
          mptStore.getAllForPrefix[SlashedRegistryEntry](prefix).map { entries =>
            entries.valuesIterator.exists(e => e.shardId === shardId && e.disputedCheckpointHash === disputedCheckpointHash)
          }
        }
    }
}
