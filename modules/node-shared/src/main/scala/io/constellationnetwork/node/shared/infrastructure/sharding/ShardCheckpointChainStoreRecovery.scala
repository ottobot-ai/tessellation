package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.NonEmptySet
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore
import io.constellationnetwork.schema.sharding.ShardCheckpoint
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.vrf.EcVrf25519

/** Reconstructs chain-store metadata from a checkpoint whose economic derivation was locally reproduced before its validity signature was
  * emitted or accepted.
  *
  * Shard gossip carries the committee signatures rather than the producer's redundant outer `Signed` envelope. A GL0 snapshot likewise
  * embeds the checkpoint value directly. Both paths reconstruct the same outer proofs and derive the same VRF output before inserting the
  * checkpoint into [[ShardChainStore]]. The store is keyed by the checkpoint signing-preimage hash, so repeating recovery is idempotent.
  *
  * This helper is not a validation boundary. A local producer may call it after replay-before-sign even though its new checkpoint has only
  * the producer's singleton execution signature. A receiver or Phase-2 callback may call it only after the relevant intake or embedded
  * artifact verifier has checked duty, signatures/quorum, and replay.
  */
object ShardCheckpointChainStoreRecovery {

  private val vrf = EcVrf25519.default

  final case class Ingested(checkpointHash: Hash, inserted: Boolean)

  def reconstructSigned(checkpoint: ShardCheckpoint): Signed[ShardCheckpoint] = {
    val proofs = checkpoint.committeeSignatures.map { sig =>
      SignatureProof(sig.peerId.toId, Signature(sig.ed25519Sig))
    }
    Signed(checkpoint, NonEmptySet.of(proofs.head, proofs.tail: _*))
  }

  private def vrfOutputFromProof[F[_]: Async](proofBytes: Array[Byte]): F[Array[Byte]] =
    Async[F]
      .delay(vrf.vrfProofToHash(proofBytes))
      .flatMap(_.liftTo[F](new IllegalArgumentException("checkpoint producer VRF proof cannot be converted to a VRF output")))

  /** Insert a previously validated checkpoint into a possibly empty local shard store, then prove that the exact signing-preimage hash is
    * present. A duplicate insert is valid only when that exact hash already resolves. Rejected storage and malformed VRF proofs fail
    * closed.
    */
  def ingestValidated[F[_]: Async: Hasher](
    checkpoint: ShardCheckpoint,
    chainStore: ShardChainStore[F]
  ): F[Ingested] = {
    val signed = reconstructSigned(checkpoint)
    for {
      checkpointHash <- Hasher[F].hash(checkpoint.signingPreimage)
      vrfOutput <- vrfOutputFromProof[F](checkpoint.producerSignature.vrfProof.toBytes)
      inserted <- chainStore.store(
        signed,
        checkpoint.parentCheckpointHash,
        checkpoint.shardOrdinal,
        checkpoint.slot.value.value,
        vrfOutput
      )
      stored <- chainStore
        .getByHash(checkpointHash)
        .flatMap(_.liftTo[F](new IllegalStateException(s"recovered checkpoint missing from shard store: ${checkpointHash.value}")))
      storedPreimageHash <- Hasher[F].hash(stored.signed.value.signingPreimage)
      _ <- Async[F].raiseUnless(stored.hash === checkpointHash && storedPreimageHash === checkpointHash) {
        new IllegalStateException(
          s"recovered checkpoint hash mismatch: expected=${checkpointHash.value} cached=${stored.hash.value} " +
            s"storedPreimage=${storedPreimageHash.value}"
        )
      }
    } yield Ingested(checkpointHash, inserted)
  }
}
