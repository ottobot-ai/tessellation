package io.constellationnetwork.node.shared.domain.snapshot.services

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.snapshot.Snapshot
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaInclusionProof

import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

trait AddressService[F[_], S <: Snapshot] {
  def getBalance(address: Address): F[Option[(Balance, SnapshotOrdinal)]]

  /** Per-address balance plus a Merkle-Patricia inclusion proof against the metagraph's committed balance root, for the roots-only sharding
    * light client (`docs/nakamoto/ROOTS-ONLY-SHARDING-ARCHITECTURE.md`). The tree's node digests are computed with the brotli-free,
    * RFC-8785-canonical [[io.constellationnetwork.security.Hasher.forCanonicalJson]] hasher so a stock TypeScript verifier
    * (`mptVerifier.ts`) can recompute the root and accept the proof with no custom code. Returns `None` when no snapshot is available yet.
    */
  def getBalanceProof(address: Address): F[Option[BalanceProof]]

  def getTotalSupply: F[Option[(BigInt, SnapshotOrdinal)]]
  def getFilteredOutTotalSupply: F[Option[(BigInt, SnapshotOrdinal)]]
  def getCirculatedSupply: F[Option[(BigInt, SnapshotOrdinal)]]
  def getFilteredOutCirculatedSupply: F[Option[(BigInt, SnapshotOrdinal)]]
  def getWalletCount: F[Option[(Int, SnapshotOrdinal)]]
}

/** Response for `GET /currency/{address}/balance/proof`. `root` is the balance-MPT root (the metagraph's committed balance commitment for
  * `ordinal`); `proof` carries `{path, witness}` where `path = SHA-256(address)` hex. A light client verifies `proof` against `root` with
  * the stock `verifyMerklePatriciaProof` TS verifier.
  */
final case class BalanceProof(
  balance: Balance,
  root: Hash,
  ordinal: SnapshotOrdinal,
  proof: MerklePatriciaInclusionProof
)

object BalanceProof {
  implicit val encoder: Encoder[BalanceProof] = (b: BalanceProof) =>
    Json.obj(
      "balance" -> b.balance.value.value.asJson,
      "root" -> b.root.asJson,
      "ordinal" -> b.ordinal.value.value.asJson,
      "proof" -> b.proof.asJson
    )
}
