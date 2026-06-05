package io.constellationnetwork.node.shared.infrastructure.snapshot.services

import java.nio.charset.StandardCharsets

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import io.constellationnetwork.node.shared.domain.snapshot.services.BalanceProof
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.StatelessMerklePatriciaProducer

import io.circe.Json
import io.circe.syntax.EncoderOps

/** Builds the per-address balance Merkle-Patricia trie that backs the roots-only-sharding light-client commitment, and produces the
  * inclusion proof a stock TypeScript verifier (`mptVerifier.ts`) consumes.
  *
  * ==Cross-language contract (must stay byte-identical to `mptVerifier.ts`)==
  *   - '''Path''' (`= SHA-256(address)` hex): [[path]] = `SHA-256(utf8(address.value))` lowercase-hex. A light client derives the same path
  *     from the bare DAG address string. NOTE: this hashes the raw address bytes, NOT a JSON-quoted string.
  *   - '''Value''': the balance amount as a plain JSON number (`balance.value.value`). This is the leaf `dataDigest` pre-image; the verifier
  *     treats `dataDigest` as opaque (it never re-hashes the value), so the value encoding only needs to be self-consistent here.
  *   - '''Node hashing''': routed through [[Hasher.forCanonicalJson]] = `SHA-256(prefixByte ++ RFC8785-canonicalJSON(commitment))`, with the
  *     `MerklePatriciaCommitment` shapes (`Leaf{remaining,dataDigest}` / `Branch{pathsDigest}` / `Extension{shared,childDigest}`) and prefix
  *     bytes (Leaf=0, Branch=1, Extension=2) that the verifier reproduces. NO Brotli, NO Kryo.
  *
  * The whole trie is rebuilt on demand from the snapshot's `balances` map (the light-client commitment is read-only; persistence/update is out
  * of scope here — this mirrors `LightClientSmt`).
  */
object BalanceMpt {

  /** The MPT path for an address: `SHA-256(utf8(address.value))`, lowercase hex (64 chars). */
  def path(address: Address): Hash =
    Hash.fromBytes(address.value.value.getBytes(StandardCharsets.UTF_8))

  /** Balance amount encoded as the trie's leaf value: a plain JSON number. */
  def valueJson(balance: Balance): Json =
    balance.value.value.asJson

  /** Build the balance trie over `balances` and produce `(root, inclusionProof)` for `address`, or `None` if `address` is absent (an
    * inclusion proof exists only for present keys) or `balances` is empty. Hashing uses the brotli-free [[Hasher.forCanonicalJson]] so the
    * result is TS-verifiable.
    */
  def buildProof[F[_]: Async](
    balances: Map[Address, Balance],
    address: Address,
    ordinal: SnapshotOrdinal
  ): F[Option[BalanceProof]] =
    balances.get(address) match {
      case None => Async[F].pure(None)
      case Some(balance) =>
        implicit val canonicalHasher: Hasher[F] = Hasher.forCanonicalJson[F]

        val data: Map[Hex, Json] = balances.map { case (addr, bal) => path(addr).coerceHex -> valueJson(bal) }
        val targetPath: Hex = path(address).coerceHex

        for {
          trie <- StatelessMerklePatriciaProducer[F].create(data)
          prover <- StatelessMerklePatriciaProducer[F].getProver(trie)
          proofE <- prover.attestPath(targetPath)
          result <- proofE match {
            case Right(proof) =>
              Async[F].pure(Option(BalanceProof(balance, trie.rootHash.value, ordinal, proof)))
            case Left(err) =>
              Async[F].raiseError[Option[BalanceProof]](
                new RuntimeException(s"Failed to build balance inclusion proof for ${address.value.value}: ${err.getMessage}")
              )
          }
        } yield result
    }

  private implicit class HashHexOps(private val h: Hash) extends AnyVal {
    // Hash.value is already a 64-char lowercase hex string; the MPT path/key type is Hex.
    def coerceHex: Hex = Hex(h.value)
  }
}
