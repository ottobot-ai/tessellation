package io.constellationnetwork.security.vrf

import java.security.{KeyPair, MessageDigest}

/** Derives a deterministic Ed25519 VRF seed from an existing secp256k1 private key.
  *
  * Uses SHA-512 with domain separation to derive a 32-byte Ed25519 seed from the raw secp256k1 scalar bytes stored in tessellation's PKCS12
  * keystores.
  *
  * This avoids introducing new key material — every node's VRF identity is deterministically bound to their existing p12 key.
  */
object VrfKeyDeriver {

  private val DomainTag = "tessellation-vrf-v1".getBytes("UTF-8")

  /** Derive a 32-byte Ed25519 seed from secp256k1 private key bytes.
    *
    * @param secp256k1PrivateKeyBytes
    *   raw 32-byte secp256k1 scalar
    * @return
    *   32-byte Ed25519 seed suitable for EcVrf25519
    */
  def deriveVrfSeed(secp256k1PrivateKeyBytes: Array[Byte]): Array[Byte] = {
    require(secp256k1PrivateKeyBytes.length == 32, s"Expected 32-byte private key, got ${secp256k1PrivateKeyBytes.length}")
    val md = MessageDigest.getInstance("SHA-512")
    md.update(DomainTag)
    md.update(secp256k1PrivateKeyBytes)
    md.digest().take(32)
  }

  /** Normalize a JCE `ECPrivateKey`'s scalar to exactly 32 big-endian bytes.
    *
    * `BigInteger.toByteArray` may emit 33 bytes (a leading sign byte when the high bit is set) or fewer than 32 (small scalars). We drop
    * the leading byte(s) or left-pad with zeros to land on the canonical 32-byte width that [[deriveVrfSeed]] requires.
    *
    * '''Determinism contract.''' This is the byte-for-byte normalization the gl0 snapshot leader loop applies before deriving its VRF
    * identity (`SnapshotLeaderLoop.deriveVrfKeys`). Both the runtime and the genesis tool MUST share THIS method so a genesis-populated
    * `vrfPublicKey` byte-matches the operator's runtime-derived VRF VK — otherwise per-(shard, epoch) committee sortition (Slice S2) would
    * reject every honest signer. Do not re-implement the drop/pad anywhere else.
    */
  def normalizeEcScalar32(keyPair: KeyPair): Array[Byte] =
    keyPair.getPrivate match {
      case ecKey: java.security.interfaces.ECPrivateKey =>
        val bytes = ecKey.getS.toByteArray
        if (bytes.length > 32) bytes.drop(bytes.length - 32)
        else if (bytes.length < 32) Array.fill(32 - bytes.length)(0.toByte) ++ bytes
        else bytes
      case other =>
        other.getEncoded.takeRight(32)
    }

  /** Derive the `(vrfSeed, vrfVerificationKey)` pair for an operator from its long-term secp256k1 keypair.
    *
    * Single canonical derivation path shared by the gl0 snapshot leader loop (`SnapshotLeaderLoop.deriveVrfKeys`) and the genesis generator
    * (`GenesisGenerator`, which populates `L0GenesisOperator.vrfPublicKey`). The sequence is: [[normalizeEcScalar32]] → [[deriveVrfSeed]] →
    * `EcVrf25519.getVerificationKey`. Keeping it in one place is the determinism guarantee for VRF-VK registry / committee sortition.
    *
    * @return
    *   `(seed, vk)` — the 32-byte Ed25519 seed and the 32-byte VRF verification key.
    */
  def deriveVrfKeyPair(keyPair: KeyPair): (Array[Byte], Array[Byte]) = {
    val seed = deriveVrfSeed(normalizeEcScalar32(keyPair))
    val vk = EcVrf25519.default.getVerificationKey(seed)
    (seed, vk)
  }
}
