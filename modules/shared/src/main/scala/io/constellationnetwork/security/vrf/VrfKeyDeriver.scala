package io.constellationnetwork.security.vrf

import java.security.MessageDigest

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
}
