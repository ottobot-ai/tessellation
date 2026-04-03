package io.constellationnetwork.security.vrf

import cats.effect.IO

import weaver.SimpleIOSuite

object VrfKeyDeriverSuite extends SimpleIOSuite {

  private val vrf = new EcVrf25519()

  test("VrfKeyDeriver - same input produces same output (deterministic)") {
    IO {
      val secp256k1Key = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(secp256k1Key)

      val seed1 = VrfKeyDeriver.deriveVrfSeed(secp256k1Key)
      val seed2 = VrfKeyDeriver.deriveVrfSeed(secp256k1Key)

      expect(java.util.Arrays.equals(seed1, seed2))
    }
  }

  test("VrfKeyDeriver - different inputs produce different outputs") {
    IO {
      val key1 = new Array[Byte](32)
      val key2 = new Array[Byte](32)
      val rng = new java.security.SecureRandom()
      rng.nextBytes(key1)
      rng.nextBytes(key2)

      val seed1 = VrfKeyDeriver.deriveVrfSeed(key1)
      val seed2 = VrfKeyDeriver.deriveVrfSeed(key2)

      expect(!java.util.Arrays.equals(seed1, seed2))
    }
  }

  test("VrfKeyDeriver - derived seed is 32 bytes") {
    IO {
      val secp256k1Key = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(secp256k1Key)

      val seed = VrfKeyDeriver.deriveVrfSeed(secp256k1Key)

      expect(seed.length == 32)
    }
  }

  test("VrfKeyDeriver - rejects wrong-length input (too short)") {
    IO {
      val shortKey = new Array[Byte](31)
      new java.security.SecureRandom().nextBytes(shortKey)

      val result =
        try {
          VrfKeyDeriver.deriveVrfSeed(shortKey)
          false
        } catch {
          case _: IllegalArgumentException => true
          case _: Throwable                => false
        }

      expect(result)
    }
  }

  test("VrfKeyDeriver - rejects wrong-length input (too long)") {
    IO {
      val longKey = new Array[Byte](33)
      new java.security.SecureRandom().nextBytes(longKey)

      val result =
        try {
          VrfKeyDeriver.deriveVrfSeed(longKey)
          false
        } catch {
          case _: IllegalArgumentException => true
          case _: Throwable                => false
        }

      expect(result)
    }
  }

  test("VrfKeyDeriver - derived seed produces valid VRF keys (roundtrip)") {
    IO {
      val secp256k1Key = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(secp256k1Key)

      // Derive VRF seed from secp256k1 key
      val vrfSeed = VrfKeyDeriver.deriveVrfSeed(secp256k1Key)

      // Generate VRF verification key
      val vrfVerificationKey = vrf.getVerificationKey(vrfSeed)

      // Create a proof
      val message = "test message".getBytes("UTF-8")
      val proof = vrf.vrfProof(vrfSeed, message)

      // Verify the proof
      expect(vrf.vrfVerify(vrfVerificationKey, message, proof))
    }
  }

  test("VrfKeyDeriver - derived seed is stable across VRF operations") {
    IO {
      val secp256k1Key = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(secp256k1Key)
      val message = "stability test".getBytes("UTF-8")

      // First derivation and proof
      val seed1 = VrfKeyDeriver.deriveVrfSeed(secp256k1Key)
      val vk1 = vrf.getVerificationKey(seed1)
      val proof1 = vrf.vrfProof(seed1, message)
      val beta1 = vrf.vrfProofToHash(proof1)

      // Second derivation and proof (should be identical)
      val seed2 = VrfKeyDeriver.deriveVrfSeed(secp256k1Key)
      val vk2 = vrf.getVerificationKey(seed2)
      val proof2 = vrf.vrfProof(seed2, message)
      val beta2 = vrf.vrfProofToHash(proof2)

      expect(java.util.Arrays.equals(vk1, vk2))
        .and(expect(java.util.Arrays.equals(proof1, proof2)))
        .and(expect(beta1.isDefined && beta2.isDefined))
        .and(expect(java.util.Arrays.equals(beta1.get, beta2.get)))
    }
  }

  test("VrfKeyDeriver - all-zeros input produces non-zero output") {
    IO {
      val zeroKey = new Array[Byte](32) // All zeros

      val seed = VrfKeyDeriver.deriveVrfSeed(zeroKey)

      // The derived seed should not be all zeros (domain separation ensures this)
      expect(!seed.forall(_ == 0))
    }
  }
}
