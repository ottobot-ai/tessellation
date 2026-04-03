package io.constellationnetwork.security.vrf

import java.util.HexFormat

import cats.effect.IO

import io.circe.generic.auto._
import io.circe.parser
import weaver.SimpleIOSuite

object EcVrf25519Suite extends SimpleIOSuite {

  private val hex = HexFormat.of()
  private val vrf = new EcVrf25519()

  // Load test vectors
  case class TestInputs(secretKey: String, message: String)
  case class TestOutputs(verificationKey: String, pi: String, beta: String)
  case class TestVector(description: String, inputs: TestInputs, outputs: TestOutputs)

  private val vectors: List[TestVector] = {
    val stream = getClass.getResourceAsStream("/VrfEd25519.json")
    val json =
      try scala.io.Source.fromInputStream(stream).mkString
      finally stream.close()
    parser.decode[List[TestVector]](json).getOrElse(throw new RuntimeException("Failed to parse VRF test vectors"))
  }

  // Generate tests for each vector
  vectors.foreach { vector =>
    test(s"VRF - ${vector.description} - derive verification key") {
      IO {
        val sk = hex.parseHex(vector.inputs.secretKey)
        val expectedVk = hex.parseHex(vector.outputs.verificationKey)
        val actualVk = vrf.getVerificationKey(sk)
        expect(java.util.Arrays.equals(actualVk, expectedVk))
      }
    }

    test(s"VRF - ${vector.description} - generate proof (pi)") {
      IO {
        val sk = hex.parseHex(vector.inputs.secretKey)
        val message = if (vector.inputs.message.isEmpty) Array.emptyByteArray else hex.parseHex(vector.inputs.message)
        val expectedPi = hex.parseHex(vector.outputs.pi)
        val actualPi = vrf.vrfProof(sk, message)
        expect(java.util.Arrays.equals(actualPi, expectedPi))
      }
    }

    test(s"VRF - ${vector.description} - proof to hash (beta)") {
      IO {
        val pi = hex.parseHex(vector.outputs.pi)
        val expectedBeta = hex.parseHex(vector.outputs.beta)
        val actualBeta = vrf.vrfProofToHash(pi)
        expect(actualBeta.isDefined).and(expect(java.util.Arrays.equals(actualBeta.get, expectedBeta)))
      }
    }

    test(s"VRF - ${vector.description} - verify proof") {
      IO {
        val vk = hex.parseHex(vector.outputs.verificationKey)
        val message = if (vector.inputs.message.isEmpty) Array.emptyByteArray else hex.parseHex(vector.inputs.message)
        val pi = hex.parseHex(vector.outputs.pi)
        expect(vrf.vrfVerify(vk, message, pi))
      }
    }

    test(s"VRF - ${vector.description} - reject tampered proof") {
      IO {
        val vk = hex.parseHex(vector.outputs.verificationKey)
        val message = if (vector.inputs.message.isEmpty) Array.emptyByteArray else hex.parseHex(vector.inputs.message)
        val pi = hex.parseHex(vector.outputs.pi)
        val tampered = pi.clone()
        tampered(0) = (tampered(0) ^ 0xff).toByte
        expect(!vrf.vrfVerify(vk, message, tampered))
      }
    }
  }

  // Additional property tests
  test("VRF - roundtrip: prove then verify") {
    IO {
      val seed = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(seed)
      val vk = vrf.getVerificationKey(seed)
      val message = "Hello, VRF!".getBytes("UTF-8")
      val proof = vrf.vrfProof(seed, message)
      expect(vrf.vrfVerify(vk, message, proof))
    }
  }

  test("VRF - same input produces same output (deterministic)") {
    IO {
      val seed = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(seed)
      val message = "Deterministic test".getBytes("UTF-8")
      val proof1 = vrf.vrfProof(seed, message)
      val proof2 = vrf.vrfProof(seed, message)
      val beta1 = vrf.vrfProofToHash(proof1)
      val beta2 = vrf.vrfProofToHash(proof2)
      expect(java.util.Arrays.equals(proof1, proof2))
        .and(expect(beta1.isDefined && beta2.isDefined))
        .and(expect(java.util.Arrays.equals(beta1.get, beta2.get)))
    }
  }

  test("VRF - wrong message fails verification") {
    IO {
      val seed = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(seed)
      val vk = vrf.getVerificationKey(seed)
      val message = "Hello, VRF!".getBytes("UTF-8")
      val proof = vrf.vrfProof(seed, message)
      val wrongMessage = "Wrong message".getBytes("UTF-8")
      expect(!vrf.vrfVerify(vk, wrongMessage, proof))
    }
  }

  test("VRF - wrong key fails verification") {
    IO {
      val seed1 = new Array[Byte](32)
      val seed2 = new Array[Byte](32)
      val rng = new java.security.SecureRandom()
      rng.nextBytes(seed1)
      rng.nextBytes(seed2)
      val vk1 = vrf.getVerificationKey(seed1)
      val vk2 = vrf.getVerificationKey(seed2)
      val message = "Hello, VRF!".getBytes("UTF-8")
      val proof = vrf.vrfProof(seed1, message)
      expect(vrf.vrfVerify(vk1, message, proof)).and(expect(!vrf.vrfVerify(vk2, message, proof)))
    }
  }

  test("VRF - proof length is 80 bytes") {
    IO {
      val seed = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(seed)
      val message = "Length test".getBytes("UTF-8")
      val proof = vrf.vrfProof(seed, message)
      expect(proof.length == 80)
    }
  }

  test("VRF - beta length is 64 bytes") {
    IO {
      val seed = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(seed)
      val message = "Beta length test".getBytes("UTF-8")
      val proof = vrf.vrfProof(seed, message)
      val beta = vrf.vrfProofToHash(proof)
      expect(beta.isDefined).and(expect(beta.get.length == 64))
    }
  }

  test("VRF - invalid proof length returns false") {
    IO {
      val seed = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(seed)
      val vk = vrf.getVerificationKey(seed)
      val message = "Invalid proof".getBytes("UTF-8")
      val shortProof = new Array[Byte](79)
      val longProof = new Array[Byte](81)
      expect(!vrf.vrfVerify(vk, message, shortProof)).and(expect(!vrf.vrfVerify(vk, message, longProof)))
    }
  }

  test("VRF - invalid public key length returns false") {
    IO {
      val seed = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(seed)
      val message = "Invalid key".getBytes("UTF-8")
      val proof = vrf.vrfProof(seed, message)
      val shortKey = new Array[Byte](31)
      val longKey = new Array[Byte](33)
      expect(!vrf.vrfVerify(shortKey, message, proof)).and(expect(!vrf.vrfVerify(longKey, message, proof)))
    }
  }

  test("VRF - vrfProofToHash returns None for invalid proof length") {
    IO {
      val shortProof = new Array[Byte](79)
      val longProof = new Array[Byte](81)
      expect(vrf.vrfProofToHash(shortProof).isEmpty).and(expect(vrf.vrfProofToHash(longProof).isEmpty))
    }
  }

  test("VRF - empty message works") {
    IO {
      val seed = new Array[Byte](32)
      new java.security.SecureRandom().nextBytes(seed)
      val vk = vrf.getVerificationKey(seed)
      val message = Array.emptyByteArray
      val proof = vrf.vrfProof(seed, message)
      expect(vrf.vrfVerify(vk, message, proof))
    }
  }
}
