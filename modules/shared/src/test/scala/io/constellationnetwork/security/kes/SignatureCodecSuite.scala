package io.constellationnetwork.security.kes

import weaver.SimpleIOSuite

/** Round-trip tests for [[SignatureCodec]] — the Slice 5/6 on-the-wire encoder for KES product signatures. */
object SignatureCodecSuite extends SimpleIOSuite {

  pureTest("encode + decode round-trip recovers a signature byte-equivalent to the original") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(0x07.toByte)
    val (sk, _) = kes.createKeyPair(seed, (2, 2), 0L)
    val msg = "round-trip-message".getBytes("UTF-8")
    val sig = kes.sign(sk, msg)

    val bytes = SignatureCodec.encodeSignature(sig)
    SignatureCodec.decodeSignature(bytes) match {
      case Right(decoded) => expect.same(sig, decoded)
      case Left(err)      => failure(s"decode failed: ${err.message}")
    }
  }

  pureTest("decoded signature verifies against the original VK") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(0x13.toByte)
    val (sk, vk) = kes.createKeyPair(seed, (2, 2), 0L)
    val msg = "verify-after-decode".getBytes("UTF-8")
    val sig = kes.sign(sk, msg)

    val bytes = SignatureCodec.encodeSignature(sig)
    SignatureCodec.decodeSignature(bytes) match {
      case Right(decoded) => expect(kes.verify(decoded, msg, vk))
      case Left(err)      => failure(s"decode failed: ${err.message}")
    }
  }

  pureTest("decode of empty bytes returns Left(MalformedTree)") {
    SignatureCodec.decodeSignature(Array.empty[Byte]) match {
      case Left(KesError.MalformedTree(_)) => success
      case Left(other)                     => failure(s"expected MalformedTree, got $other")
      case Right(_)                        => failure("expected Left for empty input")
    }
  }

  pureTest("decode of truncated bytes returns Left(MalformedTree)") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(0x29.toByte)
    val (sk, _) = kes.createKeyPair(seed, (2, 2), 0L)
    val sig = kes.sign(sk, "truncate-me".getBytes("UTF-8"))
    val bytes = SignatureCodec.encodeSignature(sig)
    val truncated = bytes.take(bytes.length / 2)

    SignatureCodec.decodeSignature(truncated) match {
      case Left(KesError.MalformedTree(_)) => success
      case Left(other)                     => failure(s"expected MalformedTree, got $other")
      case Right(_)                        => failure("expected Left for truncated input")
    }
  }

  pureTest("decode of garbage bytes with implausible length prefix returns Left(MalformedTree)") {
    // 4-byte big-endian Int interpreted as a length: 0xFFFFFFFF = -1 -> negative, rejected.
    val garbage = Array.fill[Byte](16)(0xff.toByte)
    SignatureCodec.decodeSignature(garbage) match {
      case Left(KesError.MalformedTree(_)) => success
      case Left(other)                     => failure(s"expected MalformedTree, got $other")
      case Right(_)                        => failure("expected Left for garbage input")
    }
  }

  pureTest("OperationalKeyMaker.encodeSignature/decodeSignature forwarders behave identically") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(0x55.toByte)
    val (sk, vk) = kes.createKeyPair(seed, (2, 2), 0L)
    val msg = "forwarder-test".getBytes("UTF-8")
    val sig = kes.sign(sk, msg)

    val viaForwarder = OperationalKeyMaker.encodeSignature(sig)
    val viaInternal = SignatureCodec.encodeSignature(sig)
    val sameBytes = viaForwarder.toList == viaInternal.toList

    OperationalKeyMaker.decodeSignature(viaForwarder) match {
      case Right(decoded) =>
        expect(sameBytes) &&
        expect(OperationalKeyMaker.verify(decoded, msg, vk))
      case Left(err) => failure(s"decode failed: ${err.message}")
    }
  }
}
