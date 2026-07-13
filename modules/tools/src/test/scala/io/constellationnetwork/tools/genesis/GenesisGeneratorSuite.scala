package io.constellationnetwork.tools.genesis

import java.security.PublicKey

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signing

import weaver.SimpleIOSuite

/** Round-trip tests for `GenesisGenerator.generate`: every operator receives one complete KES+VRF record whose long-term signature covers
  * the canonical domain-separated chain-context preimage.
  */
object GenesisGeneratorSuite extends SimpleIOSuite {

  private def sp: Resource[IO, SecurityProvider[IO]] = SecurityProvider.forAsync[IO]

  // Smaller-than-default opts so the test runs quickly. Two operators is enough to confirm the
  // per-operator loop runs and that registrations stay paired with operators by index.
  private def opts(seed: Long = 42L): GenesisGenerator.GeneratorOpts =
    GenesisGenerator.GeneratorOpts(
      outputDir = "/tmp/unused-in-tests",
      numOperators = 2,
      stakeDistribution = List(BigDecimal("0.5"), BigDecimal("0.5")),
      stakeBudgetDatum = 1000000L,
      collateralPerOperator = 0L,
      initialBalancesCsv = None,
      seed = seed,
      keysFromDir = None,
      networkMagic = "test-cluster",
      startingEpochProgress = 0L
    )

  test("generate: one atomic consensus-key record is populated per operator") {
    sp.use { implicit s =>
      for {
        out <- GenesisGenerator.generate[IO](opts(), invocation = "test")
      } yield
        expect.same(2, out.l0Genesis.operators.size) &&
          expect.same(2, out.kesSecretKeys.size)
    }
  }

  test("generate: each kesVk is exactly 64 hex chars (32 raw bytes — Blake2b-256 root)") {
    sp.use { implicit s =>
      for {
        out <- GenesisGenerator.generate[IO](opts(), invocation = "test")
        operators = out.l0Genesis.operators
      } yield
        operators
          .map(r => expect.same(64, r.kesMasterVk.length) && expect.same(32, Hex(r.kesMasterVk).toBytes.length))
          .combineAll
    }
  }

  test("generate: kesVkStep is 0 for all entries (fresh master key at genesis time)") {
    sp.use { implicit s =>
      for {
        out <- GenesisGenerator.generate[IO](opts(), invocation = "test")
        operators = out.l0Genesis.operators
      } yield operators.map(r => expect.same(0, r.kesMasterVkStep) && expect.same(0L, r.kesPeriodOffset)).combineAll
    }
  }

  // Recover the public point from a BouncyCastle EC private key (same approach as
  // L0GenesisLoader.derivePublicKey). The generator emits operator long-term keys via
  // `deterministicKeyPair` so we walk the same pipeline here to get the matching pubkey for
  // signature verification.
  private def derivePublicKeyFromOperator(
    operatorIndex: Int,
    seedVal: Long
  )(implicit s: SecurityProvider[IO]): IO[PublicKey] =
    GenesisGenerator.deterministicKeyPair[IO](seedVal, s"operator:$operatorIndex").map(_.getPublic)

  test("generate: longTermSignature verifies against the canonical complete KES+VRF chain-context preimage") {
    sp.use { implicit s =>
      val seedVal = 123L
      for {
        out <- GenesisGenerator.generate[IO](opts(seedVal), invocation = "test")
        verifications <- out.l0Genesis.operators.zipWithIndex.traverse {
          case (operator, i) =>
            for {
              pub <- derivePublicKeyFromOperator(i, seedVal)
              preimage = io.constellationnetwork.node.shared.domain.genesis.types.L0GenesisOperator.signaturePreimage(
                out.l0Genesis.networkMagic,
                out.l0Genesis.activationOrdinal,
                out.l0Genesis.startingEpochProgress,
                Hex(operator.peerId).toBytes,
                operator.address,
                Hex(operator.kesMasterVk).toBytes,
                operator.kesMasterVkStep,
                operator.kesPeriodOffset,
                Hex(operator.vrfVk).toBytes
              )
              sigBytes = Hex(operator.longTermSignature).toBytes
              ok <- Signing.verifySignature[IO](preimage, sigBytes)(pub)
            } yield expect(ok)
        }
      } yield verifications.combineAll
    }
  }

  test("generate: each emitted record carries exact 32-byte KES and VRF keys") {
    sp.use { implicit s =>
      for {
        out <- GenesisGenerator.generate[IO](opts(), invocation = "test")
        ops = out.l0Genesis.operators
      } yield
        ops
          .map(op => expect.same(32, Hex(op.kesMasterVk).toBytes.length) && expect.same(32, Hex(op.vrfVk).toBytes.length))
          .combineAll
    }
  }

  test("generate: each operator's KES SK bytes are non-empty and unique (no key reuse across operators)") {
    sp.use { implicit s =>
      for {
        out <- GenesisGenerator.generate[IO](opts(), invocation = "test")
      } yield {
        val skBlobs = out.kesSecretKeys.map(_.bytes.toList)
        expect(skBlobs.forall(_.nonEmpty)) &&
        expect.same(skBlobs.size, skBlobs.distinct.size)
      }
    }
  }

  test("writeKesSecretKeys: writes one kes-sk.bin file per operator with restricted perms (where supported)") {
    import java.nio.file.{Files => JFiles, Paths => JPaths}
    import java.nio.file.attribute.PosixFileAttributeView

    sp.use { implicit s =>
      val seedVal = 7L
      val tmpDir = JFiles.createTempDirectory("kes-sk-test-").toAbsolutePath.toString
      for {
        out <- GenesisGenerator.generate[IO](opts(seedVal), invocation = "test")
        paths <- GenesisGenerator.writeKesSecretKeys[IO](tmpDir, out.kesSecretKeys)
        // Each file exists, is non-empty, and (on POSIX) has 0600.
        checks <- IO.delay {
          paths
            .zip(out.kesSecretKeys)
            .map {
              case (p, sk) =>
                val path = JPaths.get(p)
                val exists = JFiles.exists(path)
                val size = if (exists) JFiles.size(path) else 0L
                val expectedRel = s"keys/operator-${sk.operatorIndex}/kes-sk.bin"
                val pathOk = p.endsWith(expectedRel)
                val permsOk = scala.util.Try {
                  val view = JFiles.getFileAttributeView(path, classOf[PosixFileAttributeView])
                  if (view == null) true // non-POSIX FS, no-op
                  else {
                    val perms = view.readAttributes().permissions()
                    val asString = java.nio.file.attribute.PosixFilePermissions.toString(perms)
                    asString == "rw-------"
                  }
                }
                  .getOrElse(true)
                expect(exists) && expect(size > 0L) && expect(pathOk) && expect(permsOk)
            }
            .combineAll
        }
      } yield checks
    }
  }
}
