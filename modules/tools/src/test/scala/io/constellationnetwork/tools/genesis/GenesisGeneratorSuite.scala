package io.constellationnetwork.tools.genesis

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files => JFiles, LinkOption, Path => JPath}
import java.security.PublicKey

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.jdk.CollectionConverters._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.tokenLock.TokenLockReference
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signing

import io.circe.syntax._
import weaver.SimpleIOSuite

/** Round-trip tests for `GenesisGenerator.generate`: every operator receives one complete KES+VRF record whose long-term signature covers
  * the canonical domain-separated chain-context preimage.
  */
object GenesisGeneratorSuite extends SimpleIOSuite {

  private def sp: Resource[IO, SecurityProvider[IO]] = SecurityProvider.forAsync[IO]
  private implicit val hasher: Hasher[IO] = Hasher.forCanonicalJson[IO]
  private implicit val hasherSelector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(hasher)
  private val secretDirectoryMode = PosixFilePermissions.fromString("rwx------")

  private def withTempDirectory[A](use: JPath => IO[A]): IO[A] =
    Resource
      .make(IO.blocking(JFiles.createTempDirectory("genesis-generator-suite-"))) { root =>
        IO.blocking {
          val stream = JFiles.walk(root)
          try stream.iterator().asScala.toList.sortBy(_.getNameCount).reverse.foreach(JFiles.deleteIfExists)
          finally stream.close()
        }.void
      }
      .use(use)

  private def mode(path: JPath): String =
    PosixFilePermissions.toString(JFiles.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS))

  private def legacyLogic(delegate: Hasher[IO]): Hasher[IO] =
    new Hasher[IO] {
      def hash[A: io.circe.Encoder](data: A): IO[Hash] = delegate.hash(data)
      def hashBytes(bytes: Array[Byte]): IO[Hash] = delegate.hashBytes(bytes)
      def compare[A: io.circe.Encoder](data: A, expectedHash: Hash): IO[Boolean] = delegate.compare(data, expectedHash)
      def getLogic(ordinal: SnapshotOrdinal): io.constellationnetwork.security.HashLogic = KryoHash
      def prefixedHash[A: io.circe.Encoder](data: A, prefix: Array[Byte]): IO[Hash] = delegate.prefixedHash(data, prefix)
    }

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
      operatorKeyAlias = "alias",
      operatorKeyPasswordEnv = None,
      testOnlyDeterministicOperatorKeys = true,
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

  test("generate: public economic bundles are fully signed and contain no private-key fields") {
    sp.use { implicit s =>
      for {
        out <- GenesisGenerator.generate[IO](opts().copy(collateralPerOperator = 1000L), invocation = "test")
        stakeChecks <- out.l0Genesis.delegatedStakes.traverse { bundle =>
          for {
            eventValid <- bundle.signedEvent.hasValidSignature[IO]
            lockValid <- bundle.signedBackingTokenLock.hasValidSignature[IO]
            lockRef <- TokenLockReference.of[IO](bundle.signedBackingTokenLock)
          } yield
            eventValid &&
              lockValid &&
              bundle.signedEvent.tokenLockRef == lockRef.hash &&
              bundle.signedEvent.proofs.head.id == bundle.signedBackingTokenLock.proofs.head.id
        }
        collateralChecks <- out.l0Genesis.nodeCollaterals.traverse { bundle =>
          for {
            eventValid <- bundle.signedEvent.hasValidSignature[IO]
            lockValid <- bundle.signedBackingTokenLock.hasValidSignature[IO]
            lockRef <- TokenLockReference.of[IO](bundle.signedBackingTokenLock)
          } yield
            eventValid &&
              lockValid &&
              bundle.signedEvent.tokenLockRef == lockRef.hash &&
              bundle.signedEvent.proofs.head.id == bundle.signedBackingTokenLock.proofs.head.id
        }
        publicJson = out.l0Genesis.asJson.noSpaces.toLowerCase
      } yield
        expect(stakeChecks.forall(identity)) &&
          expect(collateralChecks.forall(identity)) &&
          expect(!publicJson.contains("privatekey")) &&
          expect(!publicJson.contains("pkcs8")) &&
          expect(!publicJson.contains("secretkey"))
    }
  }

  test("generate: rejects a legacy hash schedule at the first live ordinal") {
    sp.use { implicit s =>
      val legacy = legacyLogic(hasher)
      val legacyFirstLiveSelector: HasherSelector[IO] = new HasherSelector[IO] {
        def getCurrent: Hasher[IO] = hasher
        def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[IO] =
          if (ordinal == GenesisGenerator.FirstLiveOrdinal) legacy else hasher
      }

      GenesisGenerator
        .generate[IO](opts(), invocation = "test")(implicitly[cats.effect.Async[IO]], legacyFirstLiveSelector, s)
        .attempt
        .map { result =>
          expect(
            result.swap.exists(
              _.getMessage.contains("supports only the greenfield current JSON hash at first live ordinal")
            )
          )
        }
    }
  }

  test("generate: production keystore input requires an explicit password environment variable") {
    sp.use { implicit s =>
      GenesisGenerator
        .generate[IO](
          opts().copy(
            keysFromDir = Some("/tmp/offline-ceremony"),
            operatorKeyPasswordEnv = None,
            testOnlyDeterministicOperatorKeys = false
          ),
          invocation = "test"
        )
        .attempt
        .map(result => expect(result.swap.exists(_.getMessage.contains("--operator-key-password-env is required"))))
    }
  }

  test("generate: rejects a negative individual stake weight even when the aggregate is one") {
    sp.use { implicit s =>
      GenesisGenerator
        .generate[IO](
          opts().copy(stakeDistribution = List(BigDecimal(-1), BigDecimal(2))),
          invocation = "test"
        )
        .attempt
        .map(result => expect(result.swap.exists(_.getMessage.contains("weights must be non-negative"))))
    }
  }

  test("CLI stake-weight parsing rejects malformed or empty entries instead of silently dropping them") {
    val parse = io.constellationnetwork.tools.cli.method.GenerateGenesisCmd.parseStakeWeights _

    IO.pure(
      expect(parse("0.5,not-a-number,0.5").isLeft) &&
        expect(parse("0.5,,0.5").isLeft) &&
        expect.same(Right(List(BigDecimal("0.5"), BigDecimal("0.5"))), parse("0.5,0.5"))
    )
  }

  test("generate: balance CSV rejects malformed and negative rows instead of silently changing allocation") {
    sp.use { implicit s =>
      withTempDirectory { tmpDir =>
        for {
          operatorKey <- GenesisGenerator.deterministicKeyPair[IO](42L, "operator:0")
          validAddress = operatorKey.getPublic.toAddress.value.value
          malformed = tmpDir.resolve("malformed.csv")
          negative = tmpDir.resolve("negative.csv")
          _ <- IO.blocking {
            JFiles.writeString(malformed, "not-an-address-balance-row\n")
            JFiles.writeString(negative, s"$validAddress,-1\n")
          }
          malformedResult <- GenesisGenerator
            .generate[IO](opts().copy(initialBalancesCsv = Some(malformed.toString)), invocation = "test")
            .attempt
          negativeResult <- GenesisGenerator
            .generate[IO](opts().copy(initialBalancesCsv = Some(negative.toString)), invocation = "test")
            .attempt
        } yield
          expect(malformedResult.swap.exists(_.getMessage.contains("must contain exactly address,balance"))) &&
            expect(negativeResult.swap.exists(_.getMessage.contains("has a negative balance")))
      }
    }
  }

  test("generate: rejects a complete allocation outside the protocol Long domain before persistence") {
    sp.use { implicit s =>
      GenesisGenerator
        .generate[IO](
          opts().copy(stakeBudgetDatum = 0L, collateralPerOperator = Long.MaxValue),
          invocation = "test"
        )
        .attempt
        .map(result => expect(result.swap.exists(_.getMessage.contains("Generated L0 genesis allocation is invalid"))))
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

  test("writeAllSecretKeys: creates every secret directory as 0700 and file as 0600, then zeroes all arrays") {
    sp.use { implicit s =>
      val seedVal = 7L
      withTempDirectory { tmpDir =>
        for {
          out <- GenesisGenerator.generate[IO](opts(seedVal).copy(collateralPerOperator = 1000L), invocation = "test")
          expectedKes = out.kesSecretKeys.map(secret => secret.bytes.clone())
          expectedEconomic = out.economicSecretKeys.map(secret => secret.bytes.clone())
          paths <- GenesisGenerator.writeAllSecretKeys[IO](
            tmpDir.toString,
            out.kesSecretKeys,
            out.economicSecretKeys
          )
          checks <- IO.blocking {
            val allFiles = paths._1.map(JPath.of(_)) ++ paths._2.map(JPath.of(_))
            val allDirectories =
              List(tmpDir.resolve("keys"), tmpDir.resolve("keys/economic")) ++
                out.kesSecretKeys.map(secret => tmpDir.resolve(s"keys/operator-${secret.operatorIndex}")) ++
                out.economicSecretKeys.map(secret => tmpDir.resolve(s"keys/economic/operator-${secret.operatorIndex}"))

            expect(allFiles.forall(path => JFiles.size(path) > 0L && mode(path) == "rw-------")) &&
            expect(allDirectories.forall(path => mode(path) == "rwx------")) &&
            expect(paths._1.zip(expectedKes).forall { case (path, bytes) => JFiles.readAllBytes(JPath.of(path)).sameElements(bytes) }) &&
            expect(
              paths._2.zip(expectedEconomic).forall { case (path, bytes) => JFiles.readAllBytes(JPath.of(path)).sameElements(bytes) }
            ) &&
            expect(out.kesSecretKeys.forall(_.bytes.forall(_ == 0.toByte))) &&
            expect(out.economicSecretKeys.forall(_.bytes.forall(_ == 0.toByte)))
          }
        } yield checks
      }
    }
  }

  test("writeAllSecretKeys: rejects symlink custody paths, does not overwrite the target, and zeroes every array on failure") {
    sp.use { implicit s =>
      withTempDirectory { tmpDir =>
        for {
          out <- GenesisGenerator.generate[IO](opts(8L), invocation = "test")
          outside = tmpDir.resolve("outside")
          _ <- IO.blocking(JFiles.createDirectory(outside, PosixFilePermissions.asFileAttribute(secretDirectoryMode)))
          _ <- IO.blocking(JFiles.createSymbolicLink(tmpDir.resolve("keys"), outside))
          result <- GenesisGenerator
            .writeAllSecretKeys[IO](tmpDir.toString, out.kesSecretKeys, out.economicSecretKeys)
            .attempt
          outsideEntries <- IO.blocking {
            val stream = JFiles.list(outside)
            try stream.count()
            finally stream.close()
          }
        } yield
          expect(result.swap.exists(_.getMessage.contains("Secret path is not a real directory"))) &&
            expect.same(0L, outsideEntries) &&
            expect(out.kesSecretKeys.forall(_.bytes.forall(_ == 0.toByte))) &&
            expect(out.economicSecretKeys.forall(_.bytes.forall(_ == 0.toByte)))
      }
    }
  }

  test("persistGeneratedOutputs: a pre-L0 publication failure leaves no public L0 anchor") {
    sp.use { implicit s =>
      withTempDirectory { tmpDir =>
        for {
          out <- GenesisGenerator.generate[IO](opts(9L), invocation = "test")
          _ <- IO.blocking(JFiles.writeString(tmpDir.resolve("cl1-genesis.json"), "occupied"))
          result <- GenesisGenerator
            .persistGeneratedOutputs[IO](tmpDir.toString, out, """{"l0":"public"}""", Some("""{"cl1":"public"}"""))
            .attempt
          l0Exists <- IO.blocking(JFiles.exists(tmpDir.resolve("l0-genesis.json"), LinkOption.NOFOLLOW_LINKS))
          kesExists <- IO.blocking(JFiles.exists(tmpDir.resolve("keys/operator-0/kes-sk.bin"), LinkOption.NOFOLLOW_LINKS))
        } yield
          expect(result.isLeft) &&
            expect(!l0Exists) &&
            expect(kesExists) &&
            expect(out.kesSecretKeys.forall(_.bytes.forall(_ == 0.toByte))) &&
            expect(out.economicSecretKeys.forall(_.bytes.forall(_ == 0.toByte)))
      }
    }
  }

  test("persistGeneratedOutputs: publishes L0 last without leaving a temporary public artifact") {
    sp.use { implicit s =>
      withTempDirectory { tmpDir =>
        for {
          out <- GenesisGenerator.generate[IO](opts(10L), invocation = "test")
          persisted <- GenesisGenerator.persistGeneratedOutputs[IO](
            tmpDir.toString,
            out,
            """{"l0":"public"}""",
            Some("""{"cl1":"public"}""")
          )
          names <- IO.blocking {
            val stream = JFiles.list(tmpDir)
            try stream.iterator().asScala.map(_.getFileName.toString).toList
            finally stream.close()
          }
        } yield
          expect(JFiles.exists(JPath.of(persisted.l0Genesis))) &&
            expect(persisted.cl1Genesis.exists(path => JFiles.exists(JPath.of(path)))) &&
            expect(!names.exists(_.endsWith(".tmp")))
      }
    }
  }
}
