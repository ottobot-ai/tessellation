package io.constellationnetwork.node.shared.infrastructure.genesis

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.currency.schema.currency.CurrencySnapshot
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.schema.GlobalSnapshotInfo
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.generators.{addressGen, balanceGen}
import io.constellationnetwork.security._
import io.constellationnetwork.security.signature.Signed

import fs2.io.file.Files
import fs2.text
import io.circe.parser.{decode, parse}
import io.circe.syntax._
import io.circe.{Json, Printer}
import org.scalacheck.Gen
import weaver._
import weaver.scalacheck._

object GenesisFSSuite extends MutableIOSuite with Checkers {
  type Res = (KryoSerializer[IO], JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](nodeSharedKryoRegistrar)
    sp <- SecurityProvider.forAsync[IO]
    implicit0(js: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (ks, js, h, sp)

  val gen: Gen[(Map[Address, Balance], Address)] = for {
    balancesMap <- Gen.mapOf(balanceGen.flatMap(balance => addressGen.map((_, balance))))
    identifier <- addressGen
  } yield (balancesMap, identifier)

  test("writes and loads genesis") { res =>
    implicit val (ks, js, h, sp) = res
    val genesisFS = GenesisFS.make[IO, CurrencySnapshot]

    forall(gen) {
      case (balances, identifier) =>
        Files[F].tempDirectory.use { tempDir =>
          for {
            kp <- KeyPairGenerator.makeKeyPair
            genesis = CurrencySnapshot.mkGenesis(balances, None, None)
            signedGenesis <- Signed.forAsyncHasher(genesis, kp)
            _ <- genesisFS.write(signedGenesis, identifier, tempDir)
            loaded <- genesisFS.loadSignedGenesis(tempDir / "genesis.snapshot")
          } yield expect.eql(loaded, signedGenesis)
        }

    }
  }

  test("writes and loads metadata") { res =>
    implicit val (ks, js, h, sp) = res
    val genesisFS = GenesisFS.make[IO, CurrencySnapshot]

    forall(gen) {
      case (balances, identifier) =>
        Files[F].tempDirectory.use { tempDir =>
          for {
            kp <- KeyPairGenerator.makeKeyPair
            genesis = CurrencySnapshot.mkGenesis(balances, None, None)
            signedGenesis <- Signed.forAsyncHasher(genesis, kp)
            _ <- genesisFS.write(signedGenesis, identifier, tempDir)
            loaded <- Files[F]
              .readAll(tempDir / "genesis.address")
              .through(text.utf8.decode)
              .compile
              .string
          } yield expect.eql(loaded, identifier.value.value)
        }
    }
  }

  // Tier-1 test-vector round-trip. See `project_test_vector_pattern` memory for design rationale.
  // Verifies that an `L0GenesisData` payload writes to a UTF-8 JSON file and reads back byte-equal
  // via `GenesisFS.loadL0Genesis`. This is the primary regression gate for the loader.
  test("loadL0Genesis round-trips a Tier-1 fixture") { res =>
    implicit val (_, js, _, _) = res
    val genesisFS = GenesisFS.make[IO, CurrencySnapshot]

    val fixture = L0GenesisData(
      _meta = L0GenesisMeta(
        generatorVersion = "tools-test",
        generatedAt = "1970-01-01T00:00:00Z",
        invocation = "test-roundtrip",
        seed = 12345L,
        expectedProperties = List("round-trip preserves byte equality")
      ),
      networkMagic = "test",
      activationOrdinal = 0L,
      startingEpochProgress = 0L,
      protocolParams = L0GenesisProtocolParams.default,
      operators = List.empty,
      delegatedStakes = List.empty,
      nodeCollaterals = List.empty,
      initialBalances = List(L0GenesisBalance("DAG0qFf3aNtg9hLNcviAmwLTm1zhMKP5rxzsSrAT", 100L))
    )

    Files[IO].tempDirectory.use { tempDir =>
      val path = tempDir / "l0-genesis.json"
      val printer = Printer.spaces2.copy(sortKeys = true, dropNullValues = false)
      val jsonStr = printer.print(fixture.asJson)

      for {
        _ <- fs2.Stream
          .emit(jsonStr)
          .through(text.utf8.encode)
          .through(Files[IO].writeAll(path))
          .compile
          .drain
        loaded <- genesisFS.loadL0Genesis(path)
        // Re-encode the loaded data and compare against the original write — proves the loader
        // preserves all fields including `_meta`, `protocolParams`, and the empty record lists.
        reEncoded = printer.print(loaded.asJson)
      } yield expect.eql(reEncoded, jsonStr)
    }
  }

  // Every committed greenfield fixture must use the current atomic operator-key schema and pass the
  // same strict loader as GL0 startup. Parse-only stale fixtures are not retained.
  test("Tier-1 fixtures decode and pass strict atomic operator-key loading") { res =>
    implicit val (_, js, _, sp) = res
    val genesisFS = GenesisFS.make[IO, CurrencySnapshot]

    // Each tuple is (relativePath, expectedOperators, expectedStakes, expectedCollaterals).
    val fixtures = List(
      ("test-vectors/genesis/8-node-uniform-stake.json", 8, 8, 0),
      ("test-vectors/genesis/8-node-skewed-stake.json", 8, 8, 0),
      ("test-vectors/genesis/8-node-with-collateral.json", 8, 8, 8),
      ("test-vectors/genesis/3-node-minimal.json", 3, 3, 0)
    )

    // Try relative-to-cwd first; fall back to walking up to find the project root (sbt usually
    // runs tests from the module dir, while `just test` runs from the project root).
    def resolveFixture(rel: String): IO[fs2.io.file.Path] = {
      val cwd = fs2.io.file.Path(System.getProperty("user.dir"))
      val candidates = List(
        cwd / rel,
        cwd / s"../../$rel",
        cwd / s"../$rel"
      )
      candidates
        .collectFirstSomeM(p => Files[IO].exists(p).map(if (_) Some(p) else None))
        .flatMap(
          _.liftTo[IO](
            new IllegalStateException(
              s"Required tracked genesis fixture is missing from every repository-relative candidate: $rel"
            )
          )
        )
    }

    fixtures.traverse {
      case (rel, expOps, expStakes, expColls) =>
        resolveFixture(rel).flatMap { path =>
          implicit val hasher: Hasher[IO] = res._3
          implicit val hasherSelector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(hasher)

          genesisFS.loadL0Genesis(path).flatMap { data =>
            L0GenesisLoader.augmentSnapshotInfo[IO](GlobalSnapshotInfo.empty, data).attempt.map { augmentationResult =>
              expect
                .eql(data.operators.size, expOps)
                .and(expect.eql(data.delegatedStakes.size, expStakes))
                .and(expect.eql(data.nodeCollaterals.size, expColls))
                .and(expect(data.operators.forall(op => op.kesMasterVk.length == 64 && op.vrfVk.length == 64)))
                .and(expect(augmentationResult.isRight))
            }
          }
        }
    }.map(_.combineAll)
  }

  test("tracked L0 genesis fixtures contain no private fields or PKCS8 private-key material") { _ =>
    val fixtures = List(
      "test-vectors/genesis/8-node-uniform-stake.json",
      "test-vectors/genesis/8-node-skewed-stake.json",
      "test-vectors/genesis/8-node-with-collateral.json",
      "test-vectors/genesis/3-node-minimal.json"
    )

    def fieldNames(json: Json): Set[String] =
      json.arrayOrObject(
        Set.empty,
        _.iterator.flatMap(fieldNames).toSet,
        obj => obj.keys.toSet ++ obj.values.iterator.flatMap(fieldNames)
      )

    val cwd = fs2.io.file.Path(System.getProperty("user.dir"))
    def resolve(rel: String): IO[fs2.io.file.Path] = {
      val candidates = List(cwd / rel, cwd / s"../../$rel", cwd / s"../$rel")
      candidates
        .collectFirstSomeM(path => Files[IO].exists(path).map(if (_) Some(path) else None))
        .flatMap(_.liftTo[IO](new IllegalStateException(s"Required tracked genesis fixture is missing: $rel")))
    }

    fixtures.traverse { rel =>
      for {
        path <- resolve(rel)
        raw <- Files[IO].readAll(path).through(text.utf8.decode).compile.string
        json <- parse(raw).leftMap(error => new IllegalArgumentException(s"$rel is not valid JSON: $error")).liftTo[IO]
        normalizedFields = fieldNames(json).map(_.toLowerCase)
        forbiddenFields = normalizedFields.filter(name => name.contains("private") || name.contains("secret"))
        containsEcPkcs8 =
          raw.toLowerCase.contains("30818d020100301006072a8648ce3d020106052b8104000a")
      } yield
        expect(forbiddenFields.isEmpty) &&
          expect(!containsEcPkcs8)
    }.map(_.combineAll)
  }

  test("abandoned split or incomplete operator-key records do not decode") { _ =>
    val abandonedSplitSchema =
      """{
        |  "_meta": {
        |    "generatorVersion": "stale",
        |    "generatedAt": "1970-01-01T00:00:00Z",
        |    "invocation": "stale",
        |    "seed": 0,
        |    "expectedProperties": []
        |  },
        |  "networkMagic": "test",
        |  "activationOrdinal": 0,
        |  "startingEpochProgress": 0,
        |  "protocolParams": {
        |    "lddCutoff": 16,
        |    "etaRotationSnapshots": 3174,
        |    "genesisEta": "tessellation-nakamoto-genesis",
        |    "startingEpochProgress": 0
        |  },
        |  "operators": [{
        |    "peerId": "00",
        |    "address": "DAG0",
        |    "vrfPublicKey": null
        |  }],
        |  "kesRegistrations": [],
        |  "delegatedStakes": [],
        |  "nodeCollaterals": [],
        |  "initialBalances": []
        |}""".stripMargin

    IO.pure(expect(decode[L0GenesisData](abandonedSplitSchema).isLeft))
  }

}
