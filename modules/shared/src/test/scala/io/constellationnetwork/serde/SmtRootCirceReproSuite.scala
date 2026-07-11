package io.constellationnetwork.serde

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher

import eu.timepit.refined.auto._
import io.circe.syntax._
import weaver.MutableIOSuite

/** Current-schema regression tests for the optional SMT root on the JSON follower path.
  *
  * Historical deployed snapshot fixtures are deliberately excluded: this greenfield fork has one strict schema and does not support
  * decoding fork-era wire shapes.
  */
object SmtRootCirceReproSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    JsonSerializer.forAsync[IO].asResource.flatMap { json =>
      SecurityProvider.forAsync[IO].map(sp => (json, sp))
    }

  private def h(bytes: String): Hash = Hash(bytes * 32)

  private val someSmtRoot: Hash = h("ab")
  private val someCurrencyRoots: CurrencySnapshotMptRoots = CurrencySnapshotMptRoots(h("cc"), h("dd"))

  private def proofWith(smt: Option[Hash]): GlobalSnapshotStateProof =
    GlobalSnapshotStateProof(
      lastStateChannelSnapshotHashesProof = h("11"),
      lastTxRefsProof = h("22"),
      balancesProof = h("33"),
      lastCurrencySnapshotsProof = Some(someCurrencyRoots),
      activeAllowSpends = None,
      activeTokenLocks = None,
      tokenLockBalances = None,
      lastAllowSpendRefs = None,
      lastTokenLockRefs = None,
      updateNodeParameters = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = None,
      nodeCollateralWithdrawals = None,
      priceState = None,
      lastGlobalSnapshotsWithCurrency = None,
      mptRoot = Some(h("99")),
      historicalStakeSnapshots = Some(h("88")),
      smtRoot = smt
    )

  private def snapshotWith(proof: GlobalSnapshotStateProof): GlobalIncrementalSnapshot =
    GlobalIncrementalSnapshot(
      ordinal = SnapshotOrdinal.unsafeApply(256L),
      height = Height(0L),
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      shardCheckpoints = SortedMap.empty,
      rewards = SortedSet.empty,
      delegateRewards = Some(SortedMap.empty),
      epochProgress = EpochProgress.MinValue,
      nextFacilitators = NonEmptyList.one(PeerId(Hex("aa" * 64))),
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = proof,
      allowSpendBlocks = Some(SortedSet.empty),
      tokenLockBlocks = Some(SortedSet.empty),
      spendActions = Some(SortedMap.empty),
      updateNodeParameters = Some(SortedMap.empty),
      artifacts = Some(SortedSet.empty),
      activeDelegatedStakes = Some(SortedMap.empty),
      delegatedStakesWithdrawals = Some(SortedMap.empty),
      activeNodeCollaterals = Some(SortedMap.empty),
      nodeCollateralWithdrawals = Some(SortedMap.empty),
      version = SnapshotVersion("0.0.1"),
      slotCertificate = None,
      eta = None
    )

  test("GlobalSnapshotStateProof with SMT and currency roots round-trips through Circe") { _ =>
    val original = proofWith(Some(someSmtRoot))
    val decoded = original.asJson.as[GlobalSnapshotStateProof]
    IO.pure(
      expect(decoded == Right(original))
        .and(expect.same(decoded.toOption.flatMap(_.smtRoot), Some(someSmtRoot)))
        .and(expect.same(decoded.toOption.flatMap(_.lastCurrencySnapshotsProof), Some(someCurrencyRoots)))
    )
  }

  test("GlobalSnapshotStateProof with SMT root is JSON hash-stable") { _ =>
    val original = proofWith(Some(someSmtRoot))
    val firstJson = original.asJson.noSpaces
    val decoded = original.asJson.as[GlobalSnapshotStateProof].toOption.get
    IO.pure(expect.same(firstJson, decoded.asJson.noSpaces))
  }

  test("current Signed[GlobalIncrementalSnapshot] survives Brotli JSON with byte-identical re-encode") {
    case (json, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      val snapshot = snapshotWith(proofWith(Some(someSmtRoot)))
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        signed <- {
          implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
          forAsyncHasher(snapshot, kp)
        }
        wire <- json.serialize(signed)
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](wire)
        decoded <- IO.fromEither(decodedE)
        reWire <- json.serialize(decoded)
      } yield
        expect
          .same(decoded.value.stateProof.smtRoot, Some(someSmtRoot))
          .and(expect(decoded.value == snapshot))
          .and(expect(java.util.Arrays.equals(wire, reWire)))
  }

  test("follower signature check succeeds after current-schema JSON decode") {
    case (json, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val snapshot = snapshotWith(proofWith(Some(someSmtRoot)))
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        signed <- forAsyncHasher(snapshot, kp)
        wire <- json.serialize(signed)
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](wire)
        decoded <- IO.fromEither(decodedE)
        checked <- decoded.toHashedWithSignatureCheck
      } yield expect(checked.isRight)
  }

  test("follower signature check succeeds when current-schema SMT root is absent") {
    case (json, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val snapshot = snapshotWith(proofWith(None))
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        signed <- forAsyncHasher(snapshot, kp)
        wire <- json.serialize(signed)
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](wire)
        decoded <- IO.fromEither(decodedE)
        checked <- decoded.toHashedWithSignatureCheck
      } yield expect(checked.isRight).and(expect(decoded.value.stateProof.smtRoot.isEmpty))
  }
}
