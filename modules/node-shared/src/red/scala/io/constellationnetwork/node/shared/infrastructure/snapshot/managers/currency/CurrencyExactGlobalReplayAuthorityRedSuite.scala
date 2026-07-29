package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.LastGlobalSnapshotsSyncConfig
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap.{CurrencyId, SwapAmount}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.{NonNegLong, PosInt, PosLong}
import fs2.concurrent.SignallingRef
import weaver.MutableIOSuite

/** RED gates for candidate-lineage authority during framework-currency recreation.
  *
  * These gates do not weaken universal GL0 execution of native GL1/DAG-token transitions. They cover only historical GL0 inputs consumed
  * while reproducing CL1 framework state: a retained sibling or node-local head/cache must never become the execution authority, and one
  * candidate must have a bounded, shared exact-lineage replay view.
  *
  * The source checks are explicitly temporary boundaries while that replay-view type is unsettled. Replace each with a typed behavioral
  * test when the capability lands; do not preserve source spelling as an API contract.
  */
object CurrencyExactGlobalReplayAuthorityRedSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong.unsafeFrom(Long.MaxValue)))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  override type Res = (Hasher[IO], JsonSerializer[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] = for {
    sp <- SecurityProvider.forAsync[IO]
    implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
  } yield (Hasher.forJson[IO], json, sp)

  private val syncConfig = LastGlobalSnapshotsSyncConfig(NonNegLong(2L), PosInt(10))

  private def ord(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))

  private val emptyInfo: GlobalSnapshotInfo = GlobalSnapshotInfo(
    SortedMap.empty,
    SortedMap.empty,
    SortedMap.empty,
    SortedMap.empty,
    SortedMap.empty,
    None,
    None,
    None,
    None,
    None,
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    Some(SortedMap.empty),
    None,
    SortedMap.empty
  )

  private def snapshotAt(
    ordinal: SnapshotOrdinal,
    spendActions: SortedMap[Address, List[SpendAction]]
  )(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): IO[Hashed[GlobalIncrementalSnapshot]] =
    emptyInfo.stateProof[IO](ordinal).flatMap { stateProof =>
      Signed(
        GlobalIncrementalSnapshot(
          ordinal,
          Height.MinValue,
          SubHeight.MinValue,
          Hash.empty,
          SortedSet.empty,
          SortedMap.empty,
          SortedMap.empty,
          SortedSet.empty,
          None,
          EpochProgress.MinValue,
          NonEmptyList.one(PeerId(Hex(""))),
          SnapshotTips(SortedSet.empty, SortedSet.empty),
          stateProof,
          Some(SortedSet.empty),
          Some(SortedSet.empty),
          Option.when(spendActions.nonEmpty)(spendActions),
          Some(SortedMap.empty),
          Some(SortedSet.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty)
        ),
        NonEmptySet.one(SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("")), Signature(Hex(""))))
      ).toHashed[IO]
    }

  private def managerWithCache(
    cached: Map[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]
  ): IO[GlobalSnapshotOpsManager[IO]] =
    SignallingRef
      .of[IO, Map[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]](cached)
      .map(GlobalSnapshotOpsManager.make[IO](syncConfig, _))

  test("LINEAGE-RED-004 (temporary source boundary): a pinned replay cannot take the LastN same-ordinal fast path") { _ =>
    readSources.map { sources =>
      val lookup = sectionAfter(sources.acceptanceManager, "lastSyncGlobalSnapshot <-", 1000)

      expect.all(
        lookup.nonEmpty,
        !lookup.contains("lastGlobalSnapshots.find(_.ordinal === ordinalToFetchGlobalSnapshot)")
      )
    }
  }

  test("LINEAGE-RED-005A: cached SpendActions at the right ordinal but wrong hash cannot outrank the exact resolver") { res =>
    implicit val (hasher, json, securityProvider) = res

    for {
      ownerKey <- KeyPairGenerator.makeKeyPair[IO]
      producerKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      owner = PublicKeyOps(ownerKey.getPublic).toAddress
      producer = PublicKeyOps(producerKey.getPublic).toAddress
      destination = PublicKeyOps(destinationKey.getPublic).toAddress
      target = ord(41L)
      canonicalAction = SpendAction(
        NonEmptyList.one(SpendTransaction(None, Some(CurrencyId(owner)), SwapAmount(PosLong.unsafeFrom(1L)), producer, destination))
      )
      siblingAction = SpendAction(
        NonEmptyList.one(SpendTransaction(None, Some(CurrencyId(owner)), SwapAmount(PosLong.unsafeFrom(2L)), producer, destination))
      )
      canonical <- snapshotAt(target, SortedMap(producer -> List(canonicalAction)))
      sibling <- snapshotAt(target, SortedMap(producer -> List(siblingAction)))
      calls <- Ref.of[IO, Int](0)
      manager <- managerWithCache(Map.empty)
      result <- manager.getLastGlobalSnapshotsSpendActions(
        globalSnapshotViewOrdinal = target,
        lastGlobalSnapshots = List(sibling),
        getGlobalSnapshotByOrdinal = ordinal => calls.update(_ + 1).as(Option.when(ordinal === target)(canonical)),
        currencyId = owner,
        metagraphSyncData = Some(
          SortedMap(owner -> MetagraphSyncDataInfo(SnapshotOrdinal.MinValue, EpochProgress.MinValue, SortedSet(target)))
        ),
        alreadyProcessedGlobalOrdinals = SortedSet.empty
      )
      resolverCalls <- calls.get
      (actions, processed) = result
    } yield
      expect(canonical.hash =!= sibling.hash) &&
        expect.eql(1, resolverCalls) &&
        expect(processed == SortedSet(target)) &&
        expect.eql(List(canonicalAction), actions.getOrElse(producer, Nil))
  }

  test("LINEAGE-RED-005B: retry exhaustion cannot fall back to an ordinal-only manager cache") { res =>
    implicit val (hasher, json, _) = res
    val target = ord(52L)

    for {
      sibling <- snapshotAt(target, SortedMap.empty)
      observed <- TestControl.executeEmbed {
        for {
          calls <- Ref.of[IO, Int](0)
          manager <- managerWithCache(Map(target -> sibling))
          outcome <- manager
            .getGlobalSnapshotWithRetry(target, _ => calls.update(_ + 1).as(none[Hashed[GlobalIncrementalSnapshot]]))
            .attempt
          callCount <- calls.get
        } yield (outcome, callCount)
      }
      (outcome, callCount) = observed
    } yield expect(outcome.isLeft) && expect.eql(1, callCount)
  }

  test("LINEAGE-RED-006 (temporary source boundary): pinned message recreation cannot consume ambient-head GSI") { _ =>
    readSources.map { sources =>
      val implementation = sectionAfter(sources.acceptanceManager, "private class CurrencySnapshotAcceptanceManagerImpl", 30000)
      val messageCall = sectionAfter(implementation, "messageOps.acceptMessages(", 900)

      expect.all(
        messageCall.nonEmpty,
        !messageCall.contains("lastUnsyncBalances"),
        !messageCall.contains("lastUnsyncLastCurrencySnapshots")
      )
    }
  }

  test("LINEAGE-RED-007A (temporary source boundary): dual-trigger recreation shares one resolver instead of raw callback replay") { _ =>
    readSources.map { sources =>
      val creationCall = sectionAfter(sources.currencyValidator, ".createProposalArtifact(", 900)
      val passesRawResolver = creationCall.linesIterator.exists(_.trim == "getGlobalSnapshotByOrdinal,")

      expect.all(creationCall.nonEmpty, !passesRawResolver)
    }
  }

  test("LINEAGE-RED-007B (temporary source boundary): producer and receiver do not start one full ancestry walk per lookup") { _ =>
    readSources.map { sources =>
      val producerCallback = sectionAfter(sources.leaderLoop, "getGlobalSnapshotByOrdinal = ordinal =>", 1800)
      val receiverCallback = sectionAfter(sources.syncDaemon, "getByOrdinal = {", 1800)

      expect.all(
        producerCallback.nonEmpty,
        receiverCallback.nonEmpty,
        !producerCallback.contains(".getAncestorByOrdinal("),
        !receiverCallback.contains(".getAncestorByOrdinal(")
      )
    }
  }

  test("LINEAGE-RED-007C: missing SpendAction inputs get at most one resolver attempt per unique ordinal") { res =>
    implicit val (_, _, securityProvider) = res

    for {
      ownerKey <- KeyPairGenerator.makeKeyPair[IO]
      owner = PublicKeyOps(ownerKey.getPublic).toAddress
      targets = SortedSet(ord(61L), ord(62L), ord(63L))
      observed <- TestControl.executeEmbed {
        for {
          attempts <- Ref.of[IO, Map[SnapshotOrdinal, Int]](Map.empty)
          manager <- managerWithCache(Map.empty)
          outcome <- manager
            .getLastGlobalSnapshotsSpendActions(
              globalSnapshotViewOrdinal = targets.last,
              lastGlobalSnapshots = Nil,
              getGlobalSnapshotByOrdinal = ordinal =>
                attempts.update(current => current.updated(ordinal, current.getOrElse(ordinal, 0) + 1))
                  .as(none[Hashed[GlobalIncrementalSnapshot]]),
              currencyId = owner,
              metagraphSyncData = Some(
                SortedMap(owner -> MetagraphSyncDataInfo(SnapshotOrdinal.MinValue, EpochProgress.MinValue, targets))
              ),
              alreadyProcessedGlobalOrdinals = SortedSet.empty
            )
            .attempt
          counts <- attempts.get
        } yield (outcome, counts)
      }
      (outcome, counts) = observed
    } yield
      expect(outcome.isLeft) &&
        expect(counts.keySet == targets) &&
        expect.eql(targets.iterator.map(_ -> 1).toMap, counts)
  }

  test("LINEAGE-RED-007D (temporary source boundary): deterministic replay has no internal retry/log fan-out") { _ =>
    readSources.map { sources =>
      val lookup = sectionAfter(sources.globalSnapshotOps, "def getGlobalSnapshotWithRetry(", 1800)

      expect.all(
        lookup.nonEmpty,
        !lookup.contains("retryingOnFailuresAndAllErrors"),
        !lookup.contains("retriesSoFar")
      )
    }
  }

  private final case class Sources(
    acceptanceManager: String,
    globalSnapshotOps: String,
    currencyValidator: String,
    leaderLoop: String,
    syncDaemon: String
  )

  private def readSources: IO[Sources] = IO.blocking {
    val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
    def read(relative: String): String =
      new String(Files.readAllBytes(root.resolve(relative)), StandardCharsets.UTF_8)

    val currencyBase =
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/"
    val nakamotoBase =
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/"

    Sources(
      read(currencyBase + "CurrencySnapshotAcceptanceManager.scala"),
      read(currencyBase + "GlobalSnapshotOpsManager.scala"),
      read(
        "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotValidator.scala"
      ),
      read(nakamotoBase + "SnapshotLeaderLoop.scala"),
      read(nakamotoBase + "NakamotoSyncDaemon.scala")
    )
  }

  private def sectionAfter(source: String, anchor: String, length: Int): String = {
    val index = source.indexOf(anchor)
    if (index < 0) "" else source.substring(index, math.min(source.length, index + length))
  }

  private def repositoryRoot(start: Path): Path = {
    @annotation.tailrec
    def loop(current: Path): Path =
      if (Files.exists(current.resolve("build.sbt"))) current
      else if (current.getParent == null) throw new IllegalStateException(s"Unable to locate repository root from $start")
      else loop(current.getParent)

    loop(start)
  }
}
