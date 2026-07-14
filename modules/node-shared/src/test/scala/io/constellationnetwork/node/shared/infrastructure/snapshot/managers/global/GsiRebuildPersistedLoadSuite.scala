package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.{FileSystemMerklePatriciaProducer, InMemoryMerklePatriciaProducer}

import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.Files
import weaver.MutableIOSuite

/** FINDING-S01 completion (EPIC-9-SERDE 9.1) — byte-faithful boot/download seed from the node's OWN persisted MPT.
  *
  * The MPT-native consensus partitions (`ConsumedAllowSpends` fieldId 33 / `Slashings` fieldId 34) are in the signed consensus root but
  * have NO `GlobalSnapshotInfo` field, so a from-GSI rebuild of an EMPTY boot-time store structurally cannot reproduce the signed
  * `stateProof.mptRoot` once 33/34 are non-empty (`numShards > 1`) — the gl0 fail-closed boot gates would force a re-bootstrap. The
  * persisted MPT byte map, however, carries 33/34 verbatim (the producer persists its FULL entry map), so
  * `syncFromPersistedMptVerified(ordinal, signedRoot)` reproduces the signed root BY CONSTRUCTION on an uncorrupted store and lets a
  * restarting node boot without a needless re-bootstrap.
  *
  * Under test: `MptStore.loadPersisted` (trait surface over the persistence producer's `load`) and the root-verified wrapper
  * `syncFromPersistedMptVerified` (adopt-on-match, savepoint-restore on mismatch, `false` on no-persistence / nothing-persisted / legacy
  * `signedMptRoot = None`).
  */
object GsiRebuildPersistedLoadSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // Same implicit pattern as GsiRebuildSpentSetSurvivalSuite — MPT stateProof format for all ordinals under test.
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  private val ord2: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(2L))
  private val ord3: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(3L))
  private val ord9: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(9L))

  private def bal(v: Long): Balance = Balance(NonNegLong.unsafeFrom(v))

  private case class Fixture(
    gsi: GlobalSnapshotInfo,
    store: MptStore[IO, GlobalStateKey],
    producer: FileSystemMerklePatriciaProducer[IO],
    consumedHex: Hex,
    slashHex: Hex,
    markerBytes: Array[Byte],
    slashBytes: Array[Byte]
  )

  /** A persistence-backed store seeded from a GSI, with fieldId-33/34 markers written on top (as the gl0 fold does at `numShards > 1`),
    * built at `ord2` and EXPLICITLY persisted (deterministic — no background `persistAsync` race).
    */
  private def mkPersistedFixture(
    dir: fs2.io.file.Path
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]): IO[(Fixture, Hash, Map[Hex, Array[Byte]])] =
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      source = kp.getPublic.toAddress
      gsi = GlobalSnapshotInfo.empty.copy(balances = SortedMap(source -> bal(1000L)))

      producer <- FileSystemMerklePatriciaProducer.make[IO](dir)
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- store.syncFromGlobalSnapshotInfo(gsi, ord2)

      consumedHex <- GlobalStateKey.toHex[IO](GlobalStateKey.consumedAllowSpendKey(Hash("ab" * 32)))
      slashKey <- GlobalStateKey.slashingsKey[IO](PeerId(Hex("ab" * 64)), ShardId.unsafeApply(0), Hash("cd" * 32))
      slashHex <- GlobalStateKey.toHex[IO](slashKey)
      markerBytes = Array[Byte](1, 2, 3, 4)
      slashBytes = Array[Byte](9, 9, 9)
      _ <- store.underlying.insertBytes(Map(consumedHex -> markerBytes, slashHex -> slashBytes)).flatMap(_.liftTo[IO])
      _ <- store.build(ord2).void
      // Deterministic disk write — production paths persist via the fire-and-forget `persistAsync`; tests must not race it.
      _ <- producer.persist(ord2)

      entriesBefore <- store.allEntriesAsBytes
      signedRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](entriesBefore)
    } yield (Fixture(gsi, store, producer, consumedHex, slashHex, markerBytes, slashBytes), signedRoot, entriesBefore)

  test(
    "restart with OWN persisted MPT: syncFromPersistedMptVerified reproduces the SIGNED root byte-faithfully — 33/34 included, no GSI transit, no re-bootstrap"
  ) { res =>
    implicit val (h, sp, js) = res
    Files[IO].tempDirectory.use { dir =>
      for {
        (f, signedRoot, entriesBefore) <- mkPersistedFixture(dir)

        // ===== RESTART: the in-memory store is empty at boot (SharedStorages seeds an empty producer); disk retains the bytes.
        _ <- f.store.clear
        emptyAtBoot <- f.store.isEmpty

        // Sanity: the from-GSI candidate CANNOT reproduce the signed root here (33/34 are non-empty and the boot store is
        // empty, so there is nothing to preserve) — pre-fix this was exactly the forced re-bootstrap / divergent-base case.
        gsiAdopted <- f.store.syncFromGlobalSnapshotInfoVerified(f.gsi, ord2, signedRoot.some)

        adopted <- f.store.syncFromPersistedMptVerified(ord2, signedRoot.some)
        entriesAfter <- f.store.allEntriesAsBytes
        rootAfter <- GlobalSnapshotInfo.consensusMptRoot[IO](entriesAfter)
      } yield
        expect.all(
          emptyAtBoot,
          !gsiAdopted,
          adopted,
          // the wedge-closure: the reloaded store's consensus root EQUALS the signed root
          rootAfter === signedRoot,
          // 33/34 survived the restart VERBATIM
          entriesAfter.get(f.consumedHex).exists(_.sameElements(f.markerBytes)),
          entriesAfter.get(f.slashHex).exists(_.sameElements(f.slashBytes)),
          // full byte-identity with the pre-restart store (byte-faithful, no re-encode)
          entriesAfter.keySet === entriesBefore.keySet,
          entriesAfter.forall { case (k, v) => entriesBefore.get(k).exists(_.sameElements(v)) }
        )
    }
  }

  test("root mismatch (stale/corrupt persisted bytes): syncFromPersistedMptVerified fails CLOSED and restores the pre-call store state") {
    res =>
      implicit val (h, sp, js) = res
      Files[IO].tempDirectory.use { dir =>
        for {
          (f, _, _) <- mkPersistedFixture(dir)

          // The live store has since moved on (a DIFFERENT state at ord3) — the persisted ord2 bytes are stale vs this target.
          gsi3 = GlobalSnapshotInfo.empty
          _ <- f.store.syncFromGlobalSnapshotInfo(gsi3, ord3)
          preCallEntries <- f.store.allEntriesAsBytes
          preCallRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](preCallEntries)

          bogus = Hash("ff" * 32)
          adopted <- f.store.syncFromPersistedMptVerified(ord2, bogus.some)
          entriesAfter <- f.store.allEntriesAsBytes
          rootAfter <- GlobalSnapshotInfo.consensusMptRoot[IO](entriesAfter)
        } yield
          expect.all(
            !adopted,
            // savepoint-restored: byte-identical to the pre-call state (the destructive `producer.load` was rolled back)
            rootAfter === preCallRoot,
            entriesAfter.keySet === preCallEntries.keySet,
            entriesAfter.forall { case (k, v) => preCallEntries.get(k).exists(_.sameElements(v)) }
          )
      }
  }

  test("nothing persisted at the ordinal / legacy signedMptRoot=None / non-persistence producer: all return false, store untouched") {
    res =>
      implicit val (h, sp, js) = res
      Files[IO].tempDirectory.use { dir =>
        for {
          (f, signedRoot, _) <- mkPersistedFixture(dir)
          preCallEntries <- f.store.allEntriesAsBytes

          // (a) nothing persisted at ord9
          missingAdopted <- f.store.syncFromPersistedMptVerified(ord9, signedRoot.some)
          // (b) legacy pre-MPT snapshot — nothing sound to verify against
          noneAdopted <- f.store.syncFromPersistedMptVerified(ord2, none[Hash])
          entriesAfter <- f.store.allEntriesAsBytes

          // (c) a producer WITHOUT a persistence backend — `loadPersisted` is a no-op `false`
          memProducer <- InMemoryMerklePatriciaProducer.make[IO]()
          memStore <- MptStore.make[IO, GlobalStateKey](memProducer, GlobalStateKey.toHex[IO])
          memAdopted <- memStore.syncFromPersistedMptVerified(ord2, signedRoot.some)
        } yield
          expect.all(
            !missingAdopted,
            !noneAdopted,
            !memAdopted,
            entriesAfter.keySet === preCallEntries.keySet,
            entriesAfter.forall { case (k, v) => preCallEntries.get(k).exists(_.sameElements(v)) }
          )
      }
  }
}
