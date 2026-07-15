package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect._
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer._
import io.constellationnetwork.security.mpt.prover.MerklePatriciaSingleInclusionProver
import io.constellationnetwork.security.mpt.{MerklePatriciaTrie, MptRoot}
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec._

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.{Encoder, Json}
import weaver.MutableIOSuite

/** §3 NIPoPoW S3 — [[MptTowerStore]] suite. Mirrors [[TowerStoreSuite]] semantics against the MPT-backed impl, asserting that the
  * `TowerStore[F]` contract holds independently of the storage backend: monotone ordinal append, ordered per-level reads, latest-at-level,
  * cumulative count, and idempotent prune.
  */
object MptTowerStoreSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  /** Build a synthetic `Hash` from a short label: emit the label's bytes as hex then right-pad to 64 chars. Ensures the hex is parseable by
    * [[io.constellationnetwork.serde.codecs.instances.HashCodec]] (lowercase hex only) regardless of the label's text content.
    */
  private def h(s: String): Hash =
    Hash(s.getBytes("UTF-8").map(b => f"${b & 0xff}%02x").mkString.padTo(64, '0').take(64))

  private def trial(level: Int, passed: Boolean): LevelTrial =
    LevelTrial(level, Ratio.Zero, Ratio.Zero, passed)

  private def trials(levels: Set[Int]): Vector[LevelTrial] =
    (1 to SuperLevelParams.SuperLevelCount).toVector.map(l => trial(l, levels.contains(l)))

  private final case class Fixture(tower: TowerStore[IO], producer: InMemoryMerklePatriciaProducer[IO])

  private final case class RawFixture(
    store: MptStore[IO, TowerEntryKey],
    producer: InMemoryMerklePatriciaProducer[IO]
  )

  private def hashBytes(hash: Hash): Array[Byte] =
    ImmutableCodec[Hash].immutableBytes(hash).toArray

  private def rawFixture(res: Res, entries: Map[Hex, Array[Byte]]): IO[RawFixture] = {
    implicit val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]) = res
    val _ = (sp, js)
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO](entries)
      mpt <- MptStore.make[IO, TowerEntryKey](producer, TowerEntryKey.toHexF[IO])
    } yield RawFixture(mpt, producer)
  }

  private def fixture(res: Res, entries: List[(Hex, Hash)] = Nil): IO[Fixture] = {
    val encoded = entries.map { case (key, hash) => key -> hashBytes(hash) }.toMap
    for {
      raw <- rawFixture(res, encoded)
      tower <- MptTowerStore.make[IO](raw.store)
    } yield Fixture(tower, raw.producer)
  }

  private final class InstrumentedProducer(
    delegate: StatefulMerklePatriciaProducer[IO],
    physicalKeyReads: Ref[IO, Int],
    failBuildForOrdinal: Boolean = false,
    failAfterRemove: Boolean = false,
    successfulInserts: Option[Ref[IO, Int]] = None,
    successfulRemoves: Option[Ref[IO, Int]] = None,
    afterSuccessfulInsert: IO[Unit] = IO.unit,
    afterSuccessfulRemove: IO[Unit] = IO.unit,
    imageOverride: Option[Map[Hex, Array[Byte]]] = None
  ) extends StatefulMerklePatriciaProducer[IO] {

    def entries: IO[Map[Hex, Array[Byte]]] = imageOverride.fold(delegate.entries)(IO.pure)
    def physicalKeys: IO[Set[Hex]] =
      physicalKeyReads.update(_ + 1) >> imageOverride.fold(delegate.physicalKeys)(entries => IO.pure(entries.keySet))
    def entry(key: Hex): IO[Option[Array[Byte]]] = delegate.entry(key)
    def entriesForKeys(keys: Set[Hex]): IO[Map[Hex, Array[Byte]]] =
      imageOverride.fold(delegate.entriesForKeys(keys))(entries => IO.pure(entries.view.filterKeys(keys.contains).toMap))
    def entryCount: IO[Int] = delegate.entryCount
    def entriesWithPrefix(prefix: Hex): IO[Map[Hex, Array[Byte]]] = delegate.entriesWithPrefix(prefix)
    def build: IO[Either[MerklePatriciaError, MerklePatriciaTrie]] = delegate.build
    def buildForOrdinal(ordinal: SnapshotOrdinal): IO[Either[MerklePatriciaError, MerklePatriciaTrie]] =
      if (failBuildForOrdinal) IO.pure(Left(OperationError("injected tower build failure")))
      else delegate.buildForOrdinal(ordinal)
    def getRootHashForOrdinal(ordinal: SnapshotOrdinal): IO[Option[MptRoot]] = delegate.getRootHashForOrdinal(ordinal)
    def getCurrentRootHash: IO[Option[MptRoot]] = delegate.getCurrentRootHash
    def getLastBuiltOrdinal: IO[Option[SnapshotOrdinal]] = delegate.getLastBuiltOrdinal
    def insert[A: Encoder](data: Map[Hex, A]): IO[Either[MerklePatriciaError, Unit]] = delegate.insert(data)
    def insertBytes(data: Map[Hex, Array[Byte]]): IO[Either[MerklePatriciaError, Unit]] =
      delegate.insertBytes(data).flatTap {
        case Right(_) => successfulInserts.traverse_(_.update(_ + 1)) >> afterSuccessfulInsert
        case Left(_)  => IO.unit
      }
    def replaceBytes(upserts: Map[Hex, Array[Byte]], removals: List[Hex]): IO[Either[MerklePatriciaError, Unit]] =
      delegate.replaceBytes(upserts, removals)
    def update[A: Encoder](key: Hex, value: A): IO[Either[MerklePatriciaError, Unit]] = delegate.update(key, value)
    def remove(keys: List[Hex]): IO[Either[MerklePatriciaError, Unit]] =
      delegate.remove(keys).flatMap {
        case Right(_) =>
          successfulRemoves.traverse_(_.update(_ + 1)) >> afterSuccessfulRemove >>
            (if (failAfterRemove) IO.pure(Left(OperationError("injected post-remove tower failure")))
             else IO.pure(Right(())))
        case left @ Left(_) => IO.pure(left)
      }
    def clear: IO[Unit] = delegate.clear
    def getProver: IO[MerklePatriciaSingleInclusionProver[IO]] = delegate.getProver
    def buildHexMap(data: Map[GlobalStateKey, Json]): IO[Map[Hex, Array[Byte]]] = delegate.buildHexMap(data)
    def savepoint: IO[ProducerSavepoint[IO]] = delegate.savepoint
  }

  // Each test allocates its own MPT store — the MPT producer is stateful and tests must not share state.
  private def fresh(res: Res): IO[TowerStore[IO]] = {
    implicit val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]) = res
    val _ = (sp, js) // sp/js are required by the Resource setup but unused at call site
    MptTowerStore.inMemory[IO]
  }

  test("empty store — entriesAtLevel returns Nil; latestAt returns None; cumulativeCount = 0") { res =>
    for {
      store <- fresh(res)
      e <- store.entriesAtLevel(1, ord(0))
      l <- store.latestAt(1)
      c <- store.cumulativeCount(1)
    } yield expect(e == Nil).and(expect(l.isEmpty)).and(expect(c == 0L))
  }

  test("appendAtFinality — passes at L1+L3 store both; non-passes do not") { res =>
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(5), h("aa"), trials(Set(1, 3)))
      l1 <- store.entriesAtLevel(1, ord(0))
      l2 <- store.entriesAtLevel(2, ord(0))
      l3 <- store.entriesAtLevel(3, ord(0))
    } yield
      expect(l1 == List(TowerEntry(1, ord(5), h("aa")))).and(expect(l2 == Nil)).and(expect(l3 == List(TowerEntry(3, ord(5), h("aa")))))
  }

  test("latestAt — returns most-recent entry per level") { res =>
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(10), h("a1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(20), h("a2"), trials(Set(1, 2)))
      _ <- store.appendAtFinality(ord(30), h("a3"), trials(Set(2)))
      latestL1 <- store.latestAt(1)
      latestL2 <- store.latestAt(2)
      latestL3 <- store.latestAt(3)
    } yield
      expect(latestL1 == Some(TowerEntry(1, ord(20), h("a2"))))
        .and(expect(latestL2 == Some(TowerEntry(2, ord(30), h("a3")))))
        .and(expect(latestL3.isEmpty))
  }

  test("cumulativeCount — counts per-level passes only") { res =>
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(100), h("p1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(101), h("p2"), trials(Set(1, 2)))
      _ <- store.appendAtFinality(ord(102), h("p3"), trials(Set.empty))
      c1 <- store.cumulativeCount(1)
      c2 <- store.cumulativeCount(2)
      c3 <- store.cumulativeCount(3)
    } yield expect(c1 == 2L).and(expect(c2 == 1L)).and(expect(c3 == 0L))
  }

  test("entriesAtLevel — since filter is inclusive lower bound") { res =>
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(50), h("e1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(60), h("e2"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(70), h("e3"), trials(Set(1)))
      fromMid <- store.entriesAtLevel(1, ord(60))
      fromAfter <- store.entriesAtLevel(1, ord(61))
    } yield
      expect(fromMid == List(TowerEntry(1, ord(60), h("e2")), TowerEntry(1, ord(70), h("e3"))))
        .and(expect(fromAfter == List(TowerEntry(1, ord(70), h("e3")))))
  }

  test("pruneBelow — drops entries strictly below keepFrom; idempotent") { res =>
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(10), h("z1"), trials(Set(1, 2)))
      _ <- store.appendAtFinality(ord(20), h("z2"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(30), h("z3"), trials(Set(2)))
      _ <- store.pruneBelow(ord(20))
      l1 <- store.entriesAtLevel(1, ord(0))
      l2 <- store.entriesAtLevel(2, ord(0))
      _ <- store.pruneBelow(ord(20))
      l1Again <- store.entriesAtLevel(1, ord(0))
    } yield
      expect(l1 == List(TowerEntry(1, ord(20), h("z2"))))
        .and(expect(l2 == List(TowerEntry(2, ord(30), h("z3")))))
        .and(expect(l1Again == l1))
  }

  test("monotonic ordinal append preserves per-level ordering in the validated index") { res =>
    val ordinals = List(1L, 3L, 5L, 9L, 13L)
    for {
      store <- fresh(res)
      _ <- ordinals.traverse_(o => store.appendAtFinality(ord(o), h(s"o$o"), trials(Set(1))))
      l1 <- store.entriesAtLevel(1, ord(0))
    } yield expect(l1.map(_.ordinal.value.value) == ordinals)
  }

  test("interleaved-ordinal append at different levels — per-level ordering preserved independently") { res =>
    // L1 and L2 hit at different ordinals; verify each level's ordering is independent of the other's writes.
    for {
      store <- fresh(res)
      _ <- store.appendAtFinality(ord(100), h("x1"), trials(Set(1)))
      _ <- store.appendAtFinality(ord(50), h("x2"), trials(Set(2))) // out-of-order vs L1
      _ <- store.appendAtFinality(ord(200), h("x3"), trials(Set(1, 2)))
      _ <- store.appendAtFinality(ord(75), h("x4"), trials(Set(2))) // out-of-order vs L2's first entry
      l1 <- store.entriesAtLevel(1, ord(0))
      l2 <- store.entriesAtLevel(2, ord(0))
    } yield
      expect(l1.map(_.ordinal.value.value) == List(100L, 200L))
        .and(expect(l2.map(_.ordinal.value.value) == List(50L, 75L, 200L)))
  }

  test("TowerEntryKey hex layout — fixed-width 24 chars (8 level + 16 ordinal), lex-sortable") {
    val k1 = TowerEntryKey(1, ord(0))
    val k2 = TowerEntryKey(1, ord(1))
    val k3 = TowerEntryKey(1, ord(0xffffffffL))
    val k4 = TowerEntryKey(2, ord(0))
    for {
      h1 <- TowerEntryKey.toHexF[IO](k1)
      h2 <- TowerEntryKey.toHexF[IO](k2)
      h3 <- TowerEntryKey.toHexF[IO](k3)
      h4 <- TowerEntryKey.toHexF[IO](k4)
    } yield
      expect(h1.value.length == 24)
        .and(expect(h1.value < h2.value)) // ordinal 0 < ordinal 1 within same level
        .and(expect(h2.value < h3.value))
        .and(expect(h3.value < h4.value)) // last L1 ordinal < first L2 ordinal
        .and(expect(TowerEntryKey.levelPrefix(1).value == "00000001"))
  }

  test("TowerEntryKey decoder rejects truncated, oversized, trailing, noncanonical, malformed, and out-of-range keys") {
    val valid = Hex("000000010000000000abcdef")
    val invalid = List(
      null.asInstanceOf[Hex],
      Hex(valid.value.dropRight(2)),
      Hex(valid.value + "00"),
      Hex(valid.value.dropRight(1)),
      Hex(valid.value.toUpperCase),
      Hex(valid.value.updated(10, 'g')),
      Hex("000000000000000000000001"),
      Hex("0000000a0000000000000001"),
      Hex("000000018000000000000000")
    )

    IO.pure(
      expect(TowerEntryKey.decode(valid) == Right(TowerEntryKey(1, ord(0xabcdefL))))
        .and(expect(invalid.forall(TowerEntryKey.decode(_).isLeft)))
    )
  }

  test("TowerEntryKey complete decoder rejects duplicate logical identities before accepting a canonical subset") {
    val canonical = Hex("000000010000000000abcdef")
    val alias = Hex(canonical.value.toUpperCase)

    IO.pure(expect(TowerEntryKey.decodeAll(List(canonical, alias)).left.exists(_.isInstanceOf[DuplicateDurableNipopowIdentity])))
  }

  test("construction rejects a malformed key anywhere in the durable image instead of exposing reduced history") { res =>
    val valid = Hex("000000010000000000000001")
    val malformedOtherLevel = Hex("0000000200000000000000")
    for {
      raw <- rawFixture(
        res,
        Map(valid -> hashBytes(h("valid")), malformedOtherLevel -> hashBytes(h("bad")))
      )
      result <- MptTowerStore.make[IO](raw.store).attempt
    } yield expect(result.left.exists(_.isInstanceOf[MalformedDurableNipopowKey]))
  }

  test("construction rejects a malformed value before exposing cached latest/count reads") { res =>
    val valid = Hex("000000010000000000000001")
    for {
      raw <- rawFixture(res, Map(valid -> Array[Byte](0x01, 0x02)))
      result <- MptTowerStore.make[IO](raw.store).attempt
    } yield expect(result.left.exists(_.isInstanceOf[MalformedDurableNipopowValue]))
  }

  test("construction rejects duplicate logical identities before exposing the store") { res =>
    implicit val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]) = res
    val _ = (sp, js)
    val canonical = Hex("000000010000000000abcdef")
    val alias = Hex(canonical.value.toUpperCase)
    for {
      underlying <- InMemoryMerklePatriciaProducer.make[IO]()
      reads <- Ref.of[IO, Int](0)
      duplicateImage = Map(canonical -> hashBytes(h("canonical")), alias -> hashBytes(h("alias")))
      producer = new InstrumentedProducer(underlying, reads, imageOverride = Some(duplicateImage))
      mpt <- MptStore.make[IO, TowerEntryKey](producer, TowerEntryKey.toHexF[IO])
      result <- MptTowerStore.make[IO](mpt).attempt
    } yield expect(clue(result).left.exists(_.isInstanceOf[DuplicateDurableNipopowIdentity]))
  }

  test("construction alone performs tower-wide physical enumeration; hot paths use the private index") { res =>
    implicit val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]) = res
    val _ = (sp, js)
    val initialKey = Hex("000000010000000000000001")
    for {
      underlying <- InMemoryMerklePatriciaProducer.make[IO](Map(initialKey -> hashBytes(h("initial"))))
      physicalKeyReads <- Ref.of[IO, Int](0)
      counting = new InstrumentedProducer(underlying, physicalKeyReads)
      mpt <- MptStore.make[IO, TowerEntryKey](counting, TowerEntryKey.toHexF[IO])
      tower <- MptTowerStore.make[IO](mpt)
      readsAfterConstruction <- physicalKeyReads.get
      _ <- (1 to SuperLevelParams.SuperLevelCount).toList.traverse_ { level =>
        tower.latestAt(level) >> tower.cumulativeCount(level) >> tower.entriesAtLevel(level, ord(0))
      }
      _ <- tower.appendAtFinality(ord(2), h("next"), trials(Set(1, 3)))
      _ <- tower.pruneBelow(ord(2))
      readsAfterHotPath <- physicalKeyReads.get
      latest <- tower.latestAt(1)
      count <- tower.cumulativeCount(1)
    } yield
      expect(readsAfterConstruction == 1)
        .and(expect(readsAfterHotPath == 1))
        .and(expect(latest.contains(TowerEntry(1, ord(2), h("next")))))
        .and(expect(count == 1L))
  }

  test("post-mutation append and prune failures roll back both the durable image and private index") { res =>
    implicit val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]) = res
    val _ = (sp, js)
    val firstKey = Hex("000000010000000000000001")
    val secondKey = Hex("000000010000000000000002")
    val initial = Map(firstKey -> hashBytes(h("first")), secondKey -> hashBytes(h("second")))
    for {
      appendUnderlying <- InMemoryMerklePatriciaProducer.make[IO](initial)
      appendReads <- Ref.of[IO, Int](0)
      successfulInserts <- Ref.of[IO, Int](0)
      appendProducer = new InstrumentedProducer(
        appendUnderlying,
        appendReads,
        failBuildForOrdinal = true,
        successfulInserts = Some(successfulInserts)
      )
      appendMpt <- MptStore.make[IO, TowerEntryKey](appendProducer, TowerEntryKey.toHexF[IO])
      appendTower <- MptTowerStore.make[IO](appendMpt)
      appendResult <- appendTower.appendAtFinality(ord(3), h("third"), trials(Set(1))).attempt
      appendMutationCount <- successfulInserts.get
      appendEntries <- appendTower.entriesAtLevel(1, ord(0))
      appendPhysical <- appendUnderlying.physicalKeys
      appendPersistedOrdinal <- appendMpt.lastPersistedOrdinal
      pruneUnderlying <- InMemoryMerklePatriciaProducer.make[IO](initial)
      pruneReads <- Ref.of[IO, Int](0)
      successfulRemoves <- Ref.of[IO, Int](0)
      pruneProducer = new InstrumentedProducer(
        pruneUnderlying,
        pruneReads,
        failAfterRemove = true,
        successfulRemoves = Some(successfulRemoves)
      )
      pruneMpt <- MptStore.make[IO, TowerEntryKey](pruneProducer, TowerEntryKey.toHexF[IO])
      pruneTower <- MptTowerStore.make[IO](pruneMpt)
      pruneResult <- pruneTower.pruneBelow(ord(2)).attempt
      pruneMutationCount <- successfulRemoves.get
      pruneEntries <- pruneTower.entriesAtLevel(1, ord(0))
      prunePhysical <- pruneUnderlying.physicalKeys
    } yield
      expect(appendResult.isLeft)
        .and(expect(appendMutationCount == 1))
        .and(expect(appendEntries.map(_.ordinal) == List(ord(1), ord(2))))
        .and(expect(appendPhysical == initial.keySet))
        .and(expect(appendPersistedOrdinal.isEmpty))
        .and(expect(pruneResult.isLeft))
        .and(expect(pruneMutationCount == 1))
        .and(expect(pruneEntries.map(_.ordinal) == List(ord(1), ord(2))))
        .and(expect(prunePhysical == initial.keySet))
  }

  test("post-mutation append and prune cancellation restore producer/index state and release the mutex") { res =>
    implicit val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]) = res
    val _ = (sp, js)
    val firstKey = Hex("000000010000000000000001")
    val secondKey = Hex("000000010000000000000002")
    val initial = Map(firstKey -> hashBytes(h("first")), secondKey -> hashBytes(h("second")))
    for {
      appendUnderlying <- InMemoryMerklePatriciaProducer.make[IO](initial)
      appendReads <- Ref.of[IO, Int](0)
      appendMutated <- Deferred[IO, Unit]
      appendRelease <- Deferred[IO, Unit]
      appendProducer = new InstrumentedProducer(
        appendUnderlying,
        appendReads,
        afterSuccessfulInsert = appendMutated.complete(()).void >> appendRelease.get
      )
      appendMpt <- MptStore.make[IO, TowerEntryKey](appendProducer, TowerEntryKey.toHexF[IO])
      appendTower <- MptTowerStore.make[IO](appendMpt)
      appendFiber <- appendTower.appendAtFinality(ord(3), h("third"), trials(Set(1))).start
      _ <- appendMutated.get.timeout(2.seconds)
      _ <- appendFiber.cancel.timeout(2.seconds)
      appendEntries <- appendTower.entriesAtLevel(1, ord(0)).timeout(2.seconds)
      appendPhysical <- appendUnderlying.physicalKeys
      appendPersistedOrdinal <- appendMpt.lastPersistedOrdinal
      _ <- appendRelease.complete(()).void
      _ <- appendTower.appendAtFinality(ord(3), h("third"), trials(Set(1))).timeout(2.seconds)
      appendRetryEntries <- appendTower.entriesAtLevel(1, ord(0))
      pruneUnderlying <- InMemoryMerklePatriciaProducer.make[IO](initial)
      pruneReads <- Ref.of[IO, Int](0)
      pruneMutated <- Deferred[IO, Unit]
      pruneRelease <- Deferred[IO, Unit]
      pruneProducer = new InstrumentedProducer(
        pruneUnderlying,
        pruneReads,
        afterSuccessfulRemove = pruneMutated.complete(()).void >> pruneRelease.get
      )
      pruneMpt <- MptStore.make[IO, TowerEntryKey](pruneProducer, TowerEntryKey.toHexF[IO])
      pruneTower <- MptTowerStore.make[IO](pruneMpt)
      pruneFiber <- pruneTower.pruneBelow(ord(2)).start
      _ <- pruneMutated.get.timeout(2.seconds)
      _ <- pruneFiber.cancel.timeout(2.seconds)
      pruneEntries <- pruneTower.entriesAtLevel(1, ord(0)).timeout(2.seconds)
      prunePhysical <- pruneUnderlying.physicalKeys
    } yield
      expect(appendEntries.map(_.ordinal) == List(ord(1), ord(2)))
        .and(expect(appendPhysical == initial.keySet))
        .and(expect(appendPersistedOrdinal.isEmpty))
        .and(expect(appendRetryEntries.map(_.ordinal) == List(ord(1), ord(2), ord(3))))
        .and(expect(pruneEntries.map(_.ordinal) == List(ord(1), ord(2))))
        .and(expect(prunePhysical == initial.keySet))
  }

  test("tower append rejects duplicate trial levels before mutating the durable image") { res =>
    val duplicate = Vector(trial(1, passed = true), trial(1, passed = true))
    for {
      f <- fixture(res)
      result <- f.tower.appendAtFinality(ord(1), h("duplicate"), duplicate).attempt
      after <- f.producer.physicalKeys
    } yield
      expect(result.left.exists(_.isInstanceOf[DuplicateTowerTrialLevel]))
        .and(expect(after.isEmpty))
  }

}
