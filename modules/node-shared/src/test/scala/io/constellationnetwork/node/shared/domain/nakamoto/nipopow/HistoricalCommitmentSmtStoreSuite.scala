package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect._
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer._
import io.constellationnetwork.security.mpt.prover.MerklePatriciaSingleInclusionProver
import io.constellationnetwork.security.mpt.{MerklePatriciaTrie, MptRoot}
import io.constellationnetwork.security.smt._
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec._

import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.Files
import io.circe.{Encoder, Json}
import weaver.MutableIOSuite

/** §3 NIPoPoW — [[HistoricalCommitmentSmtStore]] component suite. Proves deterministic derived-SMT properties needed before `smtRoot` can
  * become load-bearing; exact-hash reorg recovery, authenticated production wiring, and follower enforcement remain separate gates:
  *   - producer == simulated-follower `smtRoot(N)` for the SAME chain of finalized commitments,
  *   - `smtRoot(N)` is reproducible purely from commitments `≤ N−k`,
  *   - an inclusion proof of a PAST ordinal's `PerOrdinalCommitment` verifies against the committed `smtRoot(N)`,
  *   - genesis / warmup (`N < k`) is deterministic and the first root at `N = k` includes eligible ordinal zero,
  *   - CIRCULARITY-FREE: ordinal N's OWN commitment (carrying snapshot N's incremental hash) is NOT in `smtRoot(N)`.
  *
  * The store is a pure ordinal-keyed structure: `appendAtFinality(snapshotOrdinal = N, eligibleOrdinal = N−k, commitment)`. These tests
  * drive it directly (the GSAM wiring that derives the commitment from on-disk snapshots is exercised separately); the determinism the
  * tests assert is exactly what makes producer and follower agree, since both feed the SAME `(N, N−k, commitment)` sequence.
  */
object HistoricalCommitmentSmtStoreSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val K: Long = 5L

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  /** A synthetic `Hash` from a label: hex of the label bytes, right-padded to 64 lowercase-hex chars (parseable by HashCodec). */
  private def h(s: String): Hash =
    Hash(s.getBytes("UTF-8").map(b => f"${b & 0xff}%02x").mkString.padTo(64, '0').take(64))

  /** The deterministic commitment for a finalized ordinal `i`: its mptRoot + its (distinct) incremental snapshot hash. towerEligibility is
    * the reserved `NotComputed` (the steady-state core value — see PerOrdinalCommitment / report STAGING).
    */
  private def commitmentFor(i: Long): PerOrdinalCommitment =
    PerOrdinalCommitment(
      hypergraphRoot = h(s"mptRoot-$i"),
      incrementalSnapshotHash = h(s"incrementalHash-$i"),
      towerEligibility = TowerEligibility.NotComputed
    )

  private final case class Fixture(
    store: HistoricalCommitmentSmtStore[IO],
    producer: InMemoryMerklePatriciaProducer[IO],
    durable: MptStore[IO, CommitmentKey]
  )

  private final class TestAppendHook(afterInsert: IO[Unit], beforeCommit: IO[Unit]) extends HistoricalCommitmentAppendHook[IO] {
    def afterDurableInsert: IO[Unit] = afterInsert
    def beforeDurableCommit: IO[Unit] = beforeCommit
  }

  private final class BeforeHasher(delegate: Hasher[IO], before: IO[Unit]) extends Hasher[IO] {
    def hash[A: Encoder](data: A): IO[Hash] = before >> delegate.hash(data)
    def hashBytes(bytes: Array[Byte]): IO[Hash] = before >> delegate.hashBytes(bytes)
    def compare[A: Encoder](data: A, expectedHash: Hash): IO[Boolean] = before >> delegate.compare(data, expectedHash)
    def getLogic(ordinal: SnapshotOrdinal) = delegate.getLogic(ordinal)
    def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): IO[Hash] = before >> delegate.prefixedHash(data, prefix)
  }

  private final class PausedSnapshotProducer(
    delegate: StatefulMerklePatriciaProducer[IO],
    snapshotTaken: Deferred[IO, Unit],
    releaseSnapshot: Deferred[IO, Unit]
  ) extends StatefulMerklePatriciaProducer[IO] {

    override def physicalKeyPolicy: PhysicalTrieKeyPolicy = delegate.physicalKeyPolicy

    def entries: IO[Map[Hex, Array[Byte]]] =
      delegate.entries.flatMap(image => snapshotTaken.complete(()).void >> releaseSnapshot.get.as(image))

    def physicalKeys: IO[Set[Hex]] =
      IO.raiseError(new AssertionError("recovery must not enumerate physical keys separately from values"))

    def entriesForKeys(keys: Set[Hex]): IO[Map[Hex, Array[Byte]]] =
      IO.raiseError(new AssertionError(s"recovery must not perform a second value read for ${keys.size} captured keys"))

    def entry(key: Hex): IO[Option[Array[Byte]]] = delegate.entry(key)
    def entryCount: IO[Int] = delegate.entryCount
    def entriesWithPrefix(prefix: Hex): IO[Map[Hex, Array[Byte]]] = delegate.entriesWithPrefix(prefix)
    def build: IO[Either[MerklePatriciaError, MerklePatriciaTrie]] = delegate.build
    def buildForOrdinal(ordinal: SnapshotOrdinal): IO[Either[MerklePatriciaError, MerklePatriciaTrie]] =
      delegate.buildForOrdinal(ordinal)
    def getRootHashForOrdinal(ordinal: SnapshotOrdinal): IO[Option[MptRoot]] = delegate.getRootHashForOrdinal(ordinal)
    def getCurrentRootHash: IO[Option[MptRoot]] = delegate.getCurrentRootHash
    def getLastBuiltOrdinal: IO[Option[SnapshotOrdinal]] = delegate.getLastBuiltOrdinal
    def insert[A: Encoder](data: Map[Hex, A]): IO[Either[MerklePatriciaError, Unit]] = delegate.insert(data)
    def insertBytes(data: Map[Hex, Array[Byte]]): IO[Either[MerklePatriciaError, Unit]] = delegate.insertBytes(data)
    def replaceBytes(
      upserts: Map[Hex, Array[Byte]],
      removals: List[Hex]
    ): IO[Either[MerklePatriciaError, Unit]] = delegate.replaceBytes(upserts, removals)
    def update[A: Encoder](key: Hex, value: A): IO[Either[MerklePatriciaError, Unit]] = delegate.update(key, value)
    def remove(keys: List[Hex]): IO[Either[MerklePatriciaError, Unit]] = delegate.remove(keys)
    def clear: IO[Unit] = delegate.clear
    def getProver: IO[MerklePatriciaSingleInclusionProver[IO]] = delegate.getProver
    def buildHexMap(data: Map[GlobalStateKey, Json]): IO[Map[Hex, Array[Byte]]] = delegate.buildHexMap(data)
    def savepoint: IO[ProducerSavepoint[IO]] = delegate.savepoint
  }

  private final class BeforeBuildProducer(
    delegate: StatefulMerklePatriciaProducer[IO],
    beforeBuildForOrdinal: IO[Unit],
    restoreFailure: IO[Option[Throwable]] = IO.pure(None)
  ) extends StatefulMerklePatriciaProducer[IO] {

    override def physicalKeyPolicy: PhysicalTrieKeyPolicy = delegate.physicalKeyPolicy

    def entries: IO[Map[Hex, Array[Byte]]] = delegate.entries
    def physicalKeys: IO[Set[Hex]] = delegate.physicalKeys
    def entriesForKeys(keys: Set[Hex]): IO[Map[Hex, Array[Byte]]] = delegate.entriesForKeys(keys)
    def entry(key: Hex): IO[Option[Array[Byte]]] = delegate.entry(key)
    def entryCount: IO[Int] = delegate.entryCount
    def entriesWithPrefix(prefix: Hex): IO[Map[Hex, Array[Byte]]] = delegate.entriesWithPrefix(prefix)
    def build: IO[Either[MerklePatriciaError, MerklePatriciaTrie]] = delegate.build
    def buildForOrdinal(ordinal: SnapshotOrdinal): IO[Either[MerklePatriciaError, MerklePatriciaTrie]] =
      beforeBuildForOrdinal >> delegate.buildForOrdinal(ordinal)
    def getRootHashForOrdinal(ordinal: SnapshotOrdinal): IO[Option[MptRoot]] = delegate.getRootHashForOrdinal(ordinal)
    def getCurrentRootHash: IO[Option[MptRoot]] = delegate.getCurrentRootHash
    def getLastBuiltOrdinal: IO[Option[SnapshotOrdinal]] = delegate.getLastBuiltOrdinal
    def insert[A: Encoder](data: Map[Hex, A]): IO[Either[MerklePatriciaError, Unit]] = delegate.insert(data)
    def insertBytes(data: Map[Hex, Array[Byte]]): IO[Either[MerklePatriciaError, Unit]] = delegate.insertBytes(data)
    def replaceBytes(
      upserts: Map[Hex, Array[Byte]],
      removals: List[Hex]
    ): IO[Either[MerklePatriciaError, Unit]] = delegate.replaceBytes(upserts, removals)
    def update[A: Encoder](key: Hex, value: A): IO[Either[MerklePatriciaError, Unit]] = delegate.update(key, value)
    def remove(keys: List[Hex]): IO[Either[MerklePatriciaError, Unit]] = delegate.remove(keys)
    def clear: IO[Unit] = delegate.clear
    def getProver: IO[MerklePatriciaSingleInclusionProver[IO]] = delegate.getProver
    def buildHexMap(data: Map[GlobalStateKey, Json]): IO[Map[Hex, Array[Byte]]] = delegate.buildHexMap(data)
    def savepoint: IO[ProducerSavepoint[IO]] =
      delegate.savepoint.map { savepoint =>
        new ProducerSavepoint[IO] {
          def restore: IO[Unit] =
            restoreFailure.flatMap(_.fold(savepoint.restore)(IO.raiseError))
        }
      }
  }

  private def hashBytes(hash: Hash): Array[Byte] =
    ImmutableCodec[Hash].immutableBytes(hash).toArray

  private def fixture(
    res: Res,
    entries: List[(Hex, Hash)],
    retention: Int = HistoricalCommitmentSmtStore.UnboundedVersionRetention,
    hasherOverride: Option[Hasher[IO]] = None,
    appendHook: Option[HistoricalCommitmentAppendHook[IO]] = None
  ): IO[Fixture] =
    rawFixture(
      res,
      entries.map { case (key, hash) => key -> hashBytes(hash) }.toMap,
      retention,
      hasherOverride,
      appendHook
    )

  private def rawFixture(
    res: Res,
    entries: Map[Hex, Array[Byte]],
    retention: Int = HistoricalCommitmentSmtStore.UnboundedVersionRetention,
    hasherOverride: Option[Hasher[IO]] = None,
    appendHook: Option[HistoricalCommitmentAppendHook[IO]] = None
  ): IO[Fixture] = {
    implicit val hh: Hasher[IO] = hasherOverride.getOrElse(res._1)
    implicit val sp: SecurityProvider[IO] = res._2
    implicit val js: JsonSerializer[IO] = res._3
    val _ = (sp, js)
    for {
      producer <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](CommitmentKey.EncodedBytes, entries)
      durable <- MptStore.make[IO, CommitmentKey](producer, CommitmentKey.toHexF[IO])
      store <- appendHook.fold(HistoricalCommitmentSmtStore.make[IO](durable, retention))(
        HistoricalCommitmentSmtStore.makeWithAppendHook[IO](durable, retention, _)
      )
    } yield Fixture(store, producer, durable)
  }

  private def controlledBeforeNthHash(
    delegate: Hasher[IO],
    armed: Ref[IO, Boolean],
    calls: Ref[IO, Int],
    n: Int,
    action: IO[Unit]
  ): Hasher[IO] =
    new BeforeHasher(
      delegate,
      armed.get.ifM(
        calls.updateAndGet(_ + 1).flatMap(call => action.whenA(call === n)),
        IO.unit
      )
    )

  private def fresh(
    res: Res,
    retention: Int = HistoricalCommitmentSmtStore.UnboundedVersionRetention
  ): IO[HistoricalCommitmentSmtStore[IO]] = {
    implicit val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]) = res
    val _ = (sp, js)
    HistoricalCommitmentSmtStore.inMemory[IO](retention)
  }

  /** Drive `appendAtFinality` for every snapshot N in `[k .. upTo]`, committing the eligible ordinal `N−k`'s commitment under version N.
    * This is exactly the per-accept(N) sink the GSAM wiring performs (in strict-increasing N order).
    */
  private def driveChain(store: HistoricalCommitmentSmtStore[IO], upTo: Long, k: Long = K): IO[Unit] =
    (k to upTo).toList.traverse_ { n =>
      store.appendAtFinality(ord(n), ord(n - k), commitmentFor(n - k))
    }

  test("producer == simulated-follower smtRoot for the SAME chain (two independent stores, identical sequence)") { res =>
    for {
      producer <- fresh(res)
      follower <- fresh(res)
      _ <- driveChain(producer, upTo = 30L)
      // follower feeds the SAME (N, N−k, commitment) sequence — modeling the symmetric accept() path on every node.
      _ <- driveChain(follower, upTo = 30L)
      pRoots <- (K to 30L).toList.traverse(n => producer.rootForSnapshot(ord(n)))
      fRoots <- (K to 30L).toList.traverse(n => follower.rootForSnapshot(ord(n)))
    } yield expect(pRoots === fRoots) && expect(pRoots.forall(_.isDefined))
  }

  test("smtRoot(N) is reproducible purely from the commitment SET {i : i <= N-k} (independent SMT rebuild matches)") { res =>
    implicit val (hh: Hasher[IO], _sp: SecurityProvider[IO], _js: JsonSerializer[IO]) = res
    val n = 25L
    for {
      store <- fresh(res)
      _ <- driveChain(store, upTo = n)
      storeRoot <- store.rootForSnapshot(ord(n)).map(_.get)
      // Independently rebuild an SMT over EXACTLY the commitments <= N-k, keyed by the same ordinal hex, value = commitment hash bytes.
      // No version axis, no incremental accumulation — a pure set. Its root must equal the store's smtRoot(N).
      pairs <- (0L to (n - K)).toList.traverse { i =>
        PerOrdinalCommitment.commitmentHash[IO](commitmentFor(i)).map { ch =>
          CommitmentKey.toHex(ord(i)) -> io.constellationnetwork.security.hex.Hex(ch.value).toBytes
        }
      }
      independent <- io.constellationnetwork.security.smt.InMemorySparseMerkleTree.make[IO](pairs.toMap)
      independentRoot <- independent.root
    } yield expect(storeRoot === independentRoot)
  }

  test("inclusion proof of a PAST ordinal's PerOrdinalCommitment verifies against smtRoot(N)") { res =>
    implicit val (hh: Hasher[IO], _sp: SecurityProvider[IO], _js: JsonSerializer[IO]) = res
    val verifier = SmtVerifier.make[IO]
    val n = 20L
    val target = 8L // a finalized ordinal well within the cutoff (target <= N-k = 15)
    for {
      store <- fresh(res)
      _ <- driveChain(store, upTo = n)
      root <- store.rootForSnapshot(ord(n)).map(_.get)
      expectedLeafHash <- PerOrdinalCommitment.commitmentHash[IO](commitmentFor(target))
      proof <- store.proveAt(ord(n), ord(target))
      verified <- proof match {
        case Right(incl: SmtProof.Inclusion) =>
          verifier.verify(root, incl).map {
            case Right(v) =>
              v.value match {
                // The leaf VALUE is the commitment hash bytes; revealing + re-hashing reproduces it (binds the past ordinal's whole tuple).
                case SmtEntry.Present(_, value) =>
                  expect(io.constellationnetwork.security.hex.Hex.fromBytes(value).value === expectedLeafHash.value)
                case other => failure(s"expected Present, got $other")
              }
            case Left(err) => failure(s"verify failed: $err")
          }
        case other => IO.pure(failure(s"expected Inclusion for past ordinal, got $other"))
      }
    } yield verified
  }

  test("CIRCULARITY-FREE: ordinal N's OWN commitment (its incremental hash) is NOT in smtRoot(N) — proveAt(N, N) is Absence") { res =>
    implicit val (hh: Hasher[IO], _sp: SecurityProvider[IO], _js: JsonSerializer[IO]) = res
    val verifier = SmtVerifier.make[IO]
    val n = 20L
    for {
      store <- fresh(res)
      _ <- driveChain(store, upTo = n)
      root <- store.rootForSnapshot(ord(n)).map(_.get)
      // smtRoot(N) commits ordinals <= N-k. Ordinal N itself (and every ordinal in (N-k, N]) is NOT yet committed — its incremental hash
      // first appears in smtRoot(N+k). So proving ordinal N against smtRoot(N) must be ABSENCE.
      proofSelf <- store.proveAt(ord(n), ord(n))
      selfAbsent <- proofSelf match {
        case Right(abs: SmtProof.Absence) =>
          verifier.verify(root, abs).map {
            case Right(v)  => v.value match { case SmtEntry.Absent(_) => success; case o => failure(s"expected Absent, got $o") }
            case Left(err) => failure(s"verify absence failed: $err")
          }
        case other => IO.pure(failure(s"expected Absence for ordinal N in smtRoot(N), got $other"))
      }
      // The boundary ordinal N-k IS the most-recent included ordinal (inclusion); N-k+1 is the first EXCLUDED (absence).
      proofCutoff <- store.proveAt(ord(n), ord(n - K))
      proofJustAbove <- store.proveAt(ord(n), ord(n - K + 1))
      cutoffIncluded = expect(proofCutoff.exists { case _: SmtProof.Inclusion => true; case _ => false })
      justAboveAbsent = expect(proofJustAbove.exists { case _: SmtProof.Absence => true; case _ => false })
    } yield selfAbsent && cutoffIncluded && justAboveAbsent
  }

  test("warmup N < k has no root and N = k commits eligible ordinal zero") { res =>
    for {
      store <- fresh(res)
      // Production calls appendAtFinality at N = k with eligible ordinal zero. Earlier versions have no eligible incremental commitment.
      _ <- driveChain(store, upTo = K + 3L)
      warmupRoots <- (0L until K).toList.traverse(n => store.rootForSnapshot(ord(n)))
      firstReal <- store.rootForSnapshot(ord(K))
      zeroProof <- store.proveAt(ord(K), ord(0L))
    } yield
      expect(warmupRoots.forall(_.isEmpty)) &&
        expect(firstReal.isDefined) &&
        expect(zeroProof.exists { case _: SmtProof.Inclusion => true; case _ => false })
  }

  test("appendAtFinality latest-version exact duplicate is idempotent") { res =>
    for {
      store <- fresh(res)
      _ <- driveChain(store, upTo = 12L)
      before <- store.rootForSnapshot(ord(12L)).map(_.get)
      // Re-run accept(12) (e.g. validateArtifact after produce, or a re-proposal of the same ordinal).
      again <- store.appendAtFinality(ord(12L), ord(12L - K), commitmentFor(12L - K))
    } yield expect(before === again)
  }

  test("append failure after durable insert restores the old complete generation and retry plus reopen matches clean replay") { res =>
    implicit val hh: Hasher[IO] = res._1
    val failure = new RuntimeException("injected failure after durable insert")
    for {
      armed <- Ref.of[IO, Boolean](false)
      hook = new TestAppendHook(armed.get.ifM(IO.raiseError(failure), IO.unit), IO.unit)
      f <- rawFixture(res, Map.empty, appendHook = hook.some)
      _ <- driveChain(f.store, upTo = 12L)
      beforeRoot <- f.store.rootForSnapshot(ord(12L))
      beforeHash <- f.store.commitmentHashAt(ord(8L))
      _ <- armed.set(true)
      failed <- f.store.appendAtFinality(ord(13L), ord(8L), commitmentFor(8L)).attempt
      afterFailureRoot <- f.store.rootForSnapshot(ord(12L))
      unpublishedRoot <- f.store.rootForSnapshot(ord(13L))
      afterFailureHash <- f.store.commitmentHashAt(ord(8L))
      _ <- armed.set(false)
      retried <- f.store.appendAtFinality(ord(13L), ord(8L), commitmentFor(8L))
      reopened <- HistoricalCommitmentSmtStore.make[IO](f.durable, HistoricalCommitmentSmtStore.UnboundedVersionRetention)
      appendBeforeReplay <- reopened.appendAtFinality(ord(14L), ord(9L), commitmentFor(9L)).attempt
      _ <- reopened.replayFrom(K, ord(8L))
      reopenedRoot <- reopened.rootForSnapshot(ord(13L))
      clean <- fresh(res)
      _ <- driveChain(clean, upTo = 13L)
      cleanRoot <- clean.rootForSnapshot(ord(13L))
    } yield
      expect(failed.left.exists(_ eq failure)) &&
        expect(beforeRoot.isDefined) &&
        expect(afterFailureRoot === beforeRoot) &&
        expect(unpublishedRoot.isEmpty) &&
        expect(beforeHash.isEmpty) &&
        expect(afterFailureHash.isEmpty) &&
        expect(appendBeforeReplay.left.exists(_.isInstanceOf[HistoricalCommitmentReplayRequired])) &&
        expect(reopenedRoot.contains(retried)) &&
        expect(cleanRoot.contains(retried))
  }

  test("a completed filesystem image reopens only through strict replay and reproduces the published root") { res =>
    implicit val hh: Hasher[IO] = res._1
    implicit val js: JsonSerializer[IO] = res._3
    Files[IO].tempDirectory.use { directory =>
      for {
        source <- rawFixture(res, Map.empty)
        _ <- driveChain(source.store, upTo = 13L)
        expected <- source.store.rootForSnapshot(ord(13L))
        retainedImage <- source.durable.allEntriesAsBytes
        producer <- FileSystemMerklePatriciaProducer.makeWithFixedWidthKeys[IO](
          directory,
          CommitmentKey.EncodedBytes,
          retainedImage
        )
        // Await one explicit completed legacy persistence call. This exercises reopen/replay, not subprocess power-loss safety.
        _ <- producer.persist(ord(8L))
        restartedProducer <- FileSystemMerklePatriciaProducer.makeWithFixedWidthKeys[IO](
          directory,
          CommitmentKey.EncodedBytes
        )
        restartedDurable <- MptStore.make[IO, CommitmentKey](restartedProducer, CommitmentKey.toHexF[IO])
        loaded <- restartedDurable.loadPersisted(ord(8L))
        restarted <- HistoricalCommitmentSmtStore.make[IO](
          restartedDurable,
          HistoricalCommitmentSmtStore.UnboundedVersionRetention
        )
        appendBeforeReplay <- restarted.appendAtFinality(ord(14L), ord(9L), commitmentFor(9L)).attempt
        _ <- restarted.replayFrom(K, ord(8L))
        reproduced <- restarted.rootForSnapshot(ord(13L))
      } yield
        expect(loaded) &&
          expect(appendBeforeReplay.left.exists(_.isInstanceOf[HistoricalCommitmentReplayRequired])) &&
          expect(reproduced === expected)
    }
  }

  test("append cancellation before durable commit restores the old complete generation") { res =>
    for {
      armed <- Ref.of[IO, Boolean](false)
      entered <- Deferred[IO, Unit]
      hook = new TestAppendHook(IO.unit, armed.get.ifM(entered.complete(()).void >> IO.never, IO.unit))
      f <- rawFixture(res, Map.empty, appendHook = hook.some)
      _ <- driveChain(f.store, upTo = 12L)
      before <- f.store.rootForSnapshot(ord(12L))
      _ <- armed.set(true)
      append <- f.store.appendAtFinality(ord(13L), ord(8L), commitmentFor(8L)).start
      _ <- entered.get.timeout(5.seconds)
      _ <- append.cancel
      outcome <- append.join
      after <- f.store.rootForSnapshot(ord(12L))
      unpublished <- f.store.rootForSnapshot(ord(13L))
      retained <- f.store.commitmentHashAt(ord(8L))
    } yield
      expect(outcome match { case Outcome.Canceled() => true; case _ => false }) &&
        expect(after === before) &&
        expect(unpublished.isEmpty) &&
        expect(retained.isEmpty)
  }

  test("append preparation hashing failure leaves both publications at the old generation") { res =>
    val failure = new RuntimeException("injected append preparation hashing failure")
    for {
      armed <- Ref.of[IO, Boolean](false)
      calls <- Ref.of[IO, Int](0)
      controlled = controlledBeforeNthHash(res._1, armed, calls, n = 1, action = IO.raiseError(failure))
      f <- rawFixture(res, Map.empty, hasherOverride = controlled.some)
      _ <- driveChain(f.store, upTo = 12L)
      before <- f.store.rootForSnapshot(ord(12L))
      _ <- armed.set(true)
      failed <- f.store.appendAtFinality(ord(13L), ord(8L), commitmentFor(8L)).attempt
      after <- f.store.rootForSnapshot(ord(12L))
      unpublished <- f.store.rootForSnapshot(ord(13L))
      retained <- f.store.commitmentHashAt(ord(8L))
    } yield
      expect(failed.left.exists(_ eq failure)) &&
        expect(after === before) &&
        expect(unpublished.isEmpty) &&
        expect(retained.isEmpty)
  }

  test("cancellation after durable commit begins cannot split durable and live publication") { res =>
    implicit val hh: Hasher[IO] = res._1
    implicit val js: JsonSerializer[IO] = res._3
    for {
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      delegate <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](CommitmentKey.EncodedBytes)
      producer = new BeforeBuildProducer(delegate, entered.complete(()).void >> release.get)
      durable <- MptStore.make[IO, CommitmentKey](producer, CommitmentKey.toHexF[IO])
      store <- HistoricalCommitmentSmtStore.make[IO](durable, HistoricalCommitmentSmtStore.UnboundedVersionRetention)
      append <- store.appendAtFinality(ord(K), ord(0L), commitmentFor(0L)).start
      _ <- entered.get.timeout(5.seconds)
      cancel <- append.cancel.start
      _ <- release.complete(())
      _ <- cancel.joinWithNever
      outcome <- append.join
      liveRoot <- store.rootForSnapshot(ord(K))
      durableHash <- store.commitmentHashAt(ord(0L))
      reopened <- HistoricalCommitmentSmtStore.make[IO](durable, HistoricalCommitmentSmtStore.UnboundedVersionRetention)
      _ <- reopened.replayFrom(K, ord(0L))
      reopenedRoot <- reopened.rootForSnapshot(ord(K))
    } yield
      expect(outcome match { case Outcome.Succeeded(_) | Outcome.Canceled() => true; case _ => false }) &&
        expect(liveRoot.isDefined) &&
        expect(durableHash.isDefined) &&
        expect(reopenedRoot === liveRoot)
  }

  test("durable commit failure rolls back the inserted leaf and preserves the old live generation") { res =>
    implicit val hh: Hasher[IO] = res._1
    implicit val js: JsonSerializer[IO] = res._3
    val failure = new RuntimeException("injected durable commit failure")
    for {
      armed <- Ref.of[IO, Boolean](false)
      delegate <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](CommitmentKey.EncodedBytes)
      producer = new BeforeBuildProducer(delegate, armed.get.ifM(IO.raiseError(failure), IO.unit))
      durable <- MptStore.make[IO, CommitmentKey](producer, CommitmentKey.toHexF[IO])
      store <- HistoricalCommitmentSmtStore.make[IO](durable, HistoricalCommitmentSmtStore.UnboundedVersionRetention)
      _ <- driveChain(store, upTo = 12L)
      before <- store.rootForSnapshot(ord(12L))
      _ <- armed.set(true)
      failed <- store.appendAtFinality(ord(13L), ord(8L), commitmentFor(8L)).attempt
      after <- store.rootForSnapshot(ord(12L))
      unpublished <- store.rootForSnapshot(ord(13L))
      retained <- store.commitmentHashAt(ord(8L))
    } yield
      expect(failed.left.exists(_ eq failure)) &&
        expect(after === before) &&
        expect(unpublished.isEmpty) &&
        expect(retained.isEmpty)
  }

  test("rollback failure poisons the live store and every later operation returns the same latched failure") { res =>
    implicit val hh: Hasher[IO] = res._1
    implicit val js: JsonSerializer[IO] = res._3
    val operationFailure = new RuntimeException("injected pre-commit failure")
    val rollbackFailure = new RuntimeException("injected rollback failure")
    for {
      hookArmed <- Ref.of[IO, Boolean](false)
      rollbackArmed <- Ref.of[IO, Boolean](false)
      delegate <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](CommitmentKey.EncodedBytes)
      producer = new BeforeBuildProducer(
        delegate,
        IO.unit,
        rollbackArmed.get.map(Option.when(_)(rollbackFailure))
      )
      durable <- MptStore.make[IO, CommitmentKey](producer, CommitmentKey.toHexF[IO])
      hook = new TestAppendHook(hookArmed.get.ifM(IO.raiseError(operationFailure), IO.unit), IO.unit)
      store <- HistoricalCommitmentSmtStore.makeWithAppendHook[IO](
        durable,
        HistoricalCommitmentSmtStore.UnboundedVersionRetention,
        hook
      )
      _ <- driveChain(store, upTo = 12L)
      _ <- hookArmed.set(true) >> rollbackArmed.set(true)
      failed <- store.appendAtFinality(ord(13L), ord(8L), commitmentFor(8L)).attempt
      rootRead <- store.rootForSnapshot(ord(12L)).attempt
      durableRead <- store.commitmentHashAt(ord(7L)).attempt
      retry <- store.appendAtFinality(ord(13L), ord(8L), commitmentFor(8L)).attempt
      poison = failed.left.toOption
    } yield
      expect(poison.exists(_.isInstanceOf[HistoricalCommitmentAppendRollbackFailed])) &&
        expect(poison.exists(p => rootRead.left.exists(_ eq p))) &&
        expect(poison.exists(p => durableRead.left.exists(_ eq p))) &&
        expect(poison.exists(p => retry.left.exists(_ eq p)))
  }

  test("one append hashes one structural-sharing update rather than retained history") { res =>
    for {
      armed <- Ref.of[IO, Boolean](false)
      calls <- Ref.of[IO, Int](0)
      counting = new BeforeHasher(res._1, armed.get.ifM(calls.update(_ + 1), IO.unit))
      f <- rawFixture(res, Map.empty, hasherOverride = counting.some)
      _ <- driveChain(f.store, upTo = 105L)
      _ <- calls.set(0) >> armed.set(true)
      _ <- f.store.appendAtFinality(ord(106L), ord(101L), commitmentFor(101L))
      appendHashCalls <- calls.get
    } yield
      // A full-history rebuild at 102 leaves performs tens of thousands of hashes. One forked leaf update is bounded by SMT depth plus
      // the dedicated MPT's one pending insertion and remains comfortably below this deterministic operation-count ceiling.
      expect(appendHashCalls > 0) && expect(appendHashCalls < 1024)
  }

  test("conflicting duplicate evidence and a skipped ordinal fail before either publication changes") { res =>
    for {
      store <- fresh(res)
      _ <- driveChain(store, upTo = 12L)
      before <- store.rootForSnapshot(ord(12L))
      retained <- store.commitmentHashAt(ord(7L))
      conflict <- store.appendAtFinality(ord(12L), ord(7L), commitmentFor(700L)).attempt
      wrongVersion <- store.appendAtFinality(ord(13L), ord(7L), commitmentFor(7L)).attempt
      skipped <- store.appendAtFinality(ord(14L), ord(9L), commitmentFor(9L)).attempt
      after <- store.rootForSnapshot(ord(12L))
      missingEight <- store.commitmentHashAt(ord(8L))
      missingNine <- store.commitmentHashAt(ord(9L))
    } yield
      expect(conflict.left.exists(_.isInstanceOf[ConflictingHistoricalCommitment])) &&
        expect(wrongVersion.left.exists(_.isInstanceOf[HistoricalCommitmentReplayLagMismatch])) &&
        expect(skipped.left.exists(_.isInstanceOf[NonContiguousHistoricalCommitment])) &&
        expect(after === before) &&
        expect(retained.isDefined) &&
        expect(missingEight.isEmpty) &&
        expect(missingNine.isEmpty)
  }

  test("chain-replay recovery is byte-identical and a second replayFrom is idempotent") { res =>
    for {
      live <- fresh(res)
      _ <- driveChain(live, upTo = 18L)
      liveRoots <- (K to 18L).toList.traverse(n => live.rootForSnapshot(ord(n)))
      durableEntries <- (0L to 18L - K).toList.traverse { n =>
        live.commitmentHashAt(ord(n)).map(_.get).map(CommitmentKey.toHex(ord(n)) -> _)
      }
      recovered <- fixture(res, durableEntries)
      _ <- recovered.store.replayFrom(K, ord(18L - K))
      firstReplayRoots <- (K to 18L).toList.traverse(n => recovered.store.rootForSnapshot(ord(n)))
      _ <- recovered.store.replayFrom(K, ord(18L - K))
      secondReplayRoots <- (K to 18L).toList.traverse(n => recovered.store.rootForSnapshot(ord(n)))
      // commitmentHashAt reads the current recovery KV image — present for every appended eligible ordinal.
      durableAt10 <- live.commitmentHashAt(ord(10L))
      expectedAt10 <- {
        implicit val (hh: Hasher[IO], _sp: SecurityProvider[IO], _js: JsonSerializer[IO]) = res
        PerOrdinalCommitment.commitmentHash[IO](commitmentFor(10L))
      }
    } yield
      expect(liveRoots.forall(_.isDefined)) &&
        expect(firstReplayRoots === liveRoots) &&
        expect(secondReplayRoots === firstReplayRoots) &&
        expect(durableAt10.contains(expectedAt10))
  }

  test("bounded-retention replay preserves exactly the same retained roots and pruned boundary") { res =>
    val retention = 3
    for {
      live <- fresh(res, retention)
      _ <- driveChain(live, upTo = 20L)
      durableEntries <- (0L to 20L - K).toList.traverse { n =>
        live.commitmentHashAt(ord(n)).map(_.get).map(CommitmentKey.toHex(ord(n)) -> _)
      }
      recovered <- fixture(res, durableEntries, retention)
      _ <- recovered.store.replayFrom(K, ord(20L - K))
      liveRetained <- (18L to 20L).toList.traverse(n => live.rootForSnapshot(ord(n)))
      recoveredRetained <- (18L to 20L).toList.traverse(n => recovered.store.rootForSnapshot(ord(n)))
      livePruned <- live.proveAt(ord(17L), ord(12L))
      recoveredPruned <- recovered.store.proveAt(ord(17L), ord(12L))
    } yield
      expect(liveRetained.forall(_.isDefined)) &&
        expect(recoveredRetained === liveRetained) &&
        expect(livePruned === Left(SmtProofError.UnknownVersion(ord(17L)))) &&
        expect(recoveredPruned === livePruned)
  }

  test("proveAt on a pruned/unknown version -> UnknownVersion") { res =>
    for {
      // retention = 3: only the latest 3 version-roots are queryable; older are pruned.
      store <- fresh(res, retention = 3)
      _ <- driveChain(store, upTo = 20L) // versions 6..20 recorded; only 18,19,20 retained
      old <- store.proveAt(ord(7L), ord(2L)) // version 7 pruned
    } yield expect(old === Left(SmtProofError.UnknownVersion(ord(7L))))
  }

  test("CommitmentKey decoder rejects truncated, oversized, trailing, noncanonical, malformed, and out-of-range keys") {
    val valid = Hex("0000000000abcdef")
    val invalid = List(
      null.asInstanceOf[Hex],
      Hex(valid.value.dropRight(2)),
      Hex(valid.value + "00"),
      Hex(valid.value.dropRight(1)),
      Hex(valid.value.toUpperCase),
      Hex(valid.value.updated(4, 'g')),
      Hex("8000000000000000"),
      Hex("ffffffffffffffff")
    )

    IO.pure(
      expect(CommitmentKey.decode(valid) == Right(CommitmentKey(ord(0xabcdefL))))
        .and(expect(invalid.forall(CommitmentKey.decode(_).isLeft)))
    )
  }

  test("CommitmentKey complete decoder rejects duplicate logical identities") {
    val canonical = Hex("0000000000abcdef")
    val alias = Hex(canonical.value.toUpperCase)

    IO.pure(expect(CommitmentKey.decodeAll(List(canonical, alias)).left.exists(_.isInstanceOf[DuplicateDurableNipopowIdentity])))
  }

  test("historical store rejects generic and wrong-width recovery producers before exposing an API") { res =>
    implicit val hh: Hasher[IO] = res._1
    implicit val js: JsonSerializer[IO] = res._3

    for {
      genericProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      genericDurable <- MptStore.make[IO, CommitmentKey](genericProducer, CommitmentKey.toHexF[IO])
      genericResult <- HistoricalCommitmentSmtStore
        .make[IO](genericDurable, HistoricalCommitmentSmtStore.UnboundedVersionRetention)
        .attempt
      wrongWidthProducer <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](CommitmentKey.EncodedBytes - 1)
      wrongWidthDurable <- MptStore.make[IO, CommitmentKey](wrongWidthProducer, CommitmentKey.toHexF[IO])
      wrongWidthResult <- HistoricalCommitmentSmtStore
        .make[IO](wrongWidthDurable, HistoricalCommitmentSmtStore.UnboundedVersionRetention)
        .attempt
      exactProducer <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](CommitmentKey.EncodedBytes)
      exactDurable <- MptStore.make[IO, CommitmentKey](exactProducer, CommitmentKey.toHexF[IO])
      exactResult <- HistoricalCommitmentSmtStore
        .make[IO](exactDurable, HistoricalCommitmentSmtStore.UnboundedVersionRetention)
        .attempt
    } yield
      expect.all(
        genericResult == Left(InvalidHistoricalCommitmentKeyPolicy(None)),
        wrongWidthResult == Left(InvalidHistoricalCommitmentKeyPolicy(Some(CommitmentKey.EncodedBytes - 1))),
        exactResult.isRight
      )
  }

  test("historical recovery producer rejects a malformed physical image before store construction") { res =>
    implicit val hh: Hasher[IO] = res._1
    implicit val js: JsonSerializer[IO] = res._3
    val valid = CommitmentKey.toHex(ord(1))
    val oversized = Hex("0000000000000002aa")

    for {
      result <- InMemoryMerklePatriciaProducer
        .makeWithFixedWidthKeys[IO](
          CommitmentKey.EncodedBytes,
          Map(valid -> hashBytes(h("valid")), oversized -> hashBytes(h("bad")))
        )
        .attempt
    } yield expect(result == Left(UnexpectedPhysicalTrieKeyWidth(oversized, CommitmentKey.EncodedBytes, CommitmentKey.EncodedBytes + 1)))
  }

  test("historical replay rejects a malformed recovery-KV value before publishing any derived version") { res =>
    val validKey = CommitmentKey.toHex(ord(1L))
    for {
      f <- rawFixture(res, Map.empty)
      _ <- f.store.appendAtFinality(ord(K), ord(0L), commitmentFor(0L))
      _ <- f.producer.insertBytes(Map(validKey -> Array[Byte](0x01, 0x02))).rethrow
      before <- f.store.rootForSnapshot(ord(K))
      beforeKeys <- f.producer.physicalKeys
      result <- f.store.replayFrom(K, ord(1L)).attempt
      replayPrefix <- f.store.rootForSnapshot(ord(K + 1L))
      after <- f.store.rootForSnapshot(ord(K))
      afterKeys <- f.producer.physicalKeys
    } yield
      expect(result.left.exists(_.isInstanceOf[MalformedDurableNipopowValue])) &&
        expect(before.isDefined) &&
        expect(after === before) &&
        expect(replayPrefix.isEmpty) &&
        expect(afterKeys == beforeKeys)
  }

  test("historical replay rejects a missing ordinal-zero prefix") { res =>
    for {
      f <- fixture(res, List(CommitmentKey.toHex(ord(1L)) -> h("one")))
      result <- f.store.replayFrom(K, ord(1L)).attempt
      published <- f.store.rootForSnapshot(ord(K + 1L))
    } yield
      expect(
        result.left.exists {
          case IncompleteDurableNipopowHistory(_, expected, 1, 0L, Some(observed)) =>
            expected === ord(1L) && observed === ord(1L)
          case _ => false
        }
      ) && expect(published.isEmpty)
  }

  test("historical replay rejects an internal ordinal gap") { res =>
    val entries = List(
      CommitmentKey.toHex(ord(0L)) -> h("zero"),
      CommitmentKey.toHex(ord(2L)) -> h("two")
    )
    for {
      f <- fixture(res, entries)
      result <- f.store.replayFrom(K, ord(2L)).attempt
      published <- f.store.rootForSnapshot(ord(K))
    } yield
      expect(
        result.left.exists {
          case IncompleteDurableNipopowHistory(_, expected, 2, 1L, Some(observed)) =>
            expected === ord(2L) && observed === ord(2L)
          case _ => false
        }
      ) && expect(published.isEmpty)
  }

  test("historical replay rejects a missing terminal suffix") { res =>
    val entries = List(
      CommitmentKey.toHex(ord(0L)) -> h("zero"),
      CommitmentKey.toHex(ord(1L)) -> h("one")
    )
    for {
      f <- fixture(res, entries)
      result <- f.store.replayFrom(K, ord(2L)).attempt
      published <- f.store.rootForSnapshot(ord(K))
    } yield
      expect(
        result.left.exists {
          case IncompleteDurableNipopowHistory(_, expected, 2, 2L, None) => expected === ord(2L)
          case _                                                         => false
        }
      ) && expect(published.isEmpty)
  }

  test("historical replay validates one immutable producer image under concurrent underlying mutation") { res =>
    implicit val hh: Hasher[IO] = res._1
    implicit val js: JsonSerializer[IO] = res._3
    val zero = CommitmentKey.toHex(ord(0L)) -> hashBytes(h("zero"))
    val one = CommitmentKey.toHex(ord(1L)) -> hashBytes(h("one"))

    for {
      snapshotTaken <- Deferred[IO, Unit]
      releaseSnapshot <- Deferred[IO, Unit]
      delegate <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](CommitmentKey.EncodedBytes, Map(zero))
      producer = new PausedSnapshotProducer(delegate, snapshotTaken, releaseSnapshot)
      durable <- MptStore.make[IO, CommitmentKey](producer, CommitmentKey.toHexF[IO])
      store <- HistoricalCommitmentSmtStore.make[IO](durable, HistoricalCommitmentSmtStore.UnboundedVersionRetention)
      replay <- store.replayFrom(K, ord(0L)).start
      _ <- snapshotTaken.get.timeout(5.seconds)
      _ <- delegate.insertBytes(Map(one)).rethrow
      _ <- releaseSnapshot.complete(())
      outcome <- replay.joinWithNever
      rootFromCapturedImage <- store.rootForSnapshot(ord(K))
      rootFromConcurrentSuffix <- store.rootForSnapshot(ord(K + 1L))
    } yield
      expect(outcome === ()) &&
        expect(rootFromCapturedImage.isDefined) &&
        expect(rootFromConcurrentSuffix.isEmpty)
  }

  test("historical replay preflights every derived version and rejects ordinal-plus-k overflow atomically") { res =>
    val entries = List(
      CommitmentKey.toHex(ord(0L)) -> h("zero"),
      CommitmentKey.toHex(ord(1L)) -> h("one")
    )
    for {
      f <- fixture(res, entries)
      result <- f.store.replayFrom(Long.MaxValue, ord(1L)).attempt
      replayPrefix <- f.store.rootForSnapshot(ord(Long.MaxValue))
    } yield
      expect(result.left.exists(_.isInstanceOf[MalformedDurableNipopowKey]))
        .and(expect(replayPrefix.isEmpty))
  }

  test("historical replay rejects a negative k before committing any version") { res =>
    for {
      f <- fixture(res, List(CommitmentKey.toHex(ord(1)) -> h("valid")))
      result <- f.store.replayFrom(-1L, ord(1L)).attempt
      replayPrefix <- f.store.rootForSnapshot(ord(K + 1L))
    } yield
      expect(result.left.exists(_.isInstanceOf[InvalidHistoricalReplayLag]))
        .and(expect(replayPrefix.isEmpty))
  }

  test("historical replay hashing failure preserves the complete previously published tree and exposes no prefix") { res =>
    val failure = new RuntimeException("injected replay hashing failure")
    val entries = (0L to 4L).toList.map(n => CommitmentKey.toHex(ord(n)) -> h(s"durable-$n"))
    for {
      armed <- Ref.of[IO, Boolean](false)
      calls <- Ref.of[IO, Int](0)
      controlled = controlledBeforeNthHash(res._1, armed, calls, n = 5, action = IO.raiseError(failure))
      f <- fixture(res, entries, hasherOverride = controlled.some)
      _ <- f.store.replayFrom(K, ord(4L))
      before <- (K to K + 4L).toList.traverse(n => f.store.rootForSnapshot(ord(n)))
      _ <- armed.set(true)
      result <- f.store.replayFrom(K, ord(4L)).attempt
      after <- (K to K + 4L).toList.traverse(n => f.store.rootForSnapshot(ord(n)))
    } yield
      expect(result.left.exists(_ eq failure)) &&
        expect(before.forall(_.isDefined)) &&
        expect(after === before) &&
        expect(after.forall(_.isDefined))
  }

  test("historical replay cancellation preserves the complete previously published tree and exposes no prefix") { res =>
    val entries = (0L to 4L).toList.map(n => CommitmentKey.toHex(ord(n)) -> h(s"durable-$n"))
    for {
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      armed <- Ref.of[IO, Boolean](false)
      calls <- Ref.of[IO, Int](0)
      controlled =
        controlledBeforeNthHash(res._1, armed, calls, n = 5, action = entered.complete(()).void >> release.get)
      f <- fixture(res, entries, hasherOverride = controlled.some)
      _ <- f.store.replayFrom(K, ord(4L))
      before <- (K to K + 4L).toList.traverse(n => f.store.rootForSnapshot(ord(n)))
      _ <- armed.set(true)
      replay <- f.store.replayFrom(K, ord(4L)).start
      _ <- entered.get.timeout(5.seconds)
      _ <- replay.cancel
      outcome <- replay.join
      after <- (K to K + 4L).toList.traverse(n => f.store.rootForSnapshot(ord(n)))
      wasCanceled = outcome match {
        case Outcome.Canceled() => true
        case _                  => false
      }
    } yield
      expect(wasCanceled) &&
        expect(before.forall(_.isDefined)) &&
        expect(after === before) &&
        expect(after.forall(_.isDefined))
  }
}
