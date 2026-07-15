package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.MutableIOSuite

/** Unit coverage for `MptBaseConsistency.assertBaseConsistentOrPrune` (#56.10 Phase H).
  *
  * The check is intentionally conservative: it only fires when `mptStore.lastPersistedOrdinal` is `Some(_)` AND that ordinal is strictly
  * ahead of a non-zero, root-verified recovery head. A fresh boot (`None` base) and a nominal aligned state (base ≤ anchor) both no-op. The
  * pathological "base present but anchor=0" case fails loudly because pruning has no authenticated target.
  */
object MptBaseConsistencySuite extends MutableIOSuite {

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, j)

  private implicit val logger: Logger[IO] = Slf4jLogger.getLoggerFromName[IO]("MptBaseConsistencySuite")

  private def mkMptStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield mptStore

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  test("base = None (fresh boot) → no-op regardless of recovery anchor") { res =>
    implicit val (h, js) = res
    for {
      store <- mkMptStore
      _ <- MptBaseConsistency.assertBaseConsistentOrPrune[IO](store, IO.pure(0L))
      _ <- MptBaseConsistency.assertBaseConsistentOrPrune[IO](store, IO.pure(42L))
      after <- store.lastPersistedOrdinal
    } yield expect(after.isEmpty)
  }

  test("base ≤ recovery anchor → no-op") { res =>
    implicit val (h, js) = res
    for {
      store <- mkMptStore
      _ <- store.commit(ord(10)) // base = 10
      _ <- MptBaseConsistency.assertBaseConsistentOrPrune[IO](store, IO.pure(15L))
      after <- store.lastPersistedOrdinal
    } yield expect(after.contains(ord(10)))
  }

  test("base = recovery anchor → no-op") { res =>
    implicit val (h, js) = res
    for {
      store <- mkMptStore
      _ <- store.commit(ord(7))
      _ <- MptBaseConsistency.assertBaseConsistentOrPrune[IO](store, IO.pure(7L))
      after <- store.lastPersistedOrdinal
    } yield expect(after.contains(ord(7)))
  }

  test("base ahead of non-zero recovery anchor → prune fires") { res =>
    implicit val (h, js) = res
    for {
      store <- mkMptStore
      _ <- store.commit(ord(20)) // base = 20
      // first call prunes back to the restored anchor at 10
      _ <- MptBaseConsistency.assertBaseConsistentOrPrune[IO](store, IO.pure(10L))
      // second call is idempotent — still no error, in-memory ref unchanged
      _ <- MptBaseConsistency.assertBaseConsistentOrPrune[IO](store, IO.pure(10L))
    } yield success
  }

  test("base present but recovery anchor = 0 → fail loud") { res =>
    implicit val (h, js) = res
    for {
      store <- mkMptStore
      _ <- store.commit(ord(5))
      attempt <- MptBaseConsistency.assertBaseConsistentOrPrune[IO](store, IO.pure(0L)).attempt
    } yield expect(attempt.isLeft)
  }
}
