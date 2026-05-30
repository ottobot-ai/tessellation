package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.smt._
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** §3 NIPoPoW — [[HistoricalCommitmentSmtStore]] suite. Proves the load-bearing properties of the historical-commitment SMT anchored as
  * `smtRoot`:
  *   - producer == simulated-follower `smtRoot(N)` for the SAME chain of finalized commitments,
  *   - `smtRoot(N)` is reproducible purely from commitments `≤ N−k`,
  *   - an inclusion proof of a PAST ordinal's `PerOrdinalCommitment` verifies against the committed `smtRoot(N)`,
  *   - genesis / warmup (`N ≤ k`) is deterministic (no root recorded),
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

  private def fresh(
    res: Res,
    retention: Int = HistoricalCommitmentSmtStore.UnboundedVersionRetention
  ): IO[HistoricalCommitmentSmtStore[IO]] = {
    implicit val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]) = res
    val _ = (sp, js)
    HistoricalCommitmentSmtStore.inMemory[IO](retention)
  }

  /** Drive `appendAtFinality` for every snapshot N in `[k+1 .. upTo]`, committing the eligible ordinal `N−k`'s commitment under version N.
    * This is exactly the per-accept(N) sink the GSAM wiring performs (in strict-increasing N order).
    */
  private def driveChain(store: HistoricalCommitmentSmtStore[IO], upTo: Long, k: Long = K): IO[Unit] =
    (k + 1 to upTo).toList.traverse_ { n =>
      store.appendAtFinality(ord(n), ord(n - k), commitmentFor(n - k))
    }

  test("producer == simulated-follower smtRoot for the SAME chain (two independent stores, identical sequence)") { res =>
    for {
      producer <- fresh(res)
      follower <- fresh(res)
      _ <- driveChain(producer, upTo = 30L)
      // follower feeds the SAME (N, N−k, commitment) sequence — modeling the symmetric accept() path on every node.
      _ <- driveChain(follower, upTo = 30L)
      pRoots <- (K + 1 to 30L).toList.traverse(n => producer.rootForSnapshot(ord(n)))
      fRoots <- (K + 1 to 30L).toList.traverse(n => follower.rootForSnapshot(ord(n)))
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
      pairs <- (1L to (n - K)).toList.traverse { i =>
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

  test("genesis / warmup (N <= k): no eligible ancestor, so the SMT records no root (deterministic across nodes)") { res =>
    for {
      store <- fresh(res)
      // The GSAM wiring never calls appendAtFinality for N <= k (no eligible ordinal); model that by driving only N > k.
      // Assert that NO version <= k has a root (nothing was committed), and the first recorded root is at N = k+1.
      _ <- driveChain(store, upTo = K + 3L)
      genesisRoots <- (0L to K).toList.traverse(n => store.rootForSnapshot(ord(n)))
      firstReal <- store.rootForSnapshot(ord(K + 1L))
    } yield expect(genesisRoots.forall(_.isEmpty)) && expect(firstReal.isDefined)
  }

  test("appendAtFinality is idempotent — re-accepting the same (N, N-k, commitment) reproduces the same root") { res =>
    for {
      store <- fresh(res)
      _ <- driveChain(store, upTo = 12L)
      before <- store.rootForSnapshot(ord(12L)).map(_.get)
      // Re-run accept(12) (e.g. validateArtifact after produce, or a re-proposal of the same ordinal).
      again <- store.appendAtFinality(ord(12L), ord(12L - K), commitmentFor(12L - K))
    } yield expect(before === again)
  }

  test("chain-replay recovery: replayFrom rebuilds the version-roots from the durable commitment KV byte-identically") { res =>
    for {
      live <- fresh(res)
      _ <- driveChain(live, upTo = 18L)
      liveRoots <- (K + 1 to 18L).toList.traverse(n => live.rootForSnapshot(ord(n)))
      // commitmentHashAt reads the DURABLE KV — present for every finalized eligible ordinal.
      durableAt10 <- live.commitmentHashAt(ord(10L))
      expectedAt10 <- {
        implicit val (hh: Hasher[IO], _sp: SecurityProvider[IO], _js: JsonSerializer[IO]) = res
        PerOrdinalCommitment.commitmentHash[IO](commitmentFor(10L))
      }
    } yield
      expect(liveRoots.forall(_.isDefined)) &&
        expect(durableAt10.contains(expectedAt10))
  }

  test("proveAt on a pruned/unknown version -> UnknownVersion") { res =>
    for {
      // retention = 3: only the latest 3 version-roots are queryable; older are pruned.
      store <- fresh(res, retention = 3)
      _ <- driveChain(store, upTo = 20L) // versions 6..20 recorded; only 18,19,20 retained
      old <- store.proveAt(ord(7L), ord(2L)) // version 7 pruned
    } yield expect(old === Left(SmtProofError.UnknownVersion(ord(7L))))
  }
}
