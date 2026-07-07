package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect._
import cats.effect.std.Supervisor
import cats.syntax.all._

import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotConsensusFunctionsSuite
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.EventTrigger
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalArtifactMismatch
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed

import weaver.MutableIOSuite

/** TG-01 — regression pin for the June 2026 fork-storm fix: the `smtRootBlind` compare in
  * [[io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotConsensusFunctions.validateArtifact]] (the
  * `smtRootBlind(recreatedArtifact) === smtRootBlind(artifact)` at GlobalSnapshotConsensusFunctions.scala:286-295).
  *
  * The §3 NIPoPoW `stateProof.smtRoot` is gl0-maintained and PATH-DEPENDENT (folded from a separately-maintained accumulating SMT), so
  * honest nodes provably cannot reproduce it in lockstep — including it in the consensus `===` made ~97% of freshly-won blocks fail
  * content validation (the fork storm). The fix compares smtRoot-BLIND: both sides are copied with `smtRoot = None` before `===`.
  *
  * This suite pins both directions of that contract THROUGH the production `validateArtifact` path (no re-implementation of the blind
  * copy in test code):
  *
  *   1. two artifacts differing ONLY in `stateProof.smtRoot` ARE consensus-equal — `validateArtifact` returns `Right` for an incoming
  *      artifact whose `smtRoot` the local re-derivation cannot reproduce (the local unit harness re-derives `smtRoot = None`; the
  *      incoming carries `Some`). Removing the `smtRootBlind` copy makes this test fail — the exact fork-storm regression shape.
  *   1. the blind compare blinds ONLY `smtRoot`: an artifact differing in `stateProof.mptRoot` (the ledger root) is still REJECTED
  *      (`Left(GlobalArtifactMismatch)`) — the fix must not have widened into a general state-proof blindness.
  *
  * Harness: reuses [[GlobalSnapshotConsensusFunctionsSuite]]'s public fixtures (`getTestData` / `mkGlobalSnapshotConsensusFunctions`),
  * one fresh consensus-functions instance per `validateArtifact` call (each owns its MptStore — `createProposalArtifact` mutates it).
  */
object SmtRootBlindValidateArtifactSuite extends MutableIOSuite {

  override type Res = (Supervisor[IO], JsonSerializer[IO], Hasher[IO], SecurityProvider[IO], Metrics[IO])

  override def sharedResource: Resource[IO, Res] = for {
    supervisor <- Supervisor[IO]
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
    m <- Metrics.forAsync[IO](Seq((Metrics.unsafeLabelName("application"), name)))
  } yield (supervisor, j, h, sp, m)

  private val foreignSmtRoot: Hash = Hash("ab" * 32)
  private val foreignMptRoot: Hash = Hash("cd" * 32)

  /** Leader-produced artifact + the inputs a follower needs to validate it. */
  private def mkLeaderArtifact(
    implicit j: JsonSerializer[IO],
    sp: SecurityProvider[IO],
    h: Hasher[IO],
    m: Metrics[IO]
  ): IO[(GlobalIncrementalSnapshot, Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo, Set[PeerId])] =
    for {
      testData <- GlobalSnapshotConsensusFunctionsSuite.getTestData
      (leaderGscf, facilitators, signedLastArtifact, signedGenesis, scEvent) = testData
      created <- leaderGscf.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )
      (artifact, _, _) = created
    } yield (artifact, signedLastArtifact, signedGenesis.value.info.toGlobalSnapshotInfo, facilitators)

  test("TG-01 smtRoot-blind: an artifact differing ONLY in stateProof.smtRoot passes validateArtifact (consensus-equal)") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      leader <- mkLeaderArtifact
      (artifact, signedLastArtifact, lastContext, facilitators) = leader

      // Tamper ONLY the smtRoot. The unit harness's re-derivation attaches no smtRoot (historicalCommitmentSmt is unwired ⇒ None),
      // so the incoming Some(foreignSmtRoot) is a value the follower provably cannot reproduce — the exact production shape (the
      // NIPoPoW SMT is path-dependent node-local state).
      tamperedSmt = artifact.copy(stateProof = artifact.stateProof.copy(smtRoot = Some(foreignSmtRoot)))

      // A fresh follower instance (own MptStore) validates the smt-divergent artifact.
      followerGscf <- GlobalSnapshotConsensusFunctionsSuite.mkGlobalSnapshotConsensusFunctions()
      result <- followerGscf.validateArtifact(
        signedLastArtifact,
        lastContext,
        EventTrigger,
        tamperedSmt,
        facilitators,
        _ => None.pure[IO]
      )
    } yield
      expect.all(
        // Sanity: the tamper genuinely changed the artifact — the raw artifact Eq IS smtRoot-sensitive. This is what proves the
        // test bites: without the smtRootBlind copy in validateArtifact, the recreated artifact (smtRoot=None) would `=!=` the
        // incoming (smtRoot=Some) and validation would reject — the fork storm.
        artifact.stateProof.smtRoot =!= tamperedSmt.stateProof.smtRoot,
        tamperedSmt =!= artifact,
        // The blind compare accepts: smtRoot differences alone are consensus-equal.
        result.isRight,
        // And the leader's signed smtRoot value is preserved verbatim (validateArtifact returns the INCOMING artifact).
        result.exists(_._1.stateProof.smtRoot.contains(foreignSmtRoot))
      )
  }

  test("TG-01 mptRoot NOT blind: an artifact differing in stateProof.mptRoot is rejected (GlobalArtifactMismatch)") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      leader <- mkLeaderArtifact
      (artifact, signedLastArtifact, lastContext, facilitators) = leader

      // Baseline: an untampered artifact validates Right on a fresh follower — so a Left below is attributable to the tamper alone.
      baselineGscf <- GlobalSnapshotConsensusFunctionsSuite.mkGlobalSnapshotConsensusFunctions()
      baseline <- baselineGscf.validateArtifact(
        signedLastArtifact,
        lastContext,
        EventTrigger,
        artifact,
        facilitators,
        _ => None.pure[IO]
      )

      // Tamper the LEDGER root. The blind compare must NOT blind this: mptRoot is reproducible consensus state.
      tamperedMpt = artifact.copy(stateProof = artifact.stateProof.copy(mptRoot = Some(foreignMptRoot)))
      followerGscf <- GlobalSnapshotConsensusFunctionsSuite.mkGlobalSnapshotConsensusFunctions()
      result <- followerGscf.validateArtifact(
        signedLastArtifact,
        lastContext,
        EventTrigger,
        tamperedMpt,
        facilitators,
        _ => None.pure[IO]
      )
    } yield
      expect.all(
        baseline.isRight,
        // The tamper genuinely changed the mptRoot (the harness runs LegacyFormat, so the honest re-derivation carries
        // mptRoot = None and the incoming carries Some(foreign) — a strict mptRoot-only difference).
        artifact.stateProof.mptRoot =!= tamperedMpt.stateProof.mptRoot,
        // mptRoot divergence is REJECTED — and specifically as an artifact mismatch (the consensus compare fired). Were the
        // smtRootBlind copy ever widened to also blind mptRoot, this would come back Right and the test would fail.
        result.isLeft,
        result.swap.exists {
          case _: GlobalArtifactMismatch => true
          case _                         => false
        }
      )
  }
}
