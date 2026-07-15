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

/** Historical commitment activation is dark. Honest construction emits `smtRoot = None`, and artifact validation rejects a supplied root
  * rather than normalizing it away. `mptRoot` remains exact as well.
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

  test("dark smtRoot: an artifact supplying stateProof.smtRoot is rejected") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      leader <- mkLeaderArtifact
      (artifact, signedLastArtifact, lastContext, facilitators) = leader

      // Honest construction is dark and emits None. Supply a root that local reproduction cannot derive.
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
        artifact.stateProof.smtRoot.isEmpty,
        artifact.stateProof.smtRoot =!= tamperedSmt.stateProof.smtRoot,
        tamperedSmt =!= artifact,
        result.swap.exists(
          _.isInstanceOf[
            io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalArtifactActiveEraViolation
          ]
        )
      )
  }

  test("dark smtRoot does not weaken mptRoot: a differing mptRoot is rejected") { res =>
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

      // Tamper the ledger root. mptRoot is reproducible consensus state and must remain exact.
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
        // Exact artifact comparison rejects the mptRoot mismatch as an artifact mismatch.
        result.isLeft,
        result.swap.exists {
          case _: GlobalArtifactMismatch => true
          case _                         => false
        }
      )
  }
}
