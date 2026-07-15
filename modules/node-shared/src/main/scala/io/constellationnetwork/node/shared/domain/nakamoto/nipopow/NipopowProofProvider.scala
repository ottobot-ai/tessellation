package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.LddConfig

/** §3 NIPoPoW S5 — observability/light-client seam exposing a [[TowerProofBuilder]] + [[TowerVerifier]] pair to the HTTP layer along with
  * the verification parameters bound at consensus-startup time (genesis eta, eta-rotation period, LDD config).
  *
  * '''Pattern.''' Mirrors `FinalityTriggerView` (#138). Constructed inside the `GlobalSnapshotConsensus` resource block once the
  * `TowerStore`, snapshot storage, and numerics interpreters are available; handed up to the HTTP layer via a `Ref[F,
  * Option[NipopowProofProvider[F]]]` populated only while exact branch-relative tower catch-up is ready. The route returns `503 Service
  * Unavailable` while the Ref is empty or the linearizable read gate reports a non-ready/rebuild/recovery state.
  *
  * '''What the route gets from the provider:'''
  *   1. `build(since, k)` — runs the complete [[TowerProofBuilder.build]] under the tower owner's readiness/mutation gate; the route
  *      enforces the `since ≤ head` bound itself after an available result.
  *   1. `verify(proof)` — delegates to [[TowerVerifier.verify]] with the pre-bound `(genesisEta, etaRotationSnapshots, lddConfig)`. The
  *      route doesn't need to know about these consensus-internal values.
  *
  * Pure observability — never feeds back into consensus.
  */
sealed trait NipopowProofUnavailable extends Product with Serializable

object NipopowProofUnavailable {
  case object CatchupNotReady extends NipopowProofUnavailable
  case object RebuildRequired extends NipopowProofUnavailable
  case object RecoveryRequired extends NipopowProofUnavailable
}

/** Linearizable read gate supplied by the tower catch-up owner. The complete proof build runs inside [[whenReady]], under the same mutation
  * exclusion boundary as tower catch-up. A readiness check detached from that boundary is insufficient because a request can retain a
  * provider while catch-up begins.
  */
trait NipopowProofReadGate[F[_]] {
  def whenReady[A](read: => F[A]): F[Either[NipopowProofUnavailable, A]]
}

trait NipopowProofProvider[F[_]] {

  /** Build a proof anchored at `since` with L0-suffix length `k` under the shared tower read/mutation gate.
    *
    * The route is responsible for argument bounds checks (`since ≤ head`, `k ≥ 1`) — this method does not validate them.
    */
  def build(since: SnapshotOrdinal, k: Int): F[Either[NipopowProofUnavailable, TowerProof]]

  /** Verify a proof using the parameters bound at consensus-startup time. Delegates to [[TowerVerifier.verify]]. */
  def verify(proof: TowerProof): F[Either[ProofError, Unit]]
}

object NipopowProofProvider {

  /** Wrap a `(builder, verifier)` pair with the linearizable read gate and verification parameters captured at startup. The genesis eta +
    * rotation period + LDD config are immutable for the lifetime of a Nakamoto process, so binding them here is safe.
    */
  def make[F[_]](
    builder: TowerProofBuilder[F],
    verifier: TowerVerifier[F],
    readGate: NipopowProofReadGate[F],
    genesisEta: Array[Byte],
    etaRotationSnapshots: Long,
    lddConfig: LddConfig
  ): NipopowProofProvider[F] = new NipopowProofProvider[F] {

    def build(since: SnapshotOrdinal, k: Int): F[Either[NipopowProofUnavailable, TowerProof]] =
      readGate.whenReady(builder.build(since, k))

    def verify(proof: TowerProof): F[Either[ProofError, Unit]] =
      verifier.verify(proof, genesisEta, etaRotationSnapshots, lddConfig)
  }
}
