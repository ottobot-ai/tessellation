package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.LddConfig

/** §3 NIPoPoW S5 — observability/light-client seam exposing a [[TowerProofBuilder]] + [[TowerVerifier]] pair to the HTTP layer along with
  * the verification parameters bound at consensus-startup time (genesis eta, eta-rotation period, LDD config).
  *
  * '''Pattern.''' Mirrors `FinalityTriggerView` (#138). Constructed inside the `GlobalSnapshotConsensus` resource block once the
  * `TowerStore`, snapshot storage, and numerics interpreters are available; handed up to the HTTP layer via a `Ref[F,
  * Option[NipopowProofProvider[F]]]` populated at startup. The route returns `503 Service Unavailable` while the Ref is empty — same
  * pattern as `FinalityTriggersRoutes`.
  *
  * '''What the route gets from the provider:'''
  *   1. `build(since, k)` — delegates to [[TowerProofBuilder.build]]; the route enforces the `since ≤ head` bound itself before calling.
  *   1. `verify(proof)` — delegates to [[TowerVerifier.verify]] with the pre-bound `(genesisEta, etaRotationSnapshots, lddConfig)`. The
  *      route doesn't need to know about these consensus-internal values.
  *
  * Pure observability — never feeds back into consensus.
  */
trait NipopowProofProvider[F[_]] {

  /** Build a proof anchored at `since` with L0-suffix length `k`. Delegates to the underlying [[TowerProofBuilder]].
    *
    * The route is responsible for argument bounds checks (`since ≤ head`, `k ≥ 1`) — this method does not validate them.
    */
  def build(since: SnapshotOrdinal, k: Int): F[TowerProof]

  /** Verify a proof using the parameters bound at consensus-startup time. Delegates to [[TowerVerifier.verify]]. */
  def verify(proof: TowerProof): F[Either[ProofError, Unit]]
}

object NipopowProofProvider {

  /** Wrap a `(builder, verifier)` pair with the verification parameters captured at startup. The genesis eta + rotation period + LDD config
    * are immutable for the lifetime of a Nakamoto process, so binding them here is safe.
    */
  def make[F[_]](
    builder: TowerProofBuilder[F],
    verifier: TowerVerifier[F],
    genesisEta: Array[Byte],
    etaRotationSnapshots: Long,
    lddConfig: LddConfig
  ): NipopowProofProvider[F] = new NipopowProofProvider[F] {

    def build(since: SnapshotOrdinal, k: Int): F[TowerProof] =
      builder.build(since, k)

    def verify(proof: TowerProof): F[Either[ProofError, Unit]] =
      verifier.verify(proof, genesisEta, etaRotationSnapshots, lddConfig)
  }
}
