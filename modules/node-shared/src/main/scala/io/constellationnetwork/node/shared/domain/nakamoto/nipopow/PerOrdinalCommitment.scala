package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect.Sync

import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import io.circe.syntax.EncoderOps

/** Per-ordinal NIPoPoW super-level — the `towerEligibility` slot of [[PerOrdinalCommitment]].
  *
  * Sourced (when wired) from the EXISTING tower computation: the highest super-level µ a current-canonical historical snapshot passed
  * (`TowerFinalizer.finalizeFromParts` returns the `Vector[LevelTrial]`; the eligibility is `max { t.level : t.passed }`, or `0` if no
  * super-level fired). The schema RESERVES this field from the start so the committed tuple shape never has to be re-derived once the tower
  * is wired into the (node-shared, producer/follower-symmetric) commitment sink.
  *
  *   - [[TowerEligibility.Level]] — the snapshot's highest passed super-level (`0` = level-0-only, the by-construction baseline).
  *   - [[TowerEligibility.NotComputed]] — the value has not (yet) been sourced from the tower at this commitment site. Distinct from
  *     `Level(0)`: `Level(0)` is an asserted "no super-level fired"; `NotComputed` is "the tower was not consulted here". This is the value
  *     the steady-state core commits until the tower-sourcing follow-up lands (see [[HistoricalCommitmentSmtStore]] / report STAGING).
  *     Keeping it a distinct, sealed case (rather than overloading `Level(0)`) means the eventual flip to real values is a pure value
  *     change with NO tuple-shape churn and NO ambiguity about which ordinals predate the wiring.
  */
@derive(encoder, decoder, eqv, show)
sealed trait TowerEligibility extends Product with Serializable

object TowerEligibility {
  final case class Level(value: Int) extends TowerEligibility
  case object NotComputed extends TowerEligibility

  /** Derive the eligibility from a tower trial vector: the highest passed super-level, or `Level(0)` if none passed. This is the EXACT
    * value the tower already computes (`LevelTrial.passed`) — never recomputed independently. Used once the commitment sink is wired to the
    * tower (the staged follow-up); the steady-state core uses [[NotComputed]].
    */
  def fromTrials(trials: Vector[LevelTrial]): TowerEligibility =
    Level(trials.collect { case t if t.passed => t.level }.maxOption.getOrElse(0))
}

/** §3 NIPoPoW — the typed tuple committed, one per current-canonical historical snapshot ordinal, as a leaf of the historical-commitment
  * SMT ([[HistoricalCommitmentSmtStore]]) whose single root is anchored as `smtRoot` in the gl0 `GlobalSnapshotStateProof`.
  *
  * All three artifacts are deterministic functions of one exact snapshot at ordinal `i` (so producer and follower derive byte-identical
  * commitments from the same on-disk snapshot at `i`):
  *
  *   - [[hypergraphRoot]] — the global MPT root at ordinal `i` (the existing `stateProof_i.mptRoot`, over LEDGER state). Independent of
  *     `smtRoot` (it is committed in snapshot `i` already), so it is committable even at the current ordinal — but the current SMT uses the
  *     same `i ≤ N−k` lag for all three fields. That lag prevents circularity; it does not make a Phase-2 hash reorg-proof.
  *   - [[incrementalSnapshotHash]] — the content hash of the incremental snapshot at `i`. THIS is the field that would be circular if
  *     committed for the CURRENT ordinal (`smtRoot(N) → incrementalHash(N) → hash(snapshot N) → stateProof_N → smtRoot(N)`); the `i ≤ N−k`
  *     cutoff dissolves it — snapshot `i`'s incremental hash is only committed by `smtRoot(i+k)`, so references descend strictly toward
  *     genesis (well-founded, like normal block-hash chaining).
  *   - [[towerEligibility]] — the per-ordinal NIPoPoW super-level, SOURCED from the existing tower (never recomputed). Reserved in the
  *     schema from the start; populated as [[TowerEligibility.NotComputed]] by the steady-state core pending the tower-sourcing follow-up.
  *
  * The leaf VALUE stored in the SMT is `commitmentHash` = `Hasher[F].hash(this)` (Circe-encoded, Blake2b via the typeclass — never
  * hand-rolled). One inclusion proof against `smtRoot(N)` then reveals this whole tuple for any committed `i ≤ N−k`. The current store
  * still needs exact-hash rollback/rebuild support before such proofs remain valid across a Phase-2 density replacement.
  */
@derive(encoder, decoder, eqv, show)
final case class PerOrdinalCommitment(
  hypergraphRoot: Hash,
  incrementalSnapshotHash: Hash,
  towerEligibility: TowerEligibility
)

object PerOrdinalCommitment {

  /** The leaf commitment value: `Hasher[F].hash(commitment)`. The SMT stores this hash as the leaf value bytes (hex-encoded), so an
    * inclusion proof binds the ordinal's position to this digest, and revealing the tuple + re-hashing reproduces it.
    */
  def commitmentHash[F[_]: Sync: Hasher](commitment: PerOrdinalCommitment): F[Hash] =
    Hasher[F].hash(commitment.asJson)
}
