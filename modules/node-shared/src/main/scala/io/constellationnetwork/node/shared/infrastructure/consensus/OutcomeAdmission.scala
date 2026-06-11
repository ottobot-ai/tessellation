package io.constellationnetwork.node.shared.infrastructure.consensus

import io.constellationnetwork.schema.peer.PeerId

/** Layer-specific view of which peers a finished consensus outcome ADMITS to facilitate the next round.
  *
  * Admission = `eligibleOrFacilitators ∪ finished.candidates` of the consensus-agreed outcome — the only deterministic source of the
  * facilitator base (every honest node derives the identical set from the identical outcome).
  *
  * Two consumers (2026-06-11, run bimn7o09f — the ml0 2-node cohort boot-race fork):
  *
  *   - '''StateCreator gate''': a node whose `selfId` is NOT admitted by `lastOutcome` must not create (facilitate) a round. Before this,
  *     the facilitator base appended `selfId` unconditionally, so a joiner that reached Ready mid-round self-appointed as Leader of its own
  *     solo round and permanently forked the cohort (each side `facilitators=1`, mutually evicted, never reconciled — restart re-forked
  *     within one round).
  *   - '''Join gate''' ([[state.StateTransitions.initFromDownload]]): a joining node defers its Observing → Ready transition until the
  *     outcome it adopted from the cluster shows it admitted; until then it stays in the download/observe loop, where its registration
  *     remains advertised and the incumbent's rounds fold it through `candidates` into the eligible set.
  */
trait OutcomeAdmission[Outcome] {

  /** The consensus-agreed peers admitted to facilitate the round AFTER `outcome`. */
  def admittedPeers(outcome: Outcome): Set[PeerId]
}

object OutcomeAdmission {
  def apply[Outcome](implicit ev: OutcomeAdmission[Outcome]): OutcomeAdmission[Outcome] = ev

  def instance[Outcome](f: Outcome => Set[PeerId]): OutcomeAdmission[Outcome] = (outcome: Outcome) => f(outcome)
}
