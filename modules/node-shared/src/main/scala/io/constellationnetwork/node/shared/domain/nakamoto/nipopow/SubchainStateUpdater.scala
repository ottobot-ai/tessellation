package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

/** Pure deterministic transformation: given the parent snapshot's [[SubchainState]] and this snapshot's level-trial outcomes,
  * produce the new state. Increments each level-µ count by 1 iff that level's trial passed.
  *
  * Producer and verifier run the same function on the same inputs — byte-identical output by construction. No `F[_]`, no side
  * effects, no implicit state.
  */
object SubchainStateUpdater {

  /** Update parent state with this snapshot's trial results.
    *
    * '''Preconditions''':
    *   - `trials.size == SuperLevelParams.SuperLevelCount`
    *   - `trials(i).level == i + 1` (levels 1..L-1 in order; this is how [[LevelTrialComputer.runAll]] returns them)
    *   - `parent.levelCounts.size == SuperLevelParams.SuperLevelCount` (enforced by [[SubchainState]]'s require)
    */
  def updateFrom(parent: SubchainState, trials: Vector[LevelTrial]): SubchainState = {
    require(
      trials.size == SuperLevelParams.SuperLevelCount,
      s"trials must have ${SuperLevelParams.SuperLevelCount} entries, got ${trials.size}"
    )
    require(
      trials.zipWithIndex.forall { case (t, i) => t.level == i + 1 },
      s"trials must be ordered by level 1..${SuperLevelParams.SuperLevelCount}; got levels ${trials.map(_.level).mkString(",")}"
    )
    SubchainState(
      parent.levelCounts.zip(trials).map {
        case (count, trial) => if (trial.passed) count + 1L else count
      }
    )
  }
}
