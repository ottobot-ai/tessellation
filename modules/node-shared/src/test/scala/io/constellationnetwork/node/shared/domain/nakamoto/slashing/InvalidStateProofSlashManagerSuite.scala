package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.data.NonEmptySet

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeAmount, DelegatedStakeRecord, UpdateDelegatedStake}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralAmount, NodeCollateralRecord, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** WATCHTOWER invalid-state-proof LEDGER EFFECT — coverage for the pure [[InvalidStateProofSlashManager.applySlash]].
  *
  * Properties under test:
  *   - '''100% tier removes the offender's records''' from both `activeDelegatedStakes` and `activeNodeCollaterals`, leaving non-targets
  *     untouched.
  *   - '''Bounty/burn split''' is `floor(total × bountyFraction)` + remainder.
  *   - '''Determinism''': repeated application over the same inputs is byte-identical (pure).
  *   - '''Partial fraction''' reduces delegated stake in place via `currentAmount` and still fully removes collateral.
  *   - A registry entry is produced per slashed operator with the right cooldown.
  */
object InvalidStateProofSlashManagerSuite extends FunSuite {

  private val shardZero: ShardId = ShardId(0).get
  private val cpHash: Hash = Hash("c" * 64)
  private val evidenceDigest: Hash = Hash("e" * 64)
  private val ord: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(500L))
  private val epoch: EpochProgress = EpochProgress(NonNegLong.unsafeFrom(42L))

  private val offender: PeerId = PeerId(Hex("aa" * 64))
  private val honest: PeerId = PeerId(Hex("bb" * 64))
  private val delegatorA: Address = Address.fromBytes("delegatorA".getBytes("UTF-8"))
  private val delegatorB: Address = Address.fromBytes("delegatorB".getBytes("UTF-8"))

  private def proof: NonEmptySet[SignatureProof] =
    NonEmptySet.of(SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70))))

  private def delegatedRecord(operator: PeerId, source: Address, amt: Long): DelegatedStakeRecord =
    DelegatedStakeRecord(
      event = Signed(
        UpdateDelegatedStake.Create(
          source = source,
          nodeId = operator,
          amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(amt)),
          fee = io.constellationnetwork.schema.delegatedStake.DelegatedStakeFee(NonNegLong.unsafeFrom(0L)),
          tokenLockRef = Hash("d" * 64)
        ),
        proof
      ),
      createdAt = ord,
      rewards = Amount(NonNegLong.unsafeFrom(0L))
    )

  private def collateralRecord(operator: PeerId, source: Address, amt: Long): NodeCollateralRecord =
    NodeCollateralRecord(
      event = Signed(
        UpdateNodeCollateral.Create(
          source = source,
          nodeId = operator,
          amount = NodeCollateralAmount(NonNegLong.unsafeFrom(amt)),
          fee = io.constellationnetwork.schema.nodeCollateral.NodeCollateralFee(NonNegLong.unsafeFrom(0L)),
          tokenLockRef = Hash("d" * 64)
        ),
        proof
      ),
      createdAt = ord
    )

  private val priorStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]] =
    SortedMap(
      // delegatorA delegated to BOTH the offender and an honest operator; only the offender's record is slashed.
      delegatorA -> SortedSet(delegatedRecord(offender, delegatorA, 1000L), delegatedRecord(honest, delegatorA, 500L)),
      delegatorB -> SortedSet(delegatedRecord(offender, delegatorB, 2000L))
    )
  private val priorCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]] =
    SortedMap(delegatorA -> SortedSet(collateralRecord(offender, delegatorA, 300L), collateralRecord(honest, delegatorA, 700L)))

  test("100% slash removes the offender's stake + collateral records, keeps honest records, computes total = 1000+2000+300") {
    val res = InvalidStateProofSlashManager.applySlash(
      slashTargets = Set(offender),
      priorDelegatedStakes = priorStakes,
      priorNodeCollaterals = priorCollaterals,
      eventOrdinal = ord,
      currentEpoch = epoch,
      shardId = shardZero,
      disputedCheckpointHash = cpHash,
      evidenceDigest = evidenceDigest,
      slashFraction = 1.0d,
      bountyFraction = 0.05d,
      cooldownEpochs = 100L
    )
    // delegatorA keeps ONLY the honest record; delegatorB had only the offender ⇒ dropped entirely.
    val aStakeOps = res.slashedDelegatedStakes.getOrElse(delegatorA, SortedSet.empty[DelegatedStakeRecord]).toList.map(_.event.value.nodeId)
    val total = 1000L + 2000L + 300L
    expect.all(
      res.slashedDelegatedStakes.get(delegatorB).isEmpty,
      aStakeOps == List(honest),
      res.slashedNodeCollaterals.get(delegatorA).toList.flatMap(_.toList).map(_.event.value.nodeId) == List(honest),
      res.totalSlashedAmount == total,
      res.bountyAmount == math.floor(total.toDouble * 0.05d).toLong,
      res.burnedAmount == total - res.bountyAmount,
      res.newRegistryEntries.size == 1,
      res.newRegistryEntries.head.peerId == offender,
      res.newRegistryEntries.head.cooldownUntilEpoch == EpochProgress(NonNegLong.unsafeFrom(142L)),
      res.newRegistryEntries.head.reason == InvalidStateProofSlashManager.SlashReason.InvalidStateProof
    )
  }

  test("determinism: applySlash is a pure function — two runs over identical inputs are byte-identical") {
    def run = InvalidStateProofSlashManager.applySlash(
      Set(offender), priorStakes, priorCollaterals, ord, epoch, shardZero, cpHash, evidenceDigest, 1.0d, 0.05d, 100L
    )
    val a = run
    val b = run
    expect.all(
      a.slashedDelegatedStakes == b.slashedDelegatedStakes,
      a.slashedNodeCollaterals == b.slashedNodeCollaterals,
      a.newRegistryEntries == b.newRegistryEntries,
      a.totalSlashedAmount == b.totalSlashedAmount,
      a.bountyAmount == b.bountyAmount,
      a.burnedAmount == b.burnedAmount
    )
  }

  test("partial fraction reduces delegated stake in place (currentAmount) and still fully removes collateral") {
    val res = InvalidStateProofSlashManager.applySlash(
      slashTargets = Set(offender),
      priorDelegatedStakes = SortedMap(delegatorB -> SortedSet(delegatedRecord(offender, delegatorB, 2000L))),
      priorNodeCollaterals = SortedMap(delegatorA -> SortedSet(collateralRecord(offender, delegatorA, 300L))),
      eventOrdinal = ord,
      currentEpoch = epoch,
      shardId = shardZero,
      disputedCheckpointHash = cpHash,
      evidenceDigest = evidenceDigest,
      slashFraction = 0.5d,
      bountyFraction = 0.0d,
      cooldownEpochs = 10L
    )
    // 50% of 2000 = 1000 slashed from stake; 1000 remains. Collateral fully removed (300). total = 1000 + 300.
    val remaining = res.slashedDelegatedStakes.get(delegatorB).toList.flatMap(_.toList).map(_.amount.value.value)
    expect.all(
      remaining == List(1000L),
      res.slashedNodeCollaterals.get(delegatorA).isEmpty,
      res.totalSlashedAmount == 1000L + 300L,
      res.bountyAmount == 0L,
      res.burnedAmount == 1000L + 300L
    )
  }

  test("no targets ⇒ no-op: maps unchanged, total zero, no registry entries") {
    val res = InvalidStateProofSlashManager.applySlash(
      Set.empty[PeerId], priorStakes, priorCollaterals, ord, epoch, shardZero, cpHash, evidenceDigest, 1.0d, 0.05d, 100L
    )
    expect.all(
      res.slashedDelegatedStakes == priorStakes,
      res.slashedNodeCollaterals == priorCollaterals,
      res.totalSlashedAmount == 0L,
      res.bountyAmount == 0L,
      res.burnedAmount == 0L,
      res.newRegistryEntries.isEmpty
    )
  }

  test("bounty/burn split helper: floor semantics") {
    expect.all(
      InvalidStateProofSlashManager.splitBountyBurn(1000L, 0.05d) == ((50L, 950L)),
      InvalidStateProofSlashManager.splitBountyBurn(999L, 0.05d) == ((49L, 950L)),
      InvalidStateProofSlashManager.splitBountyBurn(0L, 0.5d) == ((0L, 0L))
    )
  }
}
