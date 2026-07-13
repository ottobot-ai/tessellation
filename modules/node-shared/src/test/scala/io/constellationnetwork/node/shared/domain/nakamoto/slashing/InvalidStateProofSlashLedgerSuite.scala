package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.InvalidStateProofSlashingConfig
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashManager.{SlashReason, SlashedRegistryEntry}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotAcceptanceManager.{
  WatchtowerSlashRequest,
  applyWatchtowerSlashes
}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeAmount, DelegatedStakeRecord, UpdateDelegatedStake}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralAmount, NodeCollateralRecord, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** WATCHTOWER invalid-state-proof DURABLE LEDGER coverage (slashing part 3) — the new `Slashings` MPT partition (fieldId 34) write +
  * read-back + the `applyWatchtowerSlashes` GSAM fold. Companion to the pure `InvalidStateProofSlashManagerSuite`.
  *
  * Properties under test:
  *   - the `SlashedRegistryEntry` MPT value codec round-trips AND is byte-deterministic (the consensus-byte determinism bar);
  *   - `GlobalStateKey.slashingsKey` is deterministic per `(peer, shard, checkpoint)` and distinct across any differing component;
  *   - `applyWatchtowerSlashes` is a pure deterministic fold: removes the offenders' stake/collateral, emits one record per `(operator,
  *     shard, checkpoint)`, burns the full pool when there is no submitter, and credits the bounty when there is;
  *   - an end-to-end MPT write of the produced records is read back correctly by `InvalidStateProofSlashedReader.fromMptStore.wasSlashed`
  *     (true for a written `(shard, checkpoint)`; false for an un-slashed one) — the double-slash guard the validator consults.
  */
object InvalidStateProofSlashLedgerSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val shardZero: ShardId = ShardId(0).get
  private val shardOne: ShardId = ShardId(1).get
  private val cpA: Hash = Hash("a" * 64)
  private val cpB: Hash = Hash("b" * 64)
  private val ord: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(500L))
  private val epoch: EpochProgress = EpochProgress(NonNegLong.unsafeFrom(42L))

  private val offender: PeerId = PeerId(Hex("aa" * 64))
  private val honest: PeerId = PeerId(Hex("bb" * 64))
  private val delegatorA: Address = Address.fromBytes("delegatorA".getBytes("UTF-8"))
  private val delegatorB: Address = Address.fromBytes("delegatorB".getBytes("UTF-8"))
  private val submitter: Address = Address.fromBytes("submitter".getBytes("UTF-8"))

  private val config: InvalidStateProofSlashingConfig =
    InvalidStateProofSlashingConfig(
      watchtowerEnabled = true,
      slashFraction = Ratio.One,
      bountyFraction = Ratio(1, 20),
      cooldownEpochs = 100L
    )

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
      delegatorA -> SortedSet(delegatedRecord(offender, delegatorA, 1000L), delegatedRecord(honest, delegatorA, 500L)),
      delegatorB -> SortedSet(delegatedRecord(offender, delegatorB, 2000L))
    )
  private val priorCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]] =
    SortedMap(delegatorA -> SortedSet(collateralRecord(offender, delegatorA, 300L)))

  private val sampleEntry: SlashedRegistryEntry =
    SlashedRegistryEntry(
      peerId = offender,
      shardId = shardZero,
      disputedCheckpointHash = cpA,
      eventOrdinal = ord,
      cooldownUntilEpoch = EpochProgress(NonNegLong.unsafeFrom(142L)),
      evidenceDigest = cpA,
      reason = SlashReason.InvalidStateProof
    )

  // ---- value codec --------------------------------------------------------------------------------------------------------------------

  pureTest("entryCodec round-trips a SlashedRegistryEntry and is byte-deterministic") {
    val codec = InvalidStateProofSlashedReader.entryCodec
    val bytes1 = codec.immutableBytes(sampleEntry)
    val bytes2 = codec.immutableBytes(sampleEntry)
    val decoded = codec.fromImmutableBytes(bytes1)
    expect.all(
      bytes1 == bytes2,
      decoded == Right(sampleEntry)
    )
  }

  // ---- key constructor ----------------------------------------------------------------------------------------------------------------

  test("slashingsKey is deterministic per (peer,shard,checkpoint) and distinct across any differing component") { res =>
    implicit val (h, _, _) = res
    for {
      k1 <- GlobalStateKey.slashingsKey[IO](offender, shardZero, cpA)
      k1b <- GlobalStateKey.slashingsKey[IO](offender, shardZero, cpA)
      kPeer <- GlobalStateKey.slashingsKey[IO](honest, shardZero, cpA)
      kShard <- GlobalStateKey.slashingsKey[IO](offender, shardOne, cpA)
      kCp <- GlobalStateKey.slashingsKey[IO](offender, shardZero, cpB)
    } yield expect.all(k1 == k1b, k1 != kPeer, k1 != kShard, k1 != kCp)
  }

  // ---- pure fold ----------------------------------------------------------------------------------------------------------------------

  pureTest("applyWatchtowerSlashes (no submitter): removes offender records, emits one record per signer, burns the whole pool") {
    val app = applyWatchtowerSlashes(
      requests = List(WatchtowerSlashRequest(shardZero, cpA, List(offender), submitter = None)),
      priorDelegatedStakes = priorStakes,
      priorNodeCollaterals = priorCollaterals,
      priorBalances = SortedMap.empty[Address, Balance],
      eventOrdinal = ord,
      currentEpoch = epoch,
      config = config
    )
    val aStakeOps =
      app.slashedDelegatedStakes.getOrElse(delegatorA, SortedSet.empty[DelegatedStakeRecord]).toList.map(_.event.value.nodeId)
    expect.all(
      app.slashedDelegatedStakes.get(delegatorB).isEmpty,
      aStakeOps == List(honest),
      app.slashedNodeCollaterals.get(delegatorA).isEmpty,
      app.registryEntries.size == 1,
      app.registryEntries.head.peerId == offender,
      app.registryEntries.head.shardId == shardZero,
      app.registryEntries.head.disputedCheckpointHash == cpA,
      app.bountyBalanceDelta.isEmpty,
      // total = 1000 + 2000 + 300; no submitter ⇒ whole pool burns.
      app.totalBurned == 3300L
    )
  }

  pureTest("applyWatchtowerSlashes (with submitter): credits the bounty and burns the remainder") {
    val app = applyWatchtowerSlashes(
      requests = List(WatchtowerSlashRequest(shardZero, cpA, List(offender), submitter = Some(submitter))),
      priorDelegatedStakes = priorStakes,
      priorNodeCollaterals = priorCollaterals,
      priorBalances = SortedMap(submitter -> Balance(NonNegLong.unsafeFrom(10L))),
      eventOrdinal = ord,
      currentEpoch = epoch,
      config = config
    )
    val total = 3300L
    val bounty = math.floor(total.toDouble * 0.05d).toLong // 165
    expect.all(
      app.bountyBalanceDelta.get(submitter).map(_.value.value) == Some(10L + bounty),
      app.totalBurned == total - bounty
    )
  }

  pureTest("applyWatchtowerSlashes is order-deterministic over two requests and empty-input is a no-op") {
    val r1 = WatchtowerSlashRequest(shardOne, cpB, List(offender), submitter = None)
    val r2 = WatchtowerSlashRequest(shardZero, cpA, List(offender), submitter = None)
    val ab = applyWatchtowerSlashes(List(r1, r2), priorStakes, priorCollaterals, SortedMap.empty, ord, epoch, config)
    val ba = applyWatchtowerSlashes(List(r2, r1), priorStakes, priorCollaterals, SortedMap.empty, ord, epoch, config)
    val empty = applyWatchtowerSlashes(Nil, priorStakes, priorCollaterals, SortedMap.empty, ord, epoch, config)
    expect.all(
      ab.slashedDelegatedStakes == ba.slashedDelegatedStakes,
      ab.slashedNodeCollaterals == ba.slashedNodeCollaterals,
      ab.registryEntries.toSet == ba.registryEntries.toSet,
      ab.totalBurned == ba.totalBurned,
      empty.slashedDelegatedStakes == priorStakes,
      empty.slashedNodeCollaterals == priorCollaterals,
      empty.registryEntries.isEmpty,
      empty.totalBurned == 0L
    )
  }

  pureTest("applyWatchtowerSlashes coalesces one checkpoint key independent of duplicate request order") {
    val lowSubmitter = Address.fromBytes("a-submit".getBytes("UTF-8"))
    val highSubmitter = Address.fromBytes("z-submit".getBytes("UTF-8"))
    val low = WatchtowerSlashRequest(shardZero, cpA, List(offender), submitter = Some(lowSubmitter))
    val high = WatchtowerSlashRequest(shardZero, cpA, List(offender), submitter = Some(highSubmitter))
    val lowHigh = applyWatchtowerSlashes(List(low, high), priorStakes, priorCollaterals, SortedMap.empty, ord, epoch, config)
    val highLow = applyWatchtowerSlashes(List(high, low), priorStakes, priorCollaterals, SortedMap.empty, ord, epoch, config)
    val canonicalSubmitter = SortedSet(lowSubmitter, highSubmitter).head

    expect.all(
      lowHigh == highLow,
      lowHigh.registryEntries.size == 1,
      lowHigh.bountyBalanceDelta.keySet == Set(canonicalSubmitter),
      lowHigh.totalBurned == highLow.totalBurned
    )
  }

  pureTest("self-detected duplicate preserves burn-all policy independent of request order") {
    val submitter = Address.fromBytes("watchtower".getBytes("UTF-8"))
    val selfDetected = WatchtowerSlashRequest(shardZero, cpA, List(offender), submitter = None)
    val carried = WatchtowerSlashRequest(shardZero, cpA, List(offender), submitter = Some(submitter))
    val selfFirst = applyWatchtowerSlashes(List(selfDetected, carried), priorStakes, priorCollaterals, SortedMap.empty, ord, epoch, config)
    val carriedFirst =
      applyWatchtowerSlashes(List(carried, selfDetected), priorStakes, priorCollaterals, SortedMap.empty, ord, epoch, config)

    expect.all(
      selfFirst == carriedFirst,
      selfFirst.registryEntries.size == 1,
      selfFirst.bountyBalanceDelta.isEmpty,
      selfFirst.totalBurned > 0L
    )
  }

  // ---- MPT write + read-back ----------------------------------------------------------------------------------------------------------

  private def mkStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield store

  test("Slashings partition write is read back by fromMptStore.wasSlashed (true for written (shard,cp), false otherwise)") { res =>
    implicit val (h, _, js) = res
    implicit val codec: io.constellationnetwork.serde.ImmutableCodec[SlashedRegistryEntry] =
      InvalidStateProofSlashedReader.entryCodec
    for {
      store <- mkStore
      // Write the records the fold would produce for an upheld dispute on (shardZero, cpA), signed by both operators.
      app = applyWatchtowerSlashes(
        List(WatchtowerSlashRequest(shardZero, cpA, List(offender, honest), submitter = None)),
        priorStakes,
        priorCollaterals,
        SortedMap.empty[Address, Balance],
        ord,
        epoch,
        config
      )
      _ <- app.registryEntries.traverse_ { e =>
        GlobalStateKey.slashingsKey[IO](e.peerId, e.shardId, e.disputedCheckpointHash).flatMap { key =>
          store.insert[SlashedRegistryEntry](key, e)
        }
      }
      reader = InvalidStateProofSlashedReader.fromMptStore[IO](store)
      slashedA <- reader.wasSlashed(shardZero, cpA)
      duplicateA <- reader.wasSlashed(shardZero, cpA)
      notSlashedB <- reader.wasSlashed(shardZero, cpB)
      notSlashedShard <- reader.wasSlashed(shardOne, cpA)
    } yield expect.all(slashedA, duplicateA, !notSlashedB, !notSlashedShard, app.registryEntries.size == 2)
  }

  test("empty Slashings partition ⇒ wasSlashed is always false") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      reader = InvalidStateProofSlashedReader.fromMptStore[IO](store)
      r <- reader.wasSlashed(shardZero, cpA)
    } yield expect(!r)
  }

  test("fromGlobalStateReader sees a slash committed only in the exact proposal-parent branch") { res =>
    implicit val (h, _, js) = res
    implicit val codec: io.constellationnetwork.serde.ImmutableCodec[SlashedRegistryEntry] =
      InvalidStateProofSlashedReader.entryCodec
    val child = BranchId(Hash("c" * 64))
    for {
      store <- mkStore
      parentChildTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = parentChildTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      handle <- overlay.checkout(BranchId.base)
      acceptanceMpt = AcceptanceMpt.fromOverlay[IO](overlay, BranchId.base, handle)
      key <- GlobalStateKey.slashingsKey[IO](offender, shardZero, cpA)
      _ <- acceptanceMpt.insert[SlashedRegistryEntry](key, sampleEntry)
      _ <- overlay.commit(handle, child, ord)
      baseResult <- InvalidStateProofSlashedReader.fromMptStore[IO](store).wasSlashed(shardZero, cpA)
      exactParentReader = GlobalStateReader.fromOverlay[IO](overlay, child)
      parentResult <- InvalidStateProofSlashedReader.fromGlobalStateReader[IO](exactParentReader).wasSlashed(shardZero, cpA)
    } yield expect.all(!baseResult, parentResult)
  }
}
