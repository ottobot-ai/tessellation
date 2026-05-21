package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore, WithdrawalTimeLimit}
import io.constellationnetwork.schema.node._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** §G5 — byte-equivalence assertions for the full-structure materializers used by reward + metagraph-sync paths.
  *
  * Property under test: after `GlobalSnapshotInfo` is sync'd into an MPT store via the same `syncFromGlobalSnapshotInfo` the runtime uses
  * on bootstrap, calling each materializer back through a `GlobalStateReader.fromMptStore` yields a `SortedMap` whose content equals
  * `info.activeDelegatedStakes` / `info.activeNodeCollaterals` / `info.delegatedStakesWithdrawals` / `info.updateNodeParameters` — up to
  * schema-level idempotent normalizations (`Hex` lowercasing on round-trip).
  *
  * Why this matters: G5 swaps the production reward-calculation read path from `info.X` (full GSI map closures) to `materializeXFromMpt`
  * (MPT prefix-scan + per-record source-keying). The migration is byte-equivalent only if the GSI → MPT key derivation and value codec
  * round-trips agree on the leaf-level data the acceptance + verify paths consume. These tests pin that agreement at the codec/key level so
  * the GSI-to-MPT migration can't silently drift.
  *
  * Known caveat: the legacy `DelegatedStakeRecord` `Order` (= `Order[SnapshotOrdinal].contramap(_.createdAt)`) is narrower than the
  * `Ordering` declared on the same type — `SortedSetCodec.sortedSet[A: Order]` uses the `Order`-derived `Ordering`. So a multi-record
  * SortedSet whose records all share `createdAt` would collapse on round-trip. The tests below use distinct `createdAt` values to side-step
  * this independent schema bug.
  */
object G5RewardStateMaterializerSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimitCtx: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // ---- helpers --------------------------------------------------------------

  private def addr(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  // MPT immutable codec for Hash requires 64-hex-char wire form; encode each label as ASCII hex and pad.
  private def hashOf(seed: String): Hash =
    Hash(seed.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  // Use lowercase hex so the Id round-trips through the MPT `Hex` codec verbatim. The codec encodes via
  // `ByteVector.fromHexDescriptive` then decodes via `ByteVector.toHex` which always emits lowercase, and
  // `Id` equality is case-sensitive on the underlying string.
  private val testProofId = Id(Hex(""))
  private val testProof = SignatureProof(testProofId, Signature(Hex("")))
  private val testProofs = NonEmptySet.one[SignatureProof](testProof)

  private def mkDelegatedStakeCreate(source: Address, nodeId: PeerId, amount: Long, tlSeed: String): Signed[UpdateDelegatedStake.Create] =
    Signed(
      UpdateDelegatedStake.Create(
        source = source,
        nodeId = nodeId,
        amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(amount)),
        fee = DelegatedStakeFee(0L),
        tokenLockRef = hashOf(tlSeed)
      ),
      testProofs
    )

  private def mkNodeCollateralCreate(source: Address, nodeId: PeerId, amount: Long, tlSeed: String): Signed[UpdateNodeCollateral.Create] =
    Signed(
      UpdateNodeCollateral.Create(
        source = source,
        nodeId = nodeId,
        amount = NodeCollateralAmount(NonNegLong.unsafeFrom(amount)),
        tokenLockRef = hashOf(tlSeed)
      ),
      testProofs
    )

  private def mkUpdateNodeParameters(operatorAddr: Address, rewardFraction: Int): Signed[UpdateNodeParameters] =
    Signed(
      UpdateNodeParameters(
        source = operatorAddr,
        delegatedStakeRewardParameters = DelegatedStakeRewardParameters(RewardFraction.unsafeFrom(rewardFraction)),
        nodeMetadataParameters = NodeMetadataParameters("", ""),
        parent = UpdateNodeParametersReference(UpdateNodeParametersOrdinal(NonNegLong.unsafeFrom(0)), Hash.empty)
      ),
      testProofs
    )

  // Seeds the producer-backed in-memory MPT from the given `GlobalSnapshotInfo` and returns a reader
  // bound to that store.
  private def mkReader(
    info: GlobalSnapshotInfo
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[GlobalStateReader[IO]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- store.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(1L)))
    } yield GlobalStateReader.fromMptStore[IO](store)

  // ---- DelegatedStakeStateManager — activeDelegatedStakes -------------------

  test("materializeActiveDelegatedStakesFromMpt — byte-equivalent map for multi-address multi-record GSI") { res =>
    implicit val (h, _, js) = res

    val alice = addr("alice")
    val bob = addr("bob")
    val nodeP = PeerId(Hex("aaaaaaaaaaaaaaaa"))
    val nodeQ = PeerId(Hex("bbbbbbbbbbbbbbbb"))

    // Distinct `createdAt` per record so the `Order[DelegatedStakeRecord]`-keyed SortedSet codec
    // doesn't collapse equal-ordered records (see suite scaladoc — independent schema bug).
    val aliceStake1 =
      DelegatedStakeRecord(mkDelegatedStakeCreate(alice, nodeP, 1_000L, "alice-p"), SnapshotOrdinal(1L), Balance(0L), none, none)
    val aliceStake2 =
      DelegatedStakeRecord(mkDelegatedStakeCreate(alice, nodeQ, 2_000L, "alice-q"), SnapshotOrdinal(2L), Balance(0L), none, none)
    val bobStake = DelegatedStakeRecord(mkDelegatedStakeCreate(bob, nodeP, 3_000L, "bob-p"), SnapshotOrdinal(3L), Balance(0L), none, none)

    val expected: SortedMap[Address, SortedSet[DelegatedStakeRecord]] =
      SortedMap(alice -> SortedSet(aliceStake1, aliceStake2), bob -> SortedSet(bobStake))
    val info = GlobalSnapshotInfo.empty.copy(activeDelegatedStakes = Some(expected))

    for {
      reader <- mkReader(info)
      mgr = DelegatedStakeStateManager.make[IO](reader)
      got <- mgr.materializeActiveDelegatedStakesFromMpt
    } yield expect.same(got, expected)
  }

  test("materializeActiveDelegatedStakesFromMpt — empty GSI yields empty SortedMap") { res =>
    implicit val (h, _, js) = res

    val info = GlobalSnapshotInfo.empty
    for {
      reader <- mkReader(info)
      mgr = DelegatedStakeStateManager.make[IO](reader)
      got <- mgr.materializeActiveDelegatedStakesFromMpt
    } yield expect.same(got, SortedMap.empty[Address, SortedSet[DelegatedStakeRecord]])
  }

  // ---- DelegatedStakeStateManager — delegatedStakesWithdrawals --------------

  test("materializeDelegatedStakeWithdrawalsFromMpt — byte-equivalent map after MPT round-trip") { res =>
    implicit val (h, _, js) = res

    val alice = addr("alice")
    val carol = addr("carol")
    val nodeP = PeerId(Hex("aaaaaaaaaaaaaaaa"))

    val aliceWithdrawal = PendingDelegatedStakeWithdrawal(
      event = mkDelegatedStakeCreate(alice, nodeP, 1_000L, "alice-p"),
      rewards = Balance(NonNegLong(99L)),
      acceptedOrdinal = SnapshotOrdinal(1L),
      createdAt = io.constellationnetwork.schema.epoch.EpochProgress(NonNegLong(10L)),
      currentTokenLockRef = none,
      currentAmount = none
    )
    val carolWithdrawal = PendingDelegatedStakeWithdrawal(
      event = mkDelegatedStakeCreate(carol, nodeP, 2_000L, "carol-p"),
      rewards = Balance(NonNegLong(50L)),
      acceptedOrdinal = SnapshotOrdinal(2L),
      createdAt = io.constellationnetwork.schema.epoch.EpochProgress(NonNegLong(20L)),
      currentTokenLockRef = none,
      currentAmount = none
    )

    val expected: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]] =
      SortedMap(alice -> SortedSet(aliceWithdrawal), carol -> SortedSet(carolWithdrawal))
    val info = GlobalSnapshotInfo.empty.copy(delegatedStakesWithdrawals = Some(expected))

    for {
      reader <- mkReader(info)
      mgr = DelegatedStakeStateManager.make[IO](reader)
      got <- mgr.materializeDelegatedStakeWithdrawalsFromMpt
    } yield expect.same(got, expected)
  }

  // ---- NodeCollateralStateManager — activeNodeCollaterals -------------------

  test("materializeActiveNodeCollateralsFromMpt — byte-equivalent map for multi-address GSI") { res =>
    implicit val (h, _, js) = res

    val alice = addr("alice")
    val bob = addr("bob")
    val nodeP = PeerId(Hex("aaaaaaaaaaaaaaaa"))
    val nodeQ = PeerId(Hex("bbbbbbbbbbbbbbbb"))

    val aliceCollateral = NodeCollateralRecord(
      event = mkNodeCollateralCreate(alice, nodeP, 5_000L, "alice-p"),
      createdAt = SnapshotOrdinal(1L)
    )
    val bobCollateral1 = NodeCollateralRecord(
      event = mkNodeCollateralCreate(bob, nodeP, 6_000L, "bob-p"),
      createdAt = SnapshotOrdinal(2L)
    )
    val bobCollateral2 = NodeCollateralRecord(
      event = mkNodeCollateralCreate(bob, nodeQ, 7_000L, "bob-q"),
      createdAt = SnapshotOrdinal(3L)
    )

    val expected: SortedMap[Address, SortedSet[NodeCollateralRecord]] =
      SortedMap(alice -> SortedSet(aliceCollateral), bob -> SortedSet(bobCollateral1, bobCollateral2))
    val info = GlobalSnapshotInfo.empty.copy(activeNodeCollaterals = Some(expected))

    for {
      reader <- mkReader(info)
      mgr = NodeCollateralStateManager.make[IO](reader)
      got <- mgr.materializeActiveNodeCollateralsFromMpt
    } yield expect.same(got, expected)
  }

  // ---- UpdateNodeParametersStateReader --------------------------------------

  test("materializeUpdateNodeParametersFromMpt — byte-equivalent map keyed by Id") { res =>
    implicit val (h, _, js) = res

    val opA = addr("opA")
    val opB = addr("opB")
    val nodeAId = Id(Hex("aaaaaaaaaaaaaaaa"))
    val nodeBId = Id(Hex("bbbbbbbbbbbbbbbb"))

    // Mint UpdateNodeParameters signed with each operator's `Id` as the proof signer — the materializer
    // recovers the map's `Id` from `signed.proofs.head.id`, matching the GSAM-side convention.
    val unpA = Signed(
      UpdateNodeParameters(
        source = opA,
        delegatedStakeRewardParameters = DelegatedStakeRewardParameters(RewardFraction.unsafeFrom(10000000)),
        nodeMetadataParameters = NodeMetadataParameters("", ""),
        parent = UpdateNodeParametersReference(UpdateNodeParametersOrdinal(NonNegLong(0L)), Hash.empty)
      ),
      NonEmptySet.one[SignatureProof](SignatureProof(nodeAId, Signature(Hex(""))))
    )
    val unpB = Signed(
      UpdateNodeParameters(
        source = opB,
        delegatedStakeRewardParameters = DelegatedStakeRewardParameters(RewardFraction.unsafeFrom(5000000)),
        nodeMetadataParameters = NodeMetadataParameters("", ""),
        parent = UpdateNodeParametersReference(UpdateNodeParametersOrdinal(NonNegLong(0L)), Hash.empty)
      ),
      NonEmptySet.one[SignatureProof](SignatureProof(nodeBId, Signature(Hex(""))))
    )

    val expected: SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)] =
      SortedMap(nodeAId -> ((unpA, SnapshotOrdinal(1L))), nodeBId -> ((unpB, SnapshotOrdinal(2L))))
    val info = GlobalSnapshotInfo.empty.copy(updateNodeParameters = Some(expected))

    for {
      reader <- mkReader(info)
      sr = UpdateNodeParametersStateReader.make[IO](reader)
      got <- sr.materializeUpdateNodeParametersFromMpt
    } yield expect.same(got, expected)
  }

  test("materializeUpdateNodeParametersFromMpt — empty GSI yields empty SortedMap") { res =>
    implicit val (h, _, js) = res

    val info = GlobalSnapshotInfo.empty
    for {
      reader <- mkReader(info)
      sr = UpdateNodeParametersStateReader.make[IO](reader)
      got <- sr.materializeUpdateNodeParametersFromMpt
    } yield expect.same(got, SortedMap.empty[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)])
  }
}
