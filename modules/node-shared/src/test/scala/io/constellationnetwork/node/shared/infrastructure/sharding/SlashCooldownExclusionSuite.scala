package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.KeyPair

import cats.data.NonEmptyList
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashManager.{SlashReason, SlashedRegistryEntry}
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.{InvalidStateProofSlashedReader, SlashCooldownReader}
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointGl0AcceptanceManager
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore, WithdrawalTimeLimit}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.{GlobalSnapshotInfo, GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** FINDING-002 / EPIC-3.1+3.4 — slash-cooldown committee EXCLUSION, end to end.
  *
  * RED at HEAD `72f39d652` (captured before the fix): an operator with an unexpired `SlashedRegistryEntry` cooldown in the `Slashings`
  * (fieldId 34) partition was (1) still drawn into `committeeFor(shardId, epoch)` with unchanged probability — the draw never read the
  * partition — and (2) its checkpoint attestation could enter local tracking. GREEN with the
  * [[io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashCooldownReader]] gate wired through
  * [[ShardCheckpointWiring.committeeFor]]:
  *
  *   1. the slashed operator is ABSENT from the committee draw for every epoch whose anchor its record precedes (until cooldown expiry);
  *   1. its signature fails the committee-membership pre-check before it can enter the attestation tracker;
  *   1. exclusion ≡ shrinking the active set: the post-exclusion draw is BYTE-IDENTICAL to a draw whose `activeValidators` never contained
  *      the slashed peer (σ recomputed over the post-exclusion pool);
  *   1. cluster-uniformity: two independent nodes over the same pinned records — different insertion order, different base ordinals,
  *      different unrelated state — compute the identical excluded set, committee, and attestation-admission verdicts;
  *   1. epoch staggering + expiry: a record younger than the epoch's anchor does not bite yet (it bites the NEXT epoch); an expired
  *      cooldown restores eligibility;
  *   1. the Polkadot `UpToLimit` floor: exclusion never drops the eligible pool below `kQuorum`;
  *   1. cache discipline: an anchor-unsettled (future-epoch) draw is NOT memoized by `acceptanceDeps`' committee cache;
  *   1. no-op equivalence: an EMPTY `Slashings` partition draws byte-identically to the pre-fix code (`noExclusion`) — the `numShards = 1`
  *      regression bar (where nothing is constructed at all) plus the numShards>1 empty-partition bar;
  *   1. (EPIC-3.4) REORG DURABILITY: the exclusion survives a `syncFromGlobalSnapshotInfo` base rebuild — the S01/S02 preservation
  *      (`fc90f687e` / `fe6d837b1`) keeps the fieldId-34 bytes, and the SAME committee is drawn after the rebuild.
  */
object SlashCooldownExclusionSuite extends MutableIOSuite {

  override type Res =
    (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], RegisteredCheckpointSigner, CanonicalOperatorConsensusPopulation)

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  // Same implicit pattern as GsiRebuildSpentSetSurvivalSuite — required by the `syncFromGlobalSnapshotInfo` rebuild machinery.
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      checkpointSigner <- RegisteredCheckpointSigner.make.asResource
      operators <- CanonicalOperatorConsensusFixture.makePopulation(20)
    } yield (h, sp, j, checkpointSigner, operators)

  // ── Common fixtures ───────────────────────────────────────────────────────────────────────────────────────────────

  private val R: Long = 100L // eta-rotation period: anchor(E) = (E-1)·100 - 1
  private val epoch5: EtaPeriod = EtaPeriod(5L) // slashAnchorOrdinal(5, R=100) = 399
  private val shardZero: ShardId = ShardId.unsafeApply(0)

  private def validators(implicit population: CanonicalOperatorConsensusPopulation): Set[PeerId] =
    population.peerIds.toList.sorted.take(8).toSet
  private def slashedPeer(implicit population: CanonicalOperatorConsensusPopulation): PeerId = validators.toList.sorted.head
  private def operatorRegistry(implicit population: CanonicalOperatorConsensusPopulation): OperatorConsensusKeyRegistry[IO] =
    population.operatorKeyRegistry

  private val fixedEta: Array[Byte] = Array.fill[Byte](32)(7.toByte)
  private val etaForEpoch: EtaPeriod => IO[Array[Byte]] = _ => IO.pure(fixedEta)

  private def ord(v: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(v))

  private def entry(p: PeerId, eventOrd: Long, cooldownUntil: Long, cpHash: Hash = Hash("ab" * 32)): SlashedRegistryEntry =
    SlashedRegistryEntry(
      peerId = p,
      shardId = shardZero,
      disputedCheckpointHash = cpHash,
      eventOrdinal = ord(eventOrd),
      cooldownUntilEpoch = EpochProgress(NonNegLong.unsafeFrom(cooldownUntil)),
      evidenceDigest = cpHash,
      reason = SlashReason.InvalidStateProof
    )

  private def mkStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    InMemoryMerklePatriciaProducer.make[IO]().flatMap(MptStore.make[IO, GlobalStateKey](_, GlobalStateKey.toHex[IO]))

  /** Insert via the SAME `(slashingsKey, entryCodec)` shape the GSAM accept fold writes with. */
  private def insertEntry(store: MptStore[IO, GlobalStateKey], e: SlashedRegistryEntry)(implicit h: Hasher[IO]): IO[Unit] =
    GlobalStateKey
      .slashingsKey[IO](e.peerId, e.shardId, e.disputedCheckpointHash)
      .flatMap(k => store.insert[SlashedRegistryEntry](k, e)(InvalidStateProofSlashedReader.entryCodec))

  private def committeeWith(
    reader: SlashCooldownReader[IO],
    active: Set[PeerId] = null,
    epoch: EtaPeriod = epoch5,
    kDraw: Int = -1,
    kQuorum: Int = 2,
    registry: OperatorConsensusKeyRegistry[IO] = null
  )(
    implicit h: Hasher[IO],
    population: CanonicalOperatorConsensusPopulation
  ): IO[Set[PeerId]] = {
    val resolvedActive = Option(active).getOrElse(validators)
    val resolvedDraw = if (kDraw < 0) resolvedActive.size else kDraw
    val resolvedRegistry = Option(registry).getOrElse(operatorRegistry)
    ShardCheckpointWiring.committeeFor[IO](
      shardZero,
      epoch,
      IO.pure(resolvedActive),
      resolvedRegistry,
      etaForEpoch,
      resolvedDraw,
      kQuorum,
      reader
    )
  }

  /** A store-backed reader over a fresh MPT store holding `entries`, base-committed at `baseOrdinal`. */
  private def storeReader(entries: List[SlashedRegistryEntry], baseOrdinal: Long)(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(MptStore[IO, GlobalStateKey], SlashCooldownReader[IO])] =
    for {
      store <- mkStore
      _ <- entries.traverse_(insertEntry(store, _))
      _ <- store.commit(ord(baseOrdinal))
    } yield (store, SlashCooldownReader.fromMptStore[IO](store, R))

  // ═══ 1+3. THE EXCLUSION (RED→GREEN core) + σ-recompute ══════════════════════════════════════════════════════════

  test("GREEN (was RED): an operator with an unexpired cooldown is ABSENT from the committee draw") { res =>
    implicit val (h, sp, js, _, population) = res
    for {
      (_, reader) <- storeReader(List(entry(slashedPeer, eventOrd = 350L, cooldownUntil = 10000L)), baseOrdinal = 450L)
      committee <- committeeWith(reader)
      baseline <- committeeWith(SlashCooldownReader.noExclusion[IO]) // the pre-fix draw: slashed peer WAS drawn (the RED half)
    } yield
      expect.all(
        baseline.contains(slashedPeer), // documents the FINDING-002 behavior the gate removes
        !committee.contains(slashedPeer),
        committee == validators - slashedPeer // kDraw = N saturates over the post-exclusion pool ⇒ every ELIGIBLE operator drawn
      )
  }

  test("σ recompute: exclusion ≡ shrinking the active set — post-exclusion draw byte-identical to a draw over validators-minus-slashed") {
    res =>
      implicit val (h, sp, js, _, population) = res
      // Threshold-sensitive regime (mirrors the #261 suite): 20 validators, kDraw=6 ⇒ threshold 6/20=0.30 vs 6/19≈0.3158 — the draw
      // genuinely depends on the σ denominator, so this catches a fix that excludes AFTER drawing with the old σ = 1/N.
      val many: Set[PeerId] = population.peerIds
      val slashed: PeerId = many.toList.sorted.head
      val manyRegistry = population.operatorKeyRegistry
      for {
        (_, reader) <- storeReader(List(entry(slashed, eventOrd = 350L, cooldownUntil = 10000L)), baseOrdinal = 450L)
        viaExclusion <- committeeWith(reader, active = many, kDraw = 6, registry = manyRegistry)
        viaShrunkSet <- committeeWith(
          SlashCooldownReader.noExclusion[IO],
          active = many - slashed,
          kDraw = 6,
          registry = manyRegistry
        )
      } yield
        expect.all(
          !viaExclusion.contains(slashed),
          viaExclusion == viaShrunkSet // σ = 1/|post-exclusion pool| — the draw threshold's denominator shrank with the pool
        )
  }

  // ═══ 2. ATTESTATION ADMISSION through the production manager ══════════════════════════════════════════════════

  private def mkSigner(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    checkpointSigner: RegisteredCheckpointSigner
  ): IO[(KeyPair, PeerId)] =
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      peerId = PeerId.fromPublic(kp.getPublic)
      _ <- checkpointSigner.preregisterGenesis(kp, peerId)
    } yield (kp, peerId)

  private def mkValidSig(cp: ShardCheckpoint, kp: KeyPair, peerId: PeerId)(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    checkpointSigner: RegisteredCheckpointSigner
  ): IO[CommitteeMemberSignature] =
    checkpointSigner.sign(cp, kp, peerId)

  private def mkShell(placeholder: PeerId): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = Hash("0" * 64),
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = ord(500L),
      slot = SlotT.unsafeApply(100L),
      derivedStateDelta = ShardDerivedStateDelta.empty,
      committeeSignatures = NonEmptyList.of(CommitteeMemberSignature(placeholder, Hex(""), Hex(""), Hex(""), 0)),
      epoch = epoch5,
      executionBase = io.constellationnetwork.node.shared.ShardCheckpointTestFixtures.defaultExecutionBase
    )

  private def mkManager(membership: (ShardId, EtaPeriod) => IO[Set[PeerId]])(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    checkpointSigner: RegisteredCheckpointSigner
  ): IO[ShardCheckpointGl0AcceptanceManager[IO]] =
    ShardCheckpointGl0AcceptanceManager.make[IO](
      executionQuorum = 1,
      etaRotationSnapshots = R,
      committeeMembership = membership,
      operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
      shardAssignment = ShardAssignment.make[IO](numShards = 1),
      shardEtaFor = (_, _) => IO.pure(checkpointSigner.defaultShardEta.some),
      producerDutyValidator = TestCheckpointDutyValidator.allow[IO],
      reExecuteDerivation = (_, _, _, _) => IO.pure(Hash.empty)
    )

  test("a slashed signer's checkpoint attestation is rejected before it can enter the tracker") { res =>
    implicit val (h, sp, js, checkpointSigner, population) = res
    for {
      (kpSlashed, pSlashed) <- mkSigner
      (kpHonest1, pHonest1) <- mkSigner
      (_, pHonest2) <- mkSigner
      signers = Set(pSlashed, pHonest1, pHonest2)
      // The committee the manager's pre-check gates on = the REAL post-exclusion draw over the signer set.
      (_, reader) <- storeReader(List(entry(pSlashed, eventOrd = 350L, cooldownUntil = 10000L)), baseOrdinal = 450L)
      membership = (_: ShardId, epoch: EtaPeriod) =>
        committeeWith(
          reader,
          active = signers,
          epoch = epoch,
          kDraw = signers.size,
          registry = checkpointSigner.operatorKeyRegistry
        )
      manager <- mkManager(membership)

      shell = mkShell(pSlashed)
      sigSlashed <- mkValidSig(shell, kpSlashed, pSlashed)
      sigHonest1 <- mkValidSig(shell, kpHonest1, pHonest1)

      slashedValidation <- manager.verifyCommitteeSignature(shell, sigSlashed)
      honestValidation <- manager.verifyCommitteeSignature(shell, sigHonest1)
    } yield
      expect.all(
        slashedValidation.left.exists(_.contains("not in committee")),
        honestValidation.isRight
      )
  }

  // ═══ 4. CLUSTER-UNIFORMITY (the anti-fork invariant) ════════════════════════════════════════════════════════════

  test(
    "cluster-uniformity: two nodes, same pinned records — different insert order / base ordinal / unrelated state ⇒ same excluded set, committee, and admission verdict"
  ) { res =>
    implicit val (h, sp, js, checkpointSigner, population) = res
    val e1 = entry(slashedPeer, eventOrd = 350L, cooldownUntil = 10000L, cpHash = Hash("aa" * 32))
    val e2 = entry(validators.toList.sorted.apply(1), eventOrd = 360L, cooldownUntil = 10000L, cpHash = Hash("bb" * 32))
    for {
      // Node A: [e1, e2] at base 450. Node B: [e2, e1] at base 480 + unrelated fieldId-33 bytes (different local store content
      // outside the Slashings partition must not leak into the exclusion).
      (storeA, readerA) <- storeReader(List(e1, e2), baseOrdinal = 450L)
      (storeB0, _) <- storeReader(List(e2, e1), baseOrdinal = 479L)
      _ <- storeB0.insert[String](GlobalStateKey.consumedAllowSpendKey(Hash("cc" * 32)), "unrelated")(
        new io.constellationnetwork.serde.ImmutableCodec[String] {
          def immutableBytes(v: String) = scodec.bits.ByteVector.view(v.getBytes)
          def fromImmutableBytes(b: scodec.bits.ByteVector) = Right(new String(b.toArray))
        }
      )
      _ <- storeB0.commit(ord(480L))
      readerB = SlashCooldownReader.fromMptStore[IO](storeB0, R)

      exclusionA <- readerA.excludedForEpoch(epoch5)
      exclusionB <- readerB.excludedForEpoch(epoch5)
      committeeA <- committeeWith(readerA)
      committeeB <- committeeWith(readerB)

      // Same embedded envelope through both nodes' managers → identical verdicts.
      (kpX, pX) <- mkSigner
      signerSet = validators + pX
      // Register the non-signing baseline validators as complete genesis pairs too, then merge pX's real runtime pair.
      baselineRegistry = population.operatorKeyRegistry
      baselinePairs <- baselineRegistry.list
      signerPairs <- checkpointSigner.operatorKeyRegistry.list
      signerRegistry = OperatorConsensusKeyRegistry.make[IO](baselinePairs ++ signerPairs)
      membershipA = (_: ShardId, ep: EtaPeriod) =>
        committeeWith(readerA, active = signerSet, epoch = ep, kDraw = signerSet.size, registry = signerRegistry)
      membershipB = (_: ShardId, ep: EtaPeriod) =>
        committeeWith(readerB, active = signerSet, epoch = ep, kDraw = signerSet.size, registry = signerRegistry)
      managerA <- mkManager(membershipA)
      managerB <- mkManager(membershipB)
      shell = mkShell(pX)
      sigX <- mkValidSig(shell, kpX, pX)
      cp = shell.copy(committeeSignatures = NonEmptyList.of(sigX))
      verdictA <- managerA.verifyCommitteeSignature(cp, sigX)
      verdictB <- managerB.verifyCommitteeSignature(cp, sigX)
    } yield
      expect.all(
        exclusionA.candidates == exclusionB.candidates,
        exclusionA.anchorSettled && exclusionB.anchorSettled,
        committeeA == committeeB,
        verdictA == verdictB,
        verdictA.isRight // pX is eligible on both — sanity that the attestation is admissible at both nodes
      )
  }

  // ═══ 5. EPOCH STAGGERING + EXPIRY ═══════════════════════════════════════════════════════════════════════════════

  test("epoch staggering: a record YOUNGER than the epoch's anchor does not bite that epoch — it bites the next one (Cardano N-2 rule)") {
    res =>
      implicit val (h, sp, js, _, population) = res
      // anchor(5) = 399, anchor(6) = 499. eventOrdinal 420 ∈ (399, 499] ⇒ eligible at epoch 5, excluded at epoch 6.
      for {
        (_, reader) <- storeReader(List(entry(slashedPeer, eventOrd = 420L, cooldownUntil = 10000L)), baseOrdinal = 550L)
        at5 <- committeeWith(reader, epoch = EtaPeriod(5L))
        at6 <- committeeWith(reader, epoch = EtaPeriod(6L))
      } yield
        expect.all(
          at5.contains(slashedPeer),
          !at6.contains(slashedPeer)
        )
  }

  test("cooldown expiry: once cooldownUntilEpoch is at-or-below the epoch's anchor the operator is drawn again") { res =>
    implicit val (h, sp, js, _, population) = res
    // cooldownUntilEpoch = 399 = anchor(5) ⇒ NOT active at epoch 5 (strict >); was active at epoch 4 (anchor 299).
    for {
      (_, reader) <- storeReader(List(entry(slashedPeer, eventOrd = 250L, cooldownUntil = 399L)), baseOrdinal = 450L)
      at4 <- committeeWith(reader, epoch = EtaPeriod(4L))
      at5 <- committeeWith(reader, epoch = EtaPeriod(5L))
    } yield
      expect.all(
        !at4.contains(slashedPeer),
        at5.contains(slashedPeer)
      )
  }

  // ═══ 6. DEGENERATE FLOOR (Polkadot UpToLimit) ═══════════════════════════════════════════════════════════════════

  test("degenerate: exclusion never drops the eligible pool below kQuorum — everyone-slashed at |active| <= kQuorum excludes nobody") {
    res =>
      implicit val (h, sp, js, _, population) = res
      val four: Set[PeerId] = population.peerIds.toList.sorted.take(4).toSet
      val fourRegistry = population.operatorKeyRegistry
      val allSlashed = four.toList.sorted.zipWithIndex.map {
        case (p, i) => entry(p, eventOrd = 300L + i, cooldownUntil = 10000L, cpHash = Hash(f"$i%02x" * 32))
      }
      for {
        (_, reader) <- storeReader(allSlashed, baseOrdinal = 450L)
        // |active| = 4 = kQuorum ⇒ maxExcludable = 0 ⇒ nobody excluded (liveness over exclusion, documented)
        atFloor <- committeeWith(reader, active = four, kDraw = 4, kQuorum = 4, registry = fourRegistry)
        // kQuorum = 2 ⇒ maxExcludable = 2 ⇒ the two OLDEST slashes (peers 0,1) excluded; newest offenders (2,3) escape
        partial <- committeeWith(reader, active = four, kDraw = 4, kQuorum = 2, registry = fourRegistry)
      } yield
        expect.all(
          atFloor == four,
          partial == four.toList.sorted.drop(2).toSet,
          partial.size == 2 // never below the kQuorum floor
        )
  }

  // ═══ 7. CACHE DISCIPLINE (acceptanceDeps committee cache) ═══════════════════════════════════════════════════════

  private def mkShardingConfig(numShards: Int): ShardingConfig =
    ShardingConfig(
      numShards = numShards,
      retention = ShardCheckpointRetentionConfig(retainedCheckpoints = 8L),
      checkpoint = ShardCheckpointConfig(binaryBufferCap = 4096)
    )

  /** Mutable stub reader: exclusion + settledness read from a Ref, so the test can change what the store "contains" between calls. */
  private def refReader(ref: Ref[IO, (List[(PeerId, Long)], Boolean)]): SlashCooldownReader[IO] =
    (_: EtaPeriod) => ref.get.map { case (cands, settled) => SlashCooldownReader.EpochExclusion(cands, settled) }

  test("cache discipline: an anchor-UNSETTLED draw is not memoized (recomputed later, exact); a SETTLED draw is memoized") { res =>
    implicit val (h, sp, js, _, population) = res
    for {
      unsettledRef <- Ref.of[IO, (List[(PeerId, Long)], Boolean)]((List.empty, false))
      settledRef <- Ref.of[IO, (List[(PeerId, Long)], Boolean)]((List.empty, true))
      depsUnsettled <- ShardCheckpointWiring.acceptanceDeps[IO](
        cfg = mkShardingConfig(numShards = 2),
        etaRotationSnapshots = R,
        kDraw = validators.size,
        kQuorum = 2,
        selfPeerId = slashedPeer,
        operatorKeyRegistry = operatorRegistry,
        activeValidators = IO.pure(validators),
        etaForEpoch = etaForEpoch,
        slashCooldownReader = Some(refReader(unsettledRef))
      )
      depsSettled <- ShardCheckpointWiring.acceptanceDeps[IO](
        cfg = mkShardingConfig(numShards = 2),
        etaRotationSnapshots = R,
        kDraw = validators.size,
        kQuorum = 2,
        selfPeerId = slashedPeer,
        operatorKeyRegistry = operatorRegistry,
        activeValidators = IO.pure(validators),
        etaForEpoch = etaForEpoch,
        slashCooldownReader = Some(refReader(settledRef))
      )
      du = depsUnsettled.getOrElse(throw new AssertionError("expected Some deps at numShards=2"))
      ds = depsSettled.getOrElse(throw new AssertionError("expected Some deps at numShards=2"))

      // UNSETTLED: first draw sees no exclusion; the slash "lands" (ref update); the SECOND draw must reflect it (not cached).
      first <- du.committeeMembership(shardZero, epoch5)
      _ <- unsettledRef.set((List((slashedPeer, 350L)), false))
      second <- du.committeeMembership(shardZero, epoch5)

      // SETTLED: first draw memoizes; a later ref change must NOT alter the epoch's committee (pure function of (shard, epoch)).
      firstS <- ds.committeeMembership(shardZero, epoch5)
      _ <- settledRef.set((List((slashedPeer, 350L)), true))
      secondS <- ds.committeeMembership(shardZero, epoch5)
    } yield
      expect.all(
        first.contains(slashedPeer),
        !second.contains(slashedPeer), // unsettled result was NOT pinned — the later, complete view wins
        firstS.contains(slashedPeer),
        secondS == firstS // settled result IS pinned for the epoch — stable committee per (shardId, epoch)
      )
  }

  // ═══ 8. NO-OP EQUIVALENCE (byte-identity bars) ══════════════════════════════════════════════════════════════════

  test("no-op: an EMPTY Slashings partition draws byte-identically to the pre-fix code (noExclusion) — the numShards>1 empty bar") { res =>
    implicit val (h, sp, js, _, population) = res
    for {
      (_, reader) <- storeReader(List.empty, baseOrdinal = 450L)
      withEmptyPartition <- committeeWith(reader)
      preFix <- committeeWith(SlashCooldownReader.noExclusion[IO])
    } yield expect(withEmptyPartition == preFix)
  }

  test("numShards=1 regression bar: acceptanceDeps stays None — the exclusion gate (like everything else) is never constructed") { res =>
    implicit val (h, sp, js, _, population) = res
    for {
      store <- mkStore
      deps <- ShardCheckpointWiring.acceptanceDeps[IO](
        cfg = mkShardingConfig(numShards = 1),
        etaRotationSnapshots = R,
        kDraw = 4,
        kQuorum = 2,
        selfPeerId = slashedPeer,
        operatorKeyRegistry = operatorRegistry,
        activeValidators = IO.pure(validators),
        etaForEpoch = etaForEpoch,
        slashCooldownReader = Some(SlashCooldownReader.fromMptStore[IO](store, R))
      )
    } yield expect(deps.isEmpty)
  }

  // ═══ 9. EPIC-3.4 — REORG DURABILITY (leverages the landed S01/S02 preservation) ═════════════════════════════════

  test("EPIC-3.4: a slashed-then-reorged peer STAYS excluded — the fieldId-34 record and the exclusion survive a GSI base rebuild") { res =>
    implicit val (h, sp, js, _, population) = res
    for {
      // A base with ordinary GSI-native state (a balance) + the slash record committed at a finalized ordinal ≥ anchor(5).
      srcKp <- KeyPairGenerator.makeKeyPair[IO]
      srcAddr = {
        import io.constellationnetwork.security.key.ops.PublicKeyOps
        srcKp.getPublic.toAddress
      }
      gsi = GlobalSnapshotInfo.empty.copy(
        balances = SortedMap(srcAddr -> io.constellationnetwork.schema.balance.Balance(NonNegLong.unsafeFrom(1000L)))
      )
      store <- mkStore
      _ <- store.syncFromGlobalSnapshotInfo(gsi, ord(430L))
      slashEntry = entry(slashedPeer, eventOrd = 350L, cooldownUntil = 10000L)
      _ <- insertEntry(store, slashEntry)
      _ <- store.commit(ord(450L))
      reader = SlashCooldownReader.fromMptStore[IO](store, R)
      slashKeyHex <- GlobalStateKey
        .slashingsKey[IO](slashEntry.peerId, slashEntry.shardId, slashEntry.disputedCheckpointHash)
        .flatMap(GlobalStateKey.toHex[IO])

      committeeBefore <- committeeWith(reader)
      exclusionBefore <- reader.excludedForEpoch(epoch5)

      // ===== THE REORG: a `(k₁, head]` fork-switch self-heal rebuilds the base from the carried GSI (the S01 wipe path,
      // now preservation-fixed by fc90f687e/fe6d837b1 — fieldId 33/34 survive the rebuild verbatim).
      _ <- store.syncFromGlobalSnapshotInfo(gsi, ord(460L))

      entriesAfter <- store.allEntriesAsBytes
      committeeAfter <- committeeWith(reader)
      exclusionAfter <- reader.excludedForEpoch(epoch5)
    } yield
      expect.all(
        !committeeBefore.contains(slashedPeer),
        // (1) the raw fieldId-34 record survived the rebuild (the EPIC-1/S02 preservation this task leverages)
        entriesAfter.contains(slashKeyHex),
        // (2) the EXCLUSION still applies after the rebuild — the slash keeps biting across the reorg (EPIC-3.4)
        exclusionAfter.candidates == exclusionBefore.candidates,
        !committeeAfter.contains(slashedPeer),
        committeeAfter == committeeBefore
      )
  }
}
