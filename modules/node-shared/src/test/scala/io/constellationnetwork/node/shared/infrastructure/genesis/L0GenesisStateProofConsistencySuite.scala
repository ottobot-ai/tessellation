package io.constellationnetwork.node.shared.infrastructure.genesis

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.schema.GlobalSnapshot
import io.constellationnetwork.schema.delegatedStake.{
  DelegatedStakeAmount,
  DelegatedStakeFee,
  DelegatedStakeReference,
  UpdateDelegatedStake
}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.{GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Verifies the two genesis-path invariants that together resolve the stateProof / GSI / MPT consistency bug observed via
  * `iter-v25c-s0-validation.log` (slot=3 diffs on `stateProof[delegStakes,mptRoot]`).
  *
  *   1. (Within-node) The ord=1 `GlobalIncrementalSnapshot.stateProof` is computed from the same (post-augmentation) GSI that the storage
  *      layer + MPT will hold at boot. Achieved by passing `augmentedInfo` to `GlobalSnapshot.mkFirstIncrementalSnapshot`.
  *
  * 2. (Cross-node) The `Signed[UpdateDelegatedStake.Create]` bytes produced by `L0GenesisLoader.signedDeterministic` are byte-identical
  * across loads of the same fixture. Without this the `activeDelegatedStakes` MPT leaves diverge cross-node, and so do the
  * `stateProof.mptRoot`s.
  */
object L0GenesisStateProofConsistencySuite extends MutableIOSuite {
  type Res = (KryoSerializer[IO], JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](nodeSharedKryoRegistrar)
    sp <- SecurityProvider.forAsync[IO]
    implicit0(js: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (ks, js, h, sp)

  // Withdrawal time limit + selector required by the stateProof path. Use the
  // MerklePatricia branch (lastLegacyStateProofOrdinal=0) so the assertion exercises the MPT-aware
  // path, which is what the boot flow uses in production.
  private implicit val selector: GlobalStateProofSelector =
    new GlobalStateProofSelector(SnapshotOrdinal.MinValue)
  private implicit val wtl: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  // Build a minimal in-memory L0GenesisData with one delegated-stake record. The exact addresses /
  // amounts don't matter for the consistency assertion — only that the augmenter produces a GSI
  // that differs from the pre-aug GSI, so we can detect the bug.
  private def buildFixture[F[_]: cats.effect.Async: Hasher: SecurityProvider]: F[L0GenesisData] =
    for {
      delegatorKp <- KeyPairGenerator.makeKeyPair[F]
      operatorKp <- KeyPairGenerator.makeKeyPair[F]
      delegatorAddr = delegatorKp.getPublic.toAddress
      operatorPeerId = io.constellationnetwork.schema.peer.PeerId.fromPublic(operatorKp.getPublic)
      privHex = Hex.fromBytes(delegatorKp.getPrivate.getEncoded).value
      event = UpdateDelegatedStake.Create(
        source = delegatorAddr,
        nodeId = operatorPeerId,
        amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(100L)),
        fee = DelegatedStakeFee(NonNegLong(0L)),
        tokenLockRef = Hash.fromBytes("test-stateproof-consistency".getBytes("UTF-8")),
        parent = DelegatedStakeReference.empty
      )
    } yield
      L0GenesisData(
        _meta = L0GenesisMeta(
          generatorVersion = "test",
          generatedAt = "1970-01-01T00:00:00Z",
          invocation = "stateproof-consistency",
          seed = 0L,
          expectedProperties = Nil
        ),
        networkMagic = "test",
        activationOrdinal = 0L,
        startingEpochProgress = 0L,
        protocolParams = L0GenesisProtocolParams.default,
        operators = Nil,
        delegatedStakes = List(
          L0GenesisDelegatedStake(
            event = event,
            delegatorPrivateKeyHex = privHex,
            createdAt = 0L,
            rewards = 0L
          )
        ),
        nodeCollaterals = Nil,
        initialBalances = Nil
      )

  test("Issue A: mkFirstIncrementalSnapshot stateProof matches the augmented GSI's stateProof") { res =>
    implicit val (_, js, h, sp) = res

    for {
      data <- buildFixture[IO]
      // 1) build genesis snapshot from the (initial-balance only) balance map.
      balanceMap = data.initialBalanceMap
      genesisSnapshot = GlobalSnapshot.mkGenesis(balanceMap, EpochProgress(NonNegLong(0L)))
      kp <- KeyPairGenerator.makeKeyPair[IO]
      hashedGenesis <- io.constellationnetwork.security.signature.Signed
        .forAsyncHasher[IO, GlobalSnapshot](genesisSnapshot, kp)
        .flatMap(_.toHashed[IO])

      // 2) Pre-augmentation GSI (what the OLD code passed into mkFirstIncrementalSnapshot).
      baseGsi = hashedGenesis.info.toGlobalSnapshotInfo

      // 3) Augmented GSI (what the storage layer + MPT see at boot).
      augmentedGsi <- L0GenesisLoader.augmentSnapshotInfo[IO](baseGsi, data)

      // 4) Compute stateProof FROM augmented GSI directly (the source of truth at boot).
      independentProof <- augmentedGsi.stateProof[IO](hashedGenesis.ordinal)

      // 5) Build the ord=1 incremental snapshot via the NEW two-arg overload.
      firstIncr <- GlobalSnapshot.mkFirstIncrementalSnapshot[IO](hashedGenesis, augmentedGsi)

      // 6) Build the ord=1 incremental snapshot via the OLD pre-aug path (single-arg overload),
      //    so we can prove (a) the new path is consistent and (b) the old path was inconsistent.
      legacyIncr <- GlobalSnapshot.mkFirstIncrementalSnapshot[IO](hashedGenesis)

      preAugProof <- baseGsi.stateProof[IO](hashedGenesis.ordinal)
    } yield
      // New (fixed) path: snapshot.stateProof == augmentedGsi.stateProof.
      expect.same(firstIncr.stateProof, independentProof) &&
        // Sanity: legacy path (single-arg overload) sees the pre-aug GSI's stateProof — confirms
        // the bug exists in the legacy code, and that our augmenter actually changed the GSI.
        expect.same(legacyIncr.stateProof, preAugProof) &&
        // And those two proofs DIFFER (because the augmenter populated activeDelegatedStakes).
        expect(firstIncr.stateProof != legacyIncr.stateProof)
  }

  test("Issue B: signedDeterministic produces byte-identical Signed[Event] across loads") { res =>
    implicit val (_, _, h, sp) = res

    for {
      data <- buildFixture[IO]
      kp <- L0GenesisLoader.keyPairFromHex[IO](data.delegatedStakes.head.delegatorPrivateKeyHex)
      ev = data.delegatedStakes.head.event
      // Two independent invocations of the deterministic signer over the SAME (key, event) must
      // produce byte-identical Signed[Event]. The old non-deterministic path (Signed.forAsyncHasher)
      // does not satisfy this — RNG-driven k → different (r, s) → different signature bytes.
      a <- L0GenesisLoader.signedDeterministic[IO, UpdateDelegatedStake.Create](ev, kp)
      b <- L0GenesisLoader.signedDeterministic[IO, UpdateDelegatedStake.Create](ev, kp)
      // The single-proof signature hex must match (newtype unwraps via `coerce` to Hex).
      sigA = a.proofs.head.signature.coerce.value
      sigB = b.proofs.head.signature.coerce.value
    } yield expect.same(sigA, sigB) && expect.same(a, b)
  }

  test("Issue B: cross-node consistency — two GSIs built via augmentSnapshotInfo are byte-equal") { res =>
    implicit val (_, _, h, sp) = res

    // Simulates two nodes loading the same fixture: each calls augmentSnapshotInfo independently.
    // With the old (non-deterministic) path the Signed[Event] bytes differ → activeDelegatedStakes
    // MPT leaves differ → field root differs → stateProof.mptRoot differs cross-node. With the
    // deterministic path both nodes produce byte-equal GSIs.
    for {
      data <- buildFixture[IO]
      balanceMap = data.initialBalanceMap
      genesisSnapshot = GlobalSnapshot.mkGenesis(balanceMap, EpochProgress(NonNegLong(0L)))
      kp <- KeyPairGenerator.makeKeyPair[IO]
      hashedGenesis <- io.constellationnetwork.security.signature.Signed
        .forAsyncHasher[IO, GlobalSnapshot](genesisSnapshot, kp)
        .flatMap(_.toHashed[IO])
      baseGsi = hashedGenesis.info.toGlobalSnapshotInfo
      gsiNode1 <- L0GenesisLoader.augmentSnapshotInfo[IO](baseGsi, data)
      gsiNode2 <- L0GenesisLoader.augmentSnapshotInfo[IO](baseGsi, data)
      // And their stateProofs match (the cross-node invariant that was broken before B fix).
      proof1 <- gsiNode1.stateProof[IO](hashedGenesis.ordinal)
      proof2 <- gsiNode2.stateProof[IO](hashedGenesis.ordinal)
    } yield expect.same(gsiNode1, gsiNode2) && expect.same(proof1, proof2)
  }
}
