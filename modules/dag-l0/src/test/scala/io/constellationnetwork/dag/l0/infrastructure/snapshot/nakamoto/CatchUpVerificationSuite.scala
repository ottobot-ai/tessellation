package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.data.NonEmptyList
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.CanonicalOperatorConsensusFixture
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.vrf.EcVrf25519

import com.google.protobuf.ByteString
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.syntax._
import weaver.MutableIOSuite

/** Regression tests for authenticated parent-first ChainSync recovery.
  *
  * Signature plus GSI/root self-consistency is not transition validity. A successful result may be cached or logged, but runtime recovery
  * must fetch ancestry and replay before chain storage, MPT writes, attestation, or finality.
  *
  *   1. '''one payload shape''' — wrapped snapshots decode with or without optional context; fork-only bare payloads are rejected.
  *   1. '''transport binding''' — hash, ordinal, parent, producer, slot, VRF, and eta must equal the signed body/certificate.
  *   1. '''missing parent → buffer only''' — no MPT write, chain-store write, or attestation callback can run until fetched ancestry has
  *      replayed parent-first through the normal validation gate.
  *   1. '''invalid middle ancestor → descendants blocked''' — an invalid fetched ancestor is not committed and cannot trigger the drain for
  *      children waiting on its hash.
  */
object CatchUpVerificationSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  type Res = (KryoSerializer[IO], JsonSerializer[IO], Hasher[IO], SecurityProvider[IO], HasherSelector[IO])

  def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](nodeSharedKryoRegistrar)
    implicit0(sp: SecurityProvider[IO]) <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
    hs = HasherSelector.forSyncAlwaysCurrent(h)
  } yield (ks, j, h, sp, hs)

  test("GL0 leader identity accepts only the producer's preregistered atomic KES+VRF pair") { _ =>
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val producerId = operator.resolvedPair.operatorPeerId
      val unknownProducerId = PeerId(Hex("22" * 32))
      val registeredKey = operator.resolvedPair.vrfPublicKey.toBytes
      val replacementKeys = (1 to 128).toList.map { attempt =>
        registeredKey.indices.map(i => (registeredKey(i) + attempt).toByte).toArray
      }

      for {
        accepted <- NakamotoSnapshotValidator.resolveRegisteredOperatorKeys(
          operator.operatorKeyRegistry,
          producerId,
          registeredKey,
          EtaPeriod.Zero
        )
        replacements <- replacementKeys.traverse { replacement =>
          NakamotoSnapshotValidator.resolveRegisteredOperatorKeys(
            operator.operatorKeyRegistry,
            producerId,
            replacement,
            EtaPeriod.Zero
          )
        }
        missing <- NakamotoSnapshotValidator.resolveRegisteredOperatorKeys(
          operator.operatorKeyRegistry,
          unknownProducerId,
          registeredKey,
          EtaPeriod.Zero
        )
        malformed <- NakamotoSnapshotValidator.resolveRegisteredOperatorKeys(
          operator.operatorKeyRegistry,
          producerId,
          Array.fill[Byte](31)(1),
          EtaPeriod.Zero
        )
      } yield
        expect.all(
          accepted.exists(keys => java.security.MessageDigest.isEqual(keys.vrfPublicKey.toBytes, registeredKey)),
          replacements.forall(_.isEmpty),
          missing.isEmpty,
          malformed.isEmpty
        )
    }
  }

  /** A GlobalSnapshotInfo carrying `balances` so the rebuilt state proof is non-trivial (`balancesProof` is a real MPT subtree root). */
  private def mkInfo(balances: SortedMap[Address, Balance]): GlobalSnapshotInfo =
    GlobalSnapshotInfo(
      lastStateChannelSnapshotHashes = SortedMap.empty,
      lastTxRefs = SortedMap.empty,
      balances = balances,
      lastCurrencySnapshots = SortedMap.empty,
      lastCurrencySnapshotsProofs = SortedMap.empty,
      activeAllowSpends = None,
      activeTokenLocks = None,
      tokenLockBalances = None,
      lastAllowSpendRefs = None,
      lastTokenLockRefs = None,
      updateNodeParameters = None,
      activeDelegatedStakes = Some(SortedMap.empty),
      delegatedStakesWithdrawals = Some(SortedMap.empty),
      activeNodeCollaterals = Some(SortedMap.empty),
      nodeCollateralWithdrawals = Some(SortedMap.empty),
      priceState = Some(SortedMap.empty),
      metagraphSyncData = Some(SortedMap.empty),
      historicalStakeSnapshots = SortedMap.empty
    )

  /** Build a GlobalIncrementalSnapshot whose `stateProof` is computed from `info` (so an honest pairing passes gate 2). NOT yet signed. */
  private def mkSnapshot(info: GlobalSnapshotInfo)(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[GlobalIncrementalSnapshot] =
    info.stateProof[IO](SnapshotOrdinal(NonNegLong(1L))).map { sp =>
      GlobalIncrementalSnapshot(
        SnapshotOrdinal(NonNegLong(1L)),
        Height.MinValue,
        SubHeight.MinValue,
        Hash.empty,
        SortedSet.empty,
        SortedMap.empty,
        SortedMap.empty[io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint],
        SortedSet.empty,
        None,
        EpochProgress.MinValue,
        NonEmptyList.of(PeerId(Hex(""))),
        SnapshotTips(SortedSet.empty, SortedSet.empty),
        stateProof = sp,
        Some(SortedSet.empty),
        Some(SortedSet.empty),
        Some(SortedMap.empty),
        Some(SortedMap.empty),
        Some(SortedSet.empty),
        Some(SortedMap.empty),
        Some(SortedMap.empty),
        Some(SortedMap.empty),
        Some(SortedMap.empty)
      )
    }

  // Two distinct valid DAG addresses (concrete values irrelevant; only their presence in `balances` matters for the rebuilt proof).
  private val addrA: Address = Address("DAG2FGeUYivtEo9EjvpELY4ZS7zDQWvJzQYVzXkX")
  private val vrf = new EcVrf25519()

  private def mkCertifiedEnvelope(
    info: GlobalSnapshotInfo,
    keyPair: java.security.KeyPair
  )(
    implicit h: Hasher[IO],
    j: JsonSerializer[IO],
    sp: SecurityProvider[IO]
  ): IO[(Signed[GlobalIncrementalSnapshot], pb.Snapshot)] = {
    val vrfSk = Array.tabulate[Byte](32)(i => (i + 1).toByte)
    val vrfPk = vrf.getVerificationKey(vrfSk)
    val proof = vrf.vrfProof(vrfSk, "snapshot-envelope".getBytes(java.nio.charset.StandardCharsets.UTF_8))
    val output = vrf.vrfProofToHash(proof).getOrElse(throw new IllegalStateException("test VRF proof did not derive output"))
    val etaBytes = Array.fill[Byte](32)(0x5a.toByte)
    val etaHash = Hash(Hex.fromBytes(etaBytes).value)
    val slot = Slot.unsafeApply(10L)
    val parentSlot = Slot.unsafeApply(9L)

    for {
      base <- mkSnapshot(info)
      certified = base.copy(
        slotCertificate = Some(
          SlotCertificate(
            slot,
            parentSlot,
            VrfProof.fromBytes(proof),
            VrfOutput.fromBytes(output),
            VrfPublicKey.fromBytes(vrfPk),
            etaHash,
            activePoolSize = 1,
            activePoolHash = Hash.empty,
            subchainLevelCounts = SlotCertificate.ZeroSubchainLevelCounts
          )
        ),
        eta = Some(etaHash)
      )
      signed <- forAsyncHasher(certified, keyPair)
      hashed <- signed.toHashed[IO]
      payload = io.circe.Json
        .obj("snapshot" -> signed.asJson, "context" -> info.asJson)
        .noSpaces
        .getBytes(java.nio.charset.StandardCharsets.UTF_8)
    } yield {
      val envelope = pb.Snapshot(
        hash = ByteString.copyFrom(hashed.hash.value.getBytes),
        slot = slot.value.value,
        ordinal = certified.ordinal.value.value,
        parentHash = ByteString.copyFrom(certified.lastSnapshotHash.value.getBytes),
        vrfProof = ByteString.copyFrom(proof),
        vrfPublicKey = ByteString.copyFrom(vrfPk),
        eta = ByteString.copyFrom(etaBytes),
        payload = ByteString.copyFrom(payload),
        producerId = ByteString.copyFrom(signed.proofs.head.id.hex.toBytes),
        parentSlot = parentSlot.value.value,
        kesSignature = ByteString.copyFrom(Array[Byte](1, 2, 3))
      )
      (signed, envelope)
    }
  }

  test("chain-sync decoder accepts the single wrapped shape and rejects bare snapshots") { res =>
    implicit val (_, j, h, sp, _) = res
    val info = mkInfo(SortedMap(addrA -> Balance.empty))
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      snapshot <- mkSnapshot(info)
      signed <- forAsyncHasher(snapshot, keyPair)
      bare = signed.asJson.noSpaces.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      wrapped = io.circe.Json
        .obj("snapshot" -> signed.asJson, "context" -> info.asJson)
        .noSpaces
        .getBytes(java.nio.charset.StandardCharsets.UTF_8)
      wrappedWithoutContext = io.circe.Json
        .obj("snapshot" -> signed.asJson)
        .noSpaces
        .getBytes(java.nio.charset.StandardCharsets.UTF_8)
      decodedWrapped = NakamotoSyncDaemon.decodeFetchedSnapshotPayload(wrapped)
      decodedWithoutContext = NakamotoSyncDaemon.decodeFetchedSnapshotPayload(wrappedWithoutContext)
    } yield
      expect.all(
        NakamotoSyncDaemon.decodeFetchedSnapshotPayload(bare).isEmpty,
        decodedWrapped.exists(_.value === snapshot),
        decodedWithoutContext.exists(_.value === snapshot),
        NakamotoSyncDaemon.decodeFetchedSnapshotPayload("not-json".getBytes).isEmpty
      )
  }

  private final case class ReplayNode(
    name: String,
    hash: Hash,
    validation: NakamotoSnapshotValidator.ValidationResult
  )

  test("RTA-004: every failed authentication or replay result has zero authority effects") { _ =>
    val failures: List[NakamotoSnapshotValidator.ValidationResult] = List(
      NakamotoSnapshotValidator.ParentNotFound,
      NakamotoSnapshotValidator.ParentBuffered,
      NakamotoSnapshotValidator.VrfFailed(slot = 7L, detail = "ineligible"),
      NakamotoSnapshotValidator.SignatureInvalid(ordinal = 8L),
      NakamotoSnapshotValidator.KesInvalid(ordinal = 9L),
      NakamotoSnapshotValidator.HistoricalEtaUnavailable(period = 2L, parentHash = Hash.empty),
      NakamotoSnapshotValidator.ContentMismatch("envelope/lineage/context/state-proof/root mismatch"),
      NakamotoSnapshotValidator.PayloadMissing(ordinal = 10L)
    )

    for {
      effects <- Ref.of[IO, List[String]](List.empty)
      committed <- failures.zipWithIndex.traverse {
        case (failure, index) =>
          NakamotoSyncDaemon.commitReplayValidated[IO](failure) { _ =>
            effects.update(
              _ ++ List(
                s"store:$index",
                s"sign:$index",
                s"publish:$index",
                s"tracker:$index"
              )
            )
          }
      }
      observed <- effects.get
    } yield expect.all(committed.forall(result => !result), observed.isEmpty)
  }

  test("snapshot transport metadata is bound to the signed body and certificate") { res =>
    implicit val (_, j, h, sp, hs) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      tuple <- mkCertifiedEnvelope(mkInfo(SortedMap(addrA -> Balance(NonNegLong(100L)))), keyPair)
      (signed, envelope) = tuple
      valid <- NakamotoSyncDaemon.validateSnapshotEnvelope[IO](envelope, signed)
      wrongOrdinal <- NakamotoSyncDaemon.validateSnapshotEnvelope[IO](envelope.copy(ordinal = envelope.ordinal + 1L), signed)
      wrongParent <- NakamotoSyncDaemon.validateSnapshotEnvelope[IO](
        envelope.copy(parentHash = ByteString.copyFromUtf8("f" * 64)),
        signed
      )
      wrongProducer <- NakamotoSyncDaemon.validateSnapshotEnvelope[IO](
        envelope.copy(producerId = ByteString.copyFrom(Array.fill[Byte](32)(9))),
        signed
      )
      wrongEta <- NakamotoSyncDaemon.validateSnapshotEnvelope[IO](
        envelope.copy(eta = ByteString.copyFrom(Array.fill[Byte](32)(7))),
        signed
      )
    } yield expect.all(valid.isRight, wrongOrdinal.isLeft, wrongParent.isLeft, wrongProducer.isLeft, wrongEta.isLeft)
  }

  test("slot lineage uses the retained parent and bounds embedded checkpoint slots") { _ =>
    def certificate(slot: Long, parentSlot: Long): SlotCertificate =
      SlotCertificate(
        slot = Slot.unsafeApply(slot),
        parentSlot = Slot.unsafeApply(parentSlot),
        vrfProof = VrfProof(Hex("")),
        vrfOutput = VrfOutput(Hex("")),
        vrfPublicKey = VrfPublicKey(Hex("")),
        eta = Hash.empty,
        activePoolSize = 1,
        activePoolHash = Hash.empty,
        subchainLevelCounts = SlotCertificate.ZeroSubchainLevelCounts
      )

    val shardZero = io.constellationnetwork.schema.sharding.ShardId(0).get
    val retainedParent = certificate(slot = 9L, parentSlot = 8L)
    val child = certificate(slot = 10L, parentSlot = 9L)
    val forgedGap = child.copy(parentSlot = Slot.unsafeApply(0L))
    val nonMonotone = child.copy(slot = Slot.unsafeApply(9L))

    IO.pure(
      expect.all(
        NakamotoSnapshotValidator.validateSlotLineage(child, retainedParent.some, List(shardZero -> Slot.unsafeApply(10L))).isRight,
        NakamotoSnapshotValidator.validateSlotLineage(forgedGap, retainedParent.some, List.empty).isLeft,
        NakamotoSnapshotValidator.validateSlotLineage(nonMonotone, retainedParent.some, List.empty).isLeft,
        NakamotoSnapshotValidator
          .validateSlotLineage(child, retainedParent.some, List(shardZero -> Slot.unsafeApply(11L)))
          .isLeft
      )
    )
  }

  test("producer slot-lineage rejection cleans up without signing or terminating later slot work") { _ =>
    for {
      cleanupCount <- Ref.of[IO, Int](0)
      signingCount <- Ref.of[IO, Int](0)
      invalid <- SnapshotLeaderLoop.afterProducerSlotLineageValidation[IO, String](Left("future checkpoint slot"))(_ =>
        cleanupCount.update(_ + 1)
      )(
        signingCount.update(_ + 1).as("signed")
      )
      valid <- SnapshotLeaderLoop.afterProducerSlotLineageValidation[IO, String](Right(()))(_ => cleanupCount.update(_ + 1))(
        signingCount.update(_ + 1).as("signed")
      )
      cleanups <- cleanupCount.get
      signatures <- signingCount.get
    } yield expect.all(invalid.isEmpty, valid.contains("signed"), cleanups == 1, signatures == 1)
  }

  test("parent-missing snapshot is inert; fetched ancestry replays parent-first before write/store/attest") { res =>
    implicit val (ks, j, h, sp, hs) = res

    val ancestorHash = Hash("a" * 64)
    val middleHash = Hash("b" * 64)
    val tipHash = Hash("c" * 64)

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      info = mkInfo(SortedMap(addrA -> Balance(NonNegLong(100L))))
      snapshot <- mkSnapshot(info)
      signed <- forAsyncHasher(snapshot, keyPair)
      valid = NakamotoSnapshotValidator.Valid(signed, info): NakamotoSnapshotValidator.ValidationResult
      ancestor = ReplayNode("ancestor", ancestorHash, valid)
      middle = ReplayNode("middle", middleHash, valid)
      tip = ReplayNode("tip", tipHash, valid)
      pending <- Ref.of[IO, Map[Hash, List[ReplayNode]]](Map.empty)
      events <- Ref.of[IO, List[String]](List.empty)

      // The tip arrives first. Production buffers it before returning ParentBuffered; the Valid-only callback contains every economic
      // side effect and must be unreachable for this result.
      _ <- NakamotoSyncDaemon.bufferPendingChild(middleHash, tip, pending)
      parentlessCommitted <- NakamotoSyncDaemon.commitReplayValidated[IO](NakamotoSnapshotValidator.ParentBuffered) { _ =>
        events.update(_ ++ List("write:tip", "store:tip", "attest:tip"))
      }
      beforeAncestor <- events.get
      pendingBeforeAncestor <- pending.get

      // The middle is also waiting. Once the ancestor is fetched, each successful replay drains only children keyed by its own hash.
      _ <- NakamotoSyncDaemon.bufferPendingChild(ancestorHash, middle, pending)
      _ <- {
        def replay(node: ReplayNode): IO[Unit] =
          for {
            _ <- events.update(_ :+ s"validate:${node.name}")
            committed <- NakamotoSyncDaemon.commitReplayValidated[IO](node.validation) { _ =>
              events.update(_ :+ s"write:${node.name}") >>
                events.update(_ :+ s"store:${node.name}") >>
                events.update(_ :+ s"attest:${node.name}") >>
                NakamotoSyncDaemon.drainBufferedChildren(node.hash, pending)(_ => IO.unit, replay)
            }
            _ <- events.update(_ :+ s"reject:${node.name}").unlessA(committed)
          } yield ()

        replay(ancestor)
      }
      afterReplay <- events.get
      pendingAfterReplay <- pending.get
    } yield
      expect.all(
        !parentlessCommitted,
        beforeAncestor.isEmpty,
        pendingBeforeAncestor.get(middleHash).contains(List(tip)),
        afterReplay == List(
          "validate:ancestor",
          "write:ancestor",
          "store:ancestor",
          "attest:ancestor",
          "validate:middle",
          "write:middle",
          "store:middle",
          "attest:middle",
          "validate:tip",
          "write:tip",
          "store:tip",
          "attest:tip"
        ),
        pendingAfterReplay.isEmpty
      )
  }

  test("invalid middle ancestor is not written/stored/attested and prevents descendant replay") { res =>
    implicit val (ks, j, h, sp, hs) = res

    val ancestorHash = Hash("d" * 64)
    val middleHash = Hash("e" * 64)
    val tipHash = Hash("f" * 64)

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      info = mkInfo(SortedMap(addrA -> Balance(NonNegLong(100L))))
      snapshot <- mkSnapshot(info)
      signed <- forAsyncHasher(snapshot, keyPair)
      valid = NakamotoSnapshotValidator.Valid(signed, info): NakamotoSnapshotValidator.ValidationResult
      ancestor = ReplayNode("ancestor", ancestorHash, valid)
      middle = ReplayNode("middle", middleHash, NakamotoSnapshotValidator.ContentMismatch("invalid transition"))
      tip = ReplayNode("tip", tipHash, valid)
      pending <- Ref.of[IO, Map[Hash, List[ReplayNode]]](Map.empty)
      events <- Ref.of[IO, List[String]](List.empty)
      _ <- NakamotoSyncDaemon.bufferPendingChild(middleHash, tip, pending)
      _ <- NakamotoSyncDaemon.bufferPendingChild(ancestorHash, middle, pending)
      _ <- {
        def replay(node: ReplayNode): IO[Unit] =
          for {
            _ <- events.update(_ :+ s"validate:${node.name}")
            committed <- NakamotoSyncDaemon.commitReplayValidated[IO](node.validation) { _ =>
              events.update(_ :+ s"write:${node.name}") >>
                events.update(_ :+ s"store:${node.name}") >>
                events.update(_ :+ s"attest:${node.name}") >>
                NakamotoSyncDaemon.drainBufferedChildren(node.hash, pending)(_ => IO.unit, replay)
            }
            _ <- events.update(_ :+ s"reject:${node.name}").unlessA(committed)
          } yield ()

        replay(ancestor)
      }
      afterReplay <- events.get
      pendingAfterReplay <- pending.get
    } yield
      expect.all(
        afterReplay == List(
          "validate:ancestor",
          "write:ancestor",
          "store:ancestor",
          "attest:ancestor",
          "validate:middle",
          "reject:middle"
        ),
        !afterReplay.exists(_.endsWith(":tip")),
        !afterReplay.exists(event => event == "write:middle" || event == "store:middle" || event == "attest:middle"),
        pendingAfterReplay == Map(middleHash -> List(tip))
      )
  }
}
