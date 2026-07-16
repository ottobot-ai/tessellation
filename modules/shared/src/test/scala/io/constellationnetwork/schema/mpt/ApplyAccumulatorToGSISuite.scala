package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.generators._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.serde.codecs.instances.ConsumedAllowSpendCodec

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Task #12 slice 4 — tests for the ml0 ADOPT-AND-VERIFY follow primitives:
  *   - (a) `applyAccumulatorToGSI` merge correctness, incl. removals + Option None/Some emptiness, anchored to the MPT byte-equivalence
  *     contract (the candidate GSI rebuilt as a full state produces the SAME mptRoot as the producer's incremental `syncFromStateChanges`).
  *   - (b) `adoptAndVerifyChangeSetDelta` happy path: matching signed mptRoot ⇒ `Some(adoptedGSI)`, store advanced.
  *   - (c) NEGATIVE safety guard: tampered delta / wrong signed mptRoot ⇒ `None`, store ROLLED BACK to the prior state (no silent advance).
  */
object ApplyAccumulatorToGSISuite extends MutableIOSuite {

  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  // Post-MPT-migration format so `getRootHashForOrdinal` reflects the typed scodec store (irrelevant to raw build roots, but explicit).
  implicit val stateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(0L)))

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] = for {
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (j, h, sp)

  private val addr1 = addressGen.sample.get
  private val addr2 = addressGen.sample.get
  private val addr3 = addressGen.sample.get
  private val addr4 = addressGen.sample.get

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private def createSignedStake(source: Address, nodeId: io.constellationnetwork.schema.peer.PeerId, amount: Long): Signed[
    UpdateDelegatedStake.Create
  ] =
    Signed(
      UpdateDelegatedStake.Create(
        source = source,
        nodeId = nodeId,
        amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(amount)),
        fee = DelegatedStakeFee(0L),
        tokenLockRef = Hash.empty
      ),
      NonEmptySet.one[SignatureProof](SignatureProof(nodeId.toId, Signature(Hex(Hash.empty.value))))
    )

  private val ord = SnapshotOrdinal(NonNegLong(1000L))

  /** Build an MPT root over a GSI treated as a FULL state (the validation path: `syncFromGlobalSnapshotInfo`). */
  private def fullStateRoot(info: GlobalSnapshotInfo)(implicit j: JsonSerializer[IO], h: Hasher[IO]): IO[Hash] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- store.syncFromGlobalSnapshotInfo(info, ord)
      trie <- store.build(ord)
    } yield trie.toOption.get.rootHash.value

  // ---------------------------------------------------------------------------------------------------------------------------------------
  // (a) applyAccumulatorToGSI merge correctness
  // ---------------------------------------------------------------------------------------------------------------------------------------

  test("(a) applyAccumulatorToGSI: overwrite + additive fields merge per-key, Option lifts from None on first entry") { res =>
    implicit val (j, h, _) = res

    // Prior: balances on addr1/addr2; everything optional empty (Some(empty) — post-tess3 convention) except we set None on
    // lastAllowSpendRefs to exercise None→Some lift.
    val prior = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap(addr1 -> Balance(1000L), addr2 -> Balance(2000L)),
      lastTxRefs = SortedMap(addr1 -> transactionReferenceGen.sample.get),
      lastAllowSpendRefs = None
    )

    val txRefNew = transactionReferenceGen.sample.get
    val allowRefNew = AllowSpendReference(AllowSpendOrdinal(1L), testHash("ar"))
    val delta = StateChangesAccumulator(
      // overwrite addr1, add addr3
      balances = SortedMap(addr1 -> Balance(1500L), addr3 -> Balance(500L)),
      // overwrite addr1 txRef
      lastTxRefs = SortedMap(addr1 -> txRefNew),
      // first entry into a None field → must lift to Some
      lastAllowSpendRefs = SortedMap(addr2 -> allowRefNew)
    )

    val merged = GlobalStateConverter.applyAccumulatorToGSI(prior, delta)

    IO {
      expect.all(
        merged.balances == SortedMap(addr1 -> Balance(1500L), addr2 -> Balance(2000L), addr3 -> Balance(500L)),
        merged.lastTxRefs.get(addr1).contains(txRefNew),
        merged.lastAllowSpendRefs == Some(SortedMap(addr2 -> allowRefNew))
      )
    }
  }

  test("(a) applyAccumulatorToGSI: removal-set fields drop removed keys and overlay delta") { res =>
    implicit val (j, h, _) = res
    val nodeId = Id(Hex("1234567890abcdef" * 8)).toPeerId

    val recA = DelegatedStakeRecord(createSignedStake(addr1, nodeId, 1000L), SnapshotOrdinal(1L), Balance(50L), None, None)
    val recB = DelegatedStakeRecord(createSignedStake(addr2, nodeId, 2000L), SnapshotOrdinal(1L), Balance(75L), None, None)
    val recBNew = DelegatedStakeRecord(createSignedStake(addr2, nodeId, 3000L), SnapshotOrdinal(2L), Balance(99L), None, None)
    val recC = DelegatedStakeRecord(createSignedStake(addr3, nodeId, 4000L), SnapshotOrdinal(2L), Balance(10L), None, None)

    val prior = GlobalSnapshotInfo.empty.copy(
      activeDelegatedStakes = Some(
        SortedMap(
          addr1 -> SortedSet(recA), // will be REMOVED
          addr2 -> SortedSet(recB) // will be OVERWRITTEN
        )
      )
    )

    val delta = StateChangesAccumulator(
      activeDelegatedStakes = SortedMap(
        addr2 -> SortedSet(recBNew), // overwrite
        addr3 -> SortedSet(recC) // add
      ),
      removedDelegatedStakeKeys = Set(addr1) // remove
    )

    val merged = GlobalStateConverter.applyAccumulatorToGSI(prior, delta)

    IO {
      expect.all(
        merged.activeDelegatedStakes == Some(
          SortedMap(
            addr2 -> SortedSet(recBNew),
            addr3 -> SortedSet(recC)
          )
        ),
        !merged.activeDelegatedStakes.get.contains(addr1)
      )
    }
  }

  test("(a) applyAccumulatorToGSI: nested tokenLockBalances merge at flattened (token, holder) key with pair removals") { res =>
    implicit val (j, h, _) = res

    val prior = GlobalSnapshotInfo.empty.copy(
      tokenLockBalances = Some(
        SortedMap(
          addr1 -> SortedMap(addr3 -> Balance(100L), addr4 -> Balance(200L)),
          addr2 -> SortedMap(addr3 -> Balance(300L))
        )
      )
    )

    val delta = StateChangesAccumulator(
      // overwrite (addr1, addr3); add (addr2, addr4)
      tokenLockBalances = SortedMap(
        addr1 -> SortedMap(addr3 -> Balance(150L)),
        addr2 -> SortedMap(addr4 -> Balance(400L))
      ),
      // remove (addr1, addr4) — leaves addr1 bucket = {addr3}; and remove (addr2, addr3) — leaves addr2 bucket = {addr4}
      removedTokenLockBalanceKeys = Set((addr1, addr4), (addr2, addr3))
    )

    val merged = GlobalStateConverter.applyAccumulatorToGSI(prior, delta)

    IO {
      expect(
        merged.tokenLockBalances == Some(
          SortedMap(
            addr1 -> SortedMap(addr3 -> Balance(150L)),
            addr2 -> SortedMap(addr4 -> Balance(400L))
          )
        )
      )
    }
  }

  test("(a) applyAccumulatorToGSI: nested tokenLockBalances drops an outer bucket that becomes empty after removals") { res =>
    implicit val (j, h, _) = res

    val prior = GlobalSnapshotInfo.empty.copy(
      tokenLockBalances = Some(
        SortedMap(
          addr1 -> SortedMap(addr3 -> Balance(100L)),
          addr2 -> SortedMap(addr4 -> Balance(200L))
        )
      )
    )
    // remove the only holder under addr1 → the addr1 outer bucket must vanish entirely
    val delta = StateChangesAccumulator(removedTokenLockBalanceKeys = Set((addr1, addr3)))

    val merged = GlobalStateConverter.applyAccumulatorToGSI(prior, delta)

    IO(expect(merged.tokenLockBalances == Some(SortedMap(addr2 -> SortedMap(addr4 -> Balance(200L))))))
  }

  test("(a) applyAccumulatorToGSI: empty delta preserves prior Some(empty) vs None exactly") { res =>
    implicit val (j, h, _) = res

    val prior = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap(addr1 -> Balance(1L)),
      lastAllowSpendRefs = None, // None must stay None under an empty delta
      activeDelegatedStakes = Some(SortedMap.empty) // Some(empty) must stay Some(empty)
    )
    val merged = GlobalStateConverter.applyAccumulatorToGSI(prior, StateChangesAccumulator())

    IO {
      expect.all(
        merged.lastAllowSpendRefs.isEmpty, // still None
        merged.activeDelegatedStakes == Some(SortedMap.empty[Address, SortedSet[DelegatedStakeRecord]]),
        merged.balances == SortedMap(addr1 -> Balance(1L))
      )
    }
  }

  test("(a) applyAccumulatorToGSI byte-equivalence: candidate-GSI full-state root == producer incremental syncFromStateChanges root") {
    res =>
      implicit val (j, h, sp) = res
      val nodeId = Id(Hex("1234567890abcdef" * 8)).toPeerId

      // A realistic prior across several fields.
      val prior = GlobalSnapshotInfo.empty.copy(
        balances = SortedMap(addr1 -> Balance(1000L), addr2 -> Balance(2000L)),
        lastTxRefs = SortedMap(addr1 -> transactionReferenceGen.sample.get),
        lastStateChannelSnapshotHashes = SortedMap(addr1 -> testHash("sc-a")),
        activeDelegatedStakes = Some(
          SortedMap(
            addr1 -> SortedSet(DelegatedStakeRecord(createSignedStake(addr1, nodeId, 1L), SnapshotOrdinal(1L), Balance(5L), None, None)),
            addr2 -> SortedSet(DelegatedStakeRecord(createSignedStake(addr2, nodeId, 2L), SnapshotOrdinal(1L), Balance(7L), None, None))
          )
        ),
        tokenLockBalances = Some(SortedMap(addr1 -> SortedMap(addr3 -> Balance(100L))))
      )

      // A delta exercising overwrite, add, removal-set, and nested removal.
      val delta = StateChangesAccumulator(
        balances = SortedMap(addr1 -> Balance(1500L), addr3 -> Balance(500L)),
        lastStateChannelSnapshotHashes = SortedMap(addr2 -> testHash("sc-b")),
        activeDelegatedStakes = SortedMap(
          addr3 -> SortedSet(DelegatedStakeRecord(createSignedStake(addr3, nodeId, 3L), SnapshotOrdinal(2L), Balance(9L), None, None))
        ),
        removedDelegatedStakeKeys = Set(addr1),
        tokenLockBalances = SortedMap(addr2 -> SortedMap(addr4 -> Balance(400L))),
        removedTokenLockBalanceKeys = Set((addr1, addr3))
      )

      val candidate = GlobalStateConverter.applyAccumulatorToGSI(prior, delta)

      for {
        // Producer path: seed prior into a store, apply delta via syncFromStateChanges, read root.
        prodProducer <- InMemoryMerklePatriciaProducer.make[IO]()
        prodStore <- MptStore.make[IO, GlobalStateKey](prodProducer, GlobalStateKey.toHex[IO])
        _ <- prodStore.syncFromGlobalSnapshotInfo(prior, SnapshotOrdinal(NonNegLong(999L)))
        _ <- prodStore.syncFromStateChanges(delta, ord)
        prodTrie <- prodStore.build(ord)
        producerRoot = prodTrie.toOption.get.rootHash.value

        // Candidate path: treat applyAccumulatorToGSI's output as a full state, build its root.
        candidateRoot <- fullStateRoot(candidate)
      } yield expect.same(producerRoot, candidateRoot)
  }

  // ---------------------------------------------------------------------------------------------------------------------------------------
  // (b) adopt-verify happy path
  // ---------------------------------------------------------------------------------------------------------------------------------------

  test("(b) adoptAndVerifyChangeSetDelta: matching signed mptRoot ⇒ Some(adoptedGSI) and store advances") { res =>
    implicit val (j, h, sp) = res

    val priorOrd = SnapshotOrdinal(NonNegLong(999L))
    val prior = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap(addr1 -> Balance(1000L), addr2 -> Balance(2000L)),
      lastTxRefs = SortedMap(addr1 -> transactionReferenceGen.sample.get)
    )
    val delta = StateChangesAccumulator(
      balances = SortedMap(addr1 -> Balance(1500L), addr3 -> Balance(500L)),
      lastTxRefs = SortedMap(addr3 -> transactionReferenceGen.sample.get)
    )

    for {
      // Compute the SIGNED root a producer would publish for prior+delta (independent fresh store).
      sigProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      sigStore <- MptStore.make[IO, GlobalStateKey](sigProducer, GlobalStateKey.toHex[IO])
      _ <- sigStore.syncFromGlobalSnapshotInfo(prior, priorOrd)
      _ <- sigStore.syncFromStateChanges(delta, ord)
      _ <- sigStore.build(ord)
      sigBytes <- sigStore.allEntriesAsBytes
      signedRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](sigBytes)

      // ml0's store, seeded at the prior state.
      ml0Producer <- InMemoryMerklePatriciaProducer.make[IO]()
      ml0Store <- MptStore.make[IO, GlobalStateKey](ml0Producer, GlobalStateKey.toHex[IO])
      _ <- ml0Store.syncFromGlobalSnapshotInfo(prior, priorOrd)

      result <- GlobalStateConverter.adoptAndVerifyChangeSetDelta[IO](ml0Store, prior, delta, signedRoot.some, ord)

      // After Commit, ml0's store root must equal the signed root.
      _ <- ml0Store.build(ord)
      ml0Bytes <- ml0Store.allEntriesAsBytes
      ml0Root <- GlobalSnapshotInfo.consensusMptRoot[IO](ml0Bytes)
    } yield
      expect.all(
        result.isDefined,
        result.contains(GlobalStateConverter.applyAccumulatorToGSI(prior, delta)),
        ml0Root == signedRoot
      )
  }

  // ---------------------------------------------------------------------------------------------------------------------------------------
  // (c) NEGATIVE safety guard — verify-before-adopt must roll back on mismatch
  // ---------------------------------------------------------------------------------------------------------------------------------------

  test(
    "(c) current accumulator omits MPT-native fields 33/34: each signed-root mismatch returns None and rolls back every representable change"
  ) { res =>
    implicit val (j, h, sp) = res

    val priorOrd = SnapshotOrdinal(NonNegLong(999L))
    val prior = GlobalSnapshotInfo.empty.copy(balances = SortedMap(addr1 -> Balance(1000L)))
    val delta = StateChangesAccumulator(balances = SortedMap(addr1 -> Balance(1500L), addr2 -> Balance(500L)))
    val consumedHash = testHash("consumed-native-only")
    val consumed = ConsumedAllowSpend(
      allowSpendHash = consumedHash,
      source = addr1,
      destination = addr2,
      currencyId = none,
      amount = SwapAmount(PosLong.unsafeFrom(1L)),
      lastValidEpochProgress = EpochProgress(NonNegLong(10L)),
      consumedAtOrdinal = ord,
      consumingSpendRef = testHash("spend-ref")
    )

    final case class OmissionOutcome(
      nativeField: GlobalStateFieldId,
      nativeKeyPresent: Boolean,
      nativeLeafChangesRoot: Boolean,
      deltaUpsertFields: Set[GlobalStateFieldId],
      deltaRemovalFields: Set[GlobalStateFieldId],
      candidateDiffersFromPrior: Boolean,
      result: Option[GlobalSnapshotInfo],
      entriesRestored: Boolean,
      rootRestored: Boolean,
      ordinalRestored: Boolean
    )

    def exercise(
      writeNativeLeaf: MptStore[IO, GlobalStateKey] => IO[Hex]
    ): IO[OmissionOutcome] =
      for {
        // This is the complete typed delta emitted for the otherwise ordinary balance update. Its observable key set is the honest
        // characterization seam: neither native field has an upsert or removal representation.
        typedUpserts <- GlobalStateConverter.toAccumulatorBytesDelta[IO](delta)
        typedRemovals <- GlobalStateConverter.toAccumulatorRemovalKeys[IO](delta)

        // Independent producer image: apply the representable delta, then add exactly one MPT-native consensus leaf and compute the root
        // the snapshot would sign. Field 33 uses its existing typed codec; field 34 deliberately remains opaque bytes in this test.
        targetProducer <- InMemoryMerklePatriciaProducer.make[IO]()
        targetStore <- MptStore.make[IO, GlobalStateKey](targetProducer, GlobalStateKey.toHex[IO])
        _ <- targetStore.syncFromGlobalSnapshotInfo(prior, priorOrd)
        _ <- targetStore.syncFromStateChanges(delta, ord)
        deltaOnlyEntries <- targetStore.allEntriesAsBytes
        deltaOnlyRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](deltaOnlyEntries)
        nativeHex <- writeNativeLeaf(targetStore)
        _ <- targetStore.commit(ord)
        targetEntries <- targetStore.allEntriesAsBytes
        targetRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](targetEntries)

        // Follower has the exact parent but receives only the current 31-field accumulator. It temporarily applies the balance update,
        // cannot reproduce the native leaf, and must roll the transaction back instead of partially advancing MPT or returning a GSI.
        followerProducer <- InMemoryMerklePatriciaProducer.make[IO]()
        followerStore <- MptStore.make[IO, GlobalStateKey](followerProducer, GlobalStateKey.toHex[IO])
        _ <- followerStore.syncFromGlobalSnapshotInfo(prior, priorOrd)
        priorEntries <- followerStore.allEntriesAsBytes
        priorRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](priorEntries)
        priorPersistedOrdinal <- followerStore.lastPersistedOrdinal
        candidate = GlobalStateConverter.applyAccumulatorToGSI(prior, delta)
        result <- GlobalStateConverter.adoptAndVerifyChangeSetDelta[IO](followerStore, prior, delta, targetRoot.some, ord)
        afterEntries <- followerStore.allEntriesAsBytes
        afterRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](afterEntries)
        afterPersistedOrdinal <- followerStore.lastPersistedOrdinal
      } yield
        OmissionOutcome(
          nativeField = GlobalStateKey.fieldIdFromHex(nativeHex).get,
          nativeKeyPresent = targetEntries.contains(nativeHex),
          nativeLeafChangesRoot = targetRoot =!= deltaOnlyRoot,
          deltaUpsertFields = typedUpserts.keysIterator.map(_.fieldId).toSet,
          deltaRemovalFields = typedRemovals.iterator.map(_.fieldId).toSet,
          candidateDiffersFromPrior = candidate =!= prior,
          result = result,
          entriesRestored = entriesEqual(priorEntries, afterEntries),
          rootRestored = afterRoot === priorRoot,
          ordinalRestored = afterPersistedOrdinal === priorPersistedOrdinal
        )

    val field33 = exercise { store =>
      val key = GlobalStateKey.consumedAllowSpendKey(consumedHash)
      store
        .insert[ConsumedAllowSpend](key, consumed)(ConsumedAllowSpendCodec.immutableCodec)
        .productR(GlobalStateKey.toHex[IO](key))
    }
    val field34 = exercise { store =>
      for {
        key <- GlobalStateKey.slashingsKey[IO](PeerId(Hex("ab" * 64)), ShardId.unsafeApply(0), testHash("slash-checkpoint"))
        hex <- GlobalStateKey.toHex[IO](key)
        // Bytes-level only: this test must not choose or freeze the future field-34 value schema.
        _ <- store.underlying.insertBytes(Map(hex -> Array[Byte](1, 2, 3, 4))).flatMap(_.liftTo[IO])
      } yield hex
    }

    for {
      spentSet <- field33
      slashLedger <- field34
      nativeFields = GlobalStateFieldId.mptNativeConsensusFields
    } yield
      expect.all(
        nativeFields == Set(GlobalStateFieldId.ConsumedAllowSpends, GlobalStateFieldId.Slashings),
        spentSet.nativeField == GlobalStateFieldId.ConsumedAllowSpends,
        slashLedger.nativeField == GlobalStateFieldId.Slashings,
        spentSet.nativeKeyPresent,
        slashLedger.nativeKeyPresent,
        spentSet.nativeLeafChangesRoot,
        slashLedger.nativeLeafChangesRoot,
        spentSet.deltaUpsertFields.contains(GlobalStateFieldId.Balances),
        slashLedger.deltaUpsertFields.contains(GlobalStateFieldId.Balances),
        spentSet.deltaUpsertFields.intersect(nativeFields).isEmpty,
        slashLedger.deltaUpsertFields.intersect(nativeFields).isEmpty,
        spentSet.deltaRemovalFields.intersect(nativeFields).isEmpty,
        slashLedger.deltaRemovalFields.intersect(nativeFields).isEmpty,
        spentSet.candidateDiffersFromPrior,
        slashLedger.candidateDiffersFromPrior,
        spentSet.result.isEmpty,
        slashLedger.result.isEmpty,
        spentSet.entriesRestored,
        slashLedger.entriesRestored,
        spentSet.rootRestored,
        slashLedger.rootRestored,
        spentSet.ordinalRestored,
        slashLedger.ordinalRestored
      )
  }

  test("(c) adoptAndVerifyChangeSetDelta NEGATIVE: wrong signed mptRoot ⇒ None and store ROLLED BACK to prior") { res =>
    implicit val (j, h, sp) = res

    val priorOrd = SnapshotOrdinal(NonNegLong(999L))
    val prior = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap(addr1 -> Balance(1000L), addr2 -> Balance(2000L))
    )
    val delta = StateChangesAccumulator(balances = SortedMap(addr1 -> Balance(1500L), addr3 -> Balance(500L)))

    // A deliberately-wrong "signed" root (not the root prior+delta produces) — simulates a tampered/incorrect delta.
    val wrongRoot = testHash("totally-wrong-root")

    for {
      ml0Producer <- InMemoryMerklePatriciaProducer.make[IO]()
      ml0Store <- MptStore.make[IO, GlobalStateKey](ml0Producer, GlobalStateKey.toHex[IO])
      _ <- ml0Store.syncFromGlobalSnapshotInfo(prior, priorOrd)
      priorTrie <- ml0Store.build(priorOrd)
      priorRoot = priorTrie.toOption.get.rootHash.value
      priorEntries <- ml0Store.allEntriesAsBytes

      result <- GlobalStateConverter.adoptAndVerifyChangeSetDelta[IO](ml0Store, prior, delta, wrongRoot.some, ord)

      // Store must be unchanged — same entry set and the prior root rebuilds.
      afterEntries <- ml0Store.allEntriesAsBytes
      afterTrie <- ml0Store.build(priorOrd)
      afterRoot = afterTrie.toOption.get.rootHash.value
    } yield
      expect.all(
        result.isEmpty, // fell back, did NOT adopt
        afterRoot == priorRoot, // store state unchanged
        entriesEqual(priorEntries, afterEntries) // byte-for-byte unchanged (delta's addr3 etc. NOT written)
      )
  }

  test("(c) adoptAndVerifyChangeSetDelta NEGATIVE: tampered delta value ⇒ recomputed root ≠ signed ⇒ None, no advance") { res =>
    implicit val (j, h, sp) = res

    val priorOrd = SnapshotOrdinal(NonNegLong(999L))
    val prior = GlobalSnapshotInfo.empty.copy(balances = SortedMap(addr1 -> Balance(1000L)))

    val honestDelta = StateChangesAccumulator(balances = SortedMap(addr1 -> Balance(1500L)))
    // Signed root reflects the HONEST delta...
    // ...but ml0 is fed a TAMPERED delta (different balance for addr1) — recompute must diverge from the signed root.
    val tamperedDelta = StateChangesAccumulator(balances = SortedMap(addr1 -> Balance(9999L)))

    for {
      sigProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      sigStore <- MptStore.make[IO, GlobalStateKey](sigProducer, GlobalStateKey.toHex[IO])
      _ <- sigStore.syncFromGlobalSnapshotInfo(prior, priorOrd)
      _ <- sigStore.syncFromStateChanges(honestDelta, ord)
      sigTrie <- sigStore.build(ord)
      signedRoot = sigTrie.toOption.get.rootHash.value

      ml0Producer <- InMemoryMerklePatriciaProducer.make[IO]()
      ml0Store <- MptStore.make[IO, GlobalStateKey](ml0Producer, GlobalStateKey.toHex[IO])
      _ <- ml0Store.syncFromGlobalSnapshotInfo(prior, priorOrd)
      priorEntries <- ml0Store.allEntriesAsBytes

      result <- GlobalStateConverter.adoptAndVerifyChangeSetDelta[IO](ml0Store, prior, tamperedDelta, signedRoot.some, ord)

      afterEntries <- ml0Store.allEntriesAsBytes
    } yield
      expect.all(
        result.isEmpty,
        entriesEqual(priorEntries, afterEntries)
      )
  }

  test("(c) adoptAndVerifyChangeSetDelta: legacy-format ordinal (signedMptRoot = None) ⇒ None (defer to full path)") { res =>
    implicit val (j, h, sp) = res
    val prior = GlobalSnapshotInfo.empty.copy(balances = SortedMap(addr1 -> Balance(1000L)))
    val delta = StateChangesAccumulator(balances = SortedMap(addr1 -> Balance(1500L)))

    for {
      ml0Producer <- InMemoryMerklePatriciaProducer.make[IO]()
      ml0Store <- MptStore.make[IO, GlobalStateKey](ml0Producer, GlobalStateKey.toHex[IO])
      _ <- ml0Store.syncFromGlobalSnapshotInfo(prior, SnapshotOrdinal(NonNegLong(999L)))
      priorEntries <- ml0Store.allEntriesAsBytes
      result <- GlobalStateConverter.adoptAndVerifyChangeSetDelta[IO](ml0Store, prior, delta, none, ord)
      afterEntries <- ml0Store.allEntriesAsBytes
    } yield expect.all(result.isEmpty, entriesEqual(priorEntries, afterEntries))
  }

  private def entriesEqual(a: Map[Hex, Array[Byte]], b: Map[Hex, Array[Byte]]): Boolean =
    a.keySet == b.keySet && a.forall { case (k, v) => b.get(k).exists(_.sameElements(v)) }
}
