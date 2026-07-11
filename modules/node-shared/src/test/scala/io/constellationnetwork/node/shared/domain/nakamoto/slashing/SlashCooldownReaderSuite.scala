package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashManager.{SlashReason, SlashedRegistryEntry}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** [[SlashCooldownReader]] — the FINDING-002/EPIC-3.1 per-operator cooldown gate over the `Slashings` (fieldId 34) partition.
  *
  * Covers the PURE epoch-anchored semantics (anchor math, active-record filter, deterministic priority order, the Polkadot `UpToLimit`
  * floor), the MPT-backed production reader (entry round-trip through the canonical `entryCodec`, `anchorSettled` off
  * `lastPersistedOrdinal`), and the WRITE→READ consistency pin: an entry written by the real `InvalidStateProofSlashManager.applySlash`
  * excludes its operator for EXACTLY `cooldownEpochs` epochs under the Nakamoto 1-epoch-per-snapshot (TimeTrigger) advance the reader's
  * scaladoc documents.
  */
object SlashCooldownReaderSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val R: Long = 100L // eta-rotation period for the tests: anchor(E) = (E-1)·100 - 1

  private def peer(i: Int): PeerId = PeerId(Hex(f"$i%02x" * 64))

  private def entry(
    p: PeerId,
    eventOrd: Long,
    cooldownUntil: Long,
    checkpointHash: Hash = Hash("ab" * 32)
  ): SlashedRegistryEntry =
    SlashedRegistryEntry(
      peerId = p,
      shardId = io.constellationnetwork.schema.sharding.ShardId.unsafeApply(0),
      disputedCheckpointHash = checkpointHash,
      eventOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(eventOrd)),
      cooldownUntilEpoch = EpochProgress(NonNegLong.unsafeFrom(cooldownUntil)),
      evidenceDigest = checkpointHash,
      reason = SlashReason.InvalidStateProof
    )

  // ── PURE: anchor math ─────────────────────────────────────────────────────────────────────────────────────────────

  pureTest("slashAnchorOrdinal: (E-1)·R - 1 — one full eta-period behind the epoch; negative for bootstrap epochs 0/1") {
    expect.all(
      SlashCooldownReader.slashAnchorOrdinal(EtaPeriod(5L), R) == 399L, // end of period 3 (= E-2)
      SlashCooldownReader.slashAnchorOrdinal(EtaPeriod(2L), R) == 99L, // end of period 0
      SlashCooldownReader.slashAnchorOrdinal(EtaPeriod(1L), R) == -1L, // pre-genesis ⇒ structurally empty
      SlashCooldownReader.slashAnchorOrdinal(EtaPeriod(0L), R) == -101L
    )
  }

  // ── PURE: active-record filter ────────────────────────────────────────────────────────────────────────────────────

  pureTest("activeSlashCandidates: unexpired-at-anchor in; expired out; younger-than-anchor out (belongs to a LATER epoch's view)") {
    val anchor = 399L // epoch 5 at R=100
    val unexpired = entry(peer(1), eventOrd = 350L, cooldownUntil = 500L) // 350 ≤ 399 < 500 ⇒ ACTIVE
    val expired = entry(peer(2), eventOrd = 100L, cooldownUntil = 399L) // cooldown ≤ anchor ⇒ served its term
    val young = entry(peer(3), eventOrd = 400L, cooldownUntil = 900L) // written AFTER the anchor ⇒ not this epoch's view
    val exactExpiry = entry(peer(4), eventOrd = 100L, cooldownUntil = 400L) // 399 < 400 ⇒ still active (strict >)
    val candidates = SlashCooldownReader.activeSlashCandidates(List(unexpired, expired, young, exactExpiry), anchor)
    expect.all(
      candidates.map(_._1).toSet == Set(peer(1), peer(4)),
      // young peer(3) becomes active once the anchor passes its eventOrdinal (the NEXT epoch at R=100)
      SlashCooldownReader.activeSlashCandidates(List(young), 499L).map(_._1) == List(peer(3))
    )
  }

  pureTest("activeSlashCandidates: per-peer earliest ACTIVE eventOrdinal; deterministic (minEventOrdinal, peerId) order") {
    val anchor = 399L
    val entries = List(
      entry(peer(2), eventOrd = 300L, cooldownUntil = 500L, checkpointHash = Hash("aa" * 32)),
      entry(peer(2), eventOrd = 200L, cooldownUntil = 500L, checkpointHash = Hash("bb" * 32)), // earlier record, same peer
      entry(peer(1), eventOrd = 200L, cooldownUntil = 500L, checkpointHash = Hash("cc" * 32)), // ties on ordinal ⇒ peerId order
      entry(peer(3), eventOrd = 100L, cooldownUntil = 500L, checkpointHash = Hash("dd" * 32))
    )
    val candidates = SlashCooldownReader.activeSlashCandidates(entries, anchor)
    expect(candidates == List((peer(3), 100L), (peer(1), 200L), (peer(2), 200L)))
  }

  // ── PURE: the Polkadot UpToLimit floor ────────────────────────────────────────────────────────────────────────────

  pureTest("effectiveExclusion: caps at |active| - kQuorum, oldest-slash-first; newest offenders escape (Polkadot UpToLimit)") {
    val active = (1 to 4).map(peer).toSet
    val candidates = List((peer(1), 100L), (peer(2), 200L), (peer(3), 300L)) // 3 of 4 slashed
    expect.all(
      // floor 2 ⇒ maxExcludable = 4 - 2 = 2 ⇒ the two OLDEST slashes excluded; the newest (peer 3) escapes
      SlashCooldownReader.effectiveExclusion(active, candidates, minActiveFloor = 2) == Set(peer(1), peer(2)),
      // floor = |active| ⇒ nobody excluded (everyone-slashed liveness guard)
      SlashCooldownReader.effectiveExclusion(active, candidates, minActiveFloor = 4) == Set.empty[PeerId],
      // floor above |active| (mis-config shape) ⇒ still nobody excluded, never negative
      SlashCooldownReader.effectiveExclusion(active, candidates, minActiveFloor = 9) == Set.empty[PeerId],
      // candidates not in the active set neither excluded nor consuming the cap
      SlashCooldownReader.effectiveExclusion(Set(peer(3), peer(4)), candidates, minActiveFloor = 1) == Set(peer(3))
    )
  }

  // ── MPT-backed production reader ──────────────────────────────────────────────────────────────────────────────────

  private def mkStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    InMemoryMerklePatriciaProducer.make[IO]().flatMap(MptStore.make[IO, GlobalStateKey](_, GlobalStateKey.toHex[IO]))

  private def insertEntry(store: MptStore[IO, GlobalStateKey], e: SlashedRegistryEntry)(implicit h: Hasher[IO]): IO[Unit] =
    GlobalStateKey
      .slashingsKey[IO](e.peerId, e.shardId, e.disputedCheckpointHash)
      .flatMap(k => store.insert[SlashedRegistryEntry](k, e)(InvalidStateProofSlashedReader.entryCodec))

  test("fromMptStore: entry written through the GSAM codec/key shape drives exclusion; empty partition ⇒ empty exclusion") { res =>
    implicit val (h, sp, js) = res
    for {
      store <- mkStore
      reader = SlashCooldownReader.fromMptStore[IO](store, R)
      _ <- store.commit(SnapshotOrdinal(NonNegLong.unsafeFrom(450L))) // base ≥ anchor(5)=399 ⇒ settled
      empty <- reader.excludedForEpoch(EtaPeriod(5L))
      _ <- insertEntry(store, entry(peer(1), eventOrd = 350L, cooldownUntil = 500L))
      _ <- store.commit(SnapshotOrdinal(NonNegLong.unsafeFrom(451L)))
      after <- reader.excludedForEpoch(EtaPeriod(5L))
    } yield
      expect.all(
        empty.candidates.isEmpty,
        empty.anchorSettled,
        after.candidates == List((peer(1), 350L)),
        after.anchorSettled
      )
  }

  test("fromMptStore: anchorSettled=false while the base has not reached the epoch's anchor (early/adversarial future-epoch draw)") { res =>
    implicit val (h, sp, js) = res
    for {
      store <- mkStore
      _ <- insertEntry(store, entry(peer(1), eventOrd = 10L, cooldownUntil = 5000L))
      _ <- store.commit(SnapshotOrdinal(NonNegLong.unsafeFrom(50L))) // base 50 < anchor(5)=399
      view <- SlashCooldownReader.fromMptStore[IO](store, R).excludedForEpoch(EtaPeriod(5L))
      // bootstrap epochs: anchor < 0 ⇒ final without any base requirement
      bootstrap <- SlashCooldownReader.fromMptStore[IO](store, R).excludedForEpoch(EtaPeriod(1L))
    } yield
      expect.all(
        view.candidates == List((peer(1), 10L)), // visible so far — but NOT final
        !view.anchorSettled,
        bootstrap.candidates.isEmpty,
        bootstrap.anchorSettled
      )
  }

  // ── WRITE→READ consistency: the applySlash-written entry under the Nakamoto epoch≡ordinal advance ────────────────

  pureTest("applySlash-written entry: excluded for exactly cooldownEpochs epochs-as-ordinals past the slash, then re-eligible") {
    val slashedAtEpoch = 500L // = the snapshot ordinal under the Nakamoto TimeTrigger 1:1 advance
    val cooldownEpochs = 300L
    val res = InvalidStateProofSlashManager.applySlash(
      slashTargets = Set(peer(7)),
      priorDelegatedStakes = SortedMap.empty[io.constellationnetwork.schema.address.Address, SortedSet[
        io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord
      ]],
      priorNodeCollaterals = SortedMap.empty[io.constellationnetwork.schema.address.Address, SortedSet[
        io.constellationnetwork.schema.nodeCollateral.NodeCollateralRecord
      ]],
      eventOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(slashedAtEpoch)),
      currentEpoch = EpochProgress(NonNegLong.unsafeFrom(slashedAtEpoch)),
      shardId = io.constellationnetwork.schema.sharding.ShardId.unsafeApply(0),
      disputedCheckpointHash = Hash("ef" * 32),
      evidenceDigest = Hash("ef" * 32),
      slashFraction = Ratio.One,
      bountyFraction = Ratio.Zero,
      cooldownEpochs = cooldownEpochs
    )
    val written = res.newRegistryEntries
    val lastExcludedAnchor = slashedAtEpoch + cooldownEpochs - 1L // cooldownUntilEpoch=800 > 799 ⇒ still excluded
    val firstEligibleAnchor = slashedAtEpoch + cooldownEpochs // 800 > 800 is false ⇒ served
    expect.all(
      written.map(_.peerId) == List(peer(7)),
      written.head.cooldownUntilEpoch.value.value == slashedAtEpoch + cooldownEpochs,
      SlashCooldownReader.activeSlashCandidates(written, lastExcludedAnchor).map(_._1) == List(peer(7)),
      SlashCooldownReader.activeSlashCandidates(written, firstEligibleAnchor).isEmpty
    )
  }
}
