package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.ShardSlashingConfig
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardId, ShardNonParticipationCounter}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer

import weaver.MutableIOSuite

/** Tests for [[ShardNonParticipationStateManager]] + [[ShardNonParticipationSlasher]] — Slice 17 of
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.3.
  *
  * '''Required coverage''' (per slice spec):
  *   1. '''Increment + materialize round-trip''' — `recordMissedSlot(s, p, e)` then `materializeFromMpt(s, p, e)` returns a counter with
  *      `missedSlotsAsLeader=1`. Other fields are zero.
  *   1. '''Multiple increments accumulate''' — five `recordMissedSlot` calls produce `missedSlotsAsLeader=5`. Establishes the manager is
  *      not idempotent at the call boundary (documented in the state-manager scaladoc).
  *   1. '''Per-(shard, peer, epoch) isolation''' — increments scoped to `(s1, p, e)` do not bleed into `(s2, p, e)` or `(s1, p, e+1)` or
  *      `(s1, p', e)`. Confirms the MPT key composition is total over the three discriminators.
  *   1. '''Epoch boundary slash list — happy path''' — peer A with 5/10 missed slots (50% > 33%) appears in the slash list; peer B with
  *      3/10 missed slots (30% < 33%) does not.
  *   1. '''Min-denominator floor''' — peer C with 1/1 missed slots (100% missed but only 1 attempted) does NOT appear in the slash list
  *      because the default min-denominator floor (5) suppresses single-sample false positives.
  *   1. '''Mixed shard contributions deduplicate to one peerId''' — peer D missing in both shard 0 and shard 1 in the same epoch surfaces
  *      once in the slash list (the slash effect is per-peer-per-epoch, not per-peer-per-shard).
  *
  * '''Design choices for the min-denominator question.''' We chose to ship `minDenominatorPerEpoch = 5` in the production HOCON default and
  * the slasher honors it deterministically. Rationale (slashing-safety bar — `feedback_slashing_safety_bar`): a single missed sample is
  * indistinguishable from network noise (one slow RPC, one missed gossip frame), and the §10.4 severity tier ranks non-participation as the
  * least severe action. False positives are worse than false negatives — a wrongly-slashed honest operator loses bond + reputation; a
  * missed slashing of a chronic-absentee operator just defers the action by one epoch. Threshold and denominator floor are both HOCON
  * tunables; production operators can lower them once the cluster has empirical participation data.
  *
  * '''Test fixture pattern''':
  *   - Real `InMemoryMerklePatriciaProducer`-backed `MptStore` so round-trips exercise the actual codec + key path.
  *   - `Hasher.forJson` so test-key derivation matches the production hasher (any divergence would silently miss reads).
  *   - PeerIds built from a hex string for determinism — no key generation; the slasher only sorts/dedupes peerIds, so cryptographic
  *     content doesn't matter for any of the assertions.
  */
object ShardNonParticipationStateManagerSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, j)

  // ============================================================================
  // Fixtures
  // ============================================================================

  /** Deterministic peerId built from a label. The PeerId's wire shape is `Hex`-wrapped raw bytes; we pad/truncate to 64 hex chars (the
    * usual operator-key length) so the key derivation in `shardNonParticipationKey` produces stable hashes across runs.
    */
  private def mkPeer(label: String): PeerId =
    PeerId(Hex(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64)))

  /** Build an empty in-memory MPT store wired with the right key-projection function. Constructed fresh per test so per-test increments
    * never leak across tests.
    */
  private def mkStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield store

  // Convenient shard constants for the isolation tests.
  private val shard0: ShardId = ShardId.unsafeApply(0)
  private val shard1: ShardId = ShardId.unsafeApply(1)
  private val epoch0: EtaPeriod = EtaPeriod(0L)
  private val epoch1: EtaPeriod = EtaPeriod(1L)

  // Default slashing config for the slasher tests — mirrors the HOCON shipped in application.conf.
  private val defaultSlashingConfig: ShardSlashingConfig =
    ShardSlashingConfig(maxMissedPctPerEpoch = 33, minDenominatorPerEpoch = 5L)

  // ============================================================================
  // Tests
  // ============================================================================

  test("recordMissedSlot + materializeFromMpt round-trip — one increment shows up under the same key") { res =>
    implicit val (h, js) = res
    val peer = mkPeer("peer-roundtrip")
    for {
      store <- mkStore
      mgr = ShardNonParticipationStateManager.make[IO](store)
      _ <- mgr.recordMissedSlot(shard0, peer, epoch0)
      readBack <- mgr.materializeFromMpt(shard0, peer, epoch0)
    } yield
      expect.all(
        readBack.isDefined,
        readBack.contains(ShardNonParticipationCounter(shard0, peer, epoch0, 1L, 0L, 0L, 0L))
      )
  }

  test("multiple increments accumulate — five record* calls produce five") { res =>
    implicit val (h, js) = res
    val peer = mkPeer("peer-accumulate")
    for {
      store <- mkStore
      mgr = ShardNonParticipationStateManager.make[IO](store)
      // Five missed slots, three missed attestations, ten slot-eligibility events, twenty checkpoints-received events. The four counters
      // are independent — verifying all four together pins the RMW path against accidental cross-field writes.
      _ <- (1 to 5).toList.traverse_(_ => mgr.recordMissedSlot(shard0, peer, epoch0))
      _ <- (1 to 3).toList.traverse_(_ => mgr.recordMissedAttestation(shard0, peer, epoch0))
      _ <- (1 to 10).toList.traverse_(_ => mgr.recordSlotEligibility(shard0, peer, epoch0))
      _ <- (1 to 20).toList.traverse_(_ => mgr.recordCheckpointReceived(shard0, peer, epoch0))
      readBack <- mgr.materializeFromMpt(shard0, peer, epoch0)
    } yield
      expect.all(
        readBack.contains(
          ShardNonParticipationCounter(
            shardId = shard0,
            peerId = peer,
            epoch = epoch0,
            missedSlotsAsLeader = 5L,
            missedAttestationWindows = 3L,
            totalSlotsAsLeader = 10L,
            totalCheckpointsReceived = 20L
          )
        )
      )
  }

  test("per-(shard, peer, epoch) isolation — writes at one triple don't bleed into a sibling triple") { res =>
    implicit val (h, js) = res
    val peer = mkPeer("peer-isolate")
    val peerOther = mkPeer("peer-other")
    for {
      store <- mkStore
      mgr = ShardNonParticipationStateManager.make[IO](store)
      // Bump only the (shard0, peer, epoch0) cell.
      _ <- mgr.recordMissedSlot(shard0, peer, epoch0)
      _ <- mgr.recordMissedSlot(shard0, peer, epoch0)
      // Each of the sibling triples should be untouched.
      sibShard <- mgr.materializeFromMpt(shard1, peer, epoch0)
      sibEpoch <- mgr.materializeFromMpt(shard0, peer, epoch1)
      sibPeer <- mgr.materializeFromMpt(shard0, peerOther, epoch0)
      origin <- mgr.materializeFromMpt(shard0, peer, epoch0)
    } yield
      expect.all(
        sibShard.isEmpty,
        sibEpoch.isEmpty,
        sibPeer.isEmpty,
        origin.exists(_.missedSlotsAsLeader == 2L)
      )
  }

  test("epoch boundary slash list — high-miss peer slashed, low-miss peer skipped") { res =>
    implicit val (h, js) = res
    val peerA = mkPeer("peer-A-50pct")
    val peerB = mkPeer("peer-B-30pct")
    for {
      store <- mkStore
      mgr = ShardNonParticipationStateManager.make[IO](store)
      // Peer A: missed 5 of 10 → 50%. Above 33% with denominator 10 ≥ 5 ⇒ slash.
      _ <- (1 to 10).toList.traverse_(_ => mgr.recordSlotEligibility(shard0, peerA, epoch0))
      _ <- (1 to 5).toList.traverse_(_ => mgr.recordMissedSlot(shard0, peerA, epoch0))
      // Peer B: missed 3 of 10 → 30%. Below 33% with denominator 10 ≥ 5 ⇒ no slash.
      _ <- (1 to 10).toList.traverse_(_ => mgr.recordSlotEligibility(shard0, peerB, epoch0))
      _ <- (1 to 3).toList.traverse_(_ => mgr.recordMissedSlot(shard0, peerB, epoch0))
      slasher = ShardNonParticipationSlasher.make[IO](mgr, defaultSlashingConfig)
      slashed <- slasher.evaluateEpochBoundary(epoch0)
    } yield
      expect.all(
        slashed.contains(peerA),
        !slashed.contains(peerB),
        slashed.size == 1
      )
  }

  test("min-denominator floor — 100% missed at single sample does not fire") { res =>
    implicit val (h, js) = res
    val peerC = mkPeer("peer-C-1of1")
    for {
      store <- mkStore
      mgr = ShardNonParticipationStateManager.make[IO](store)
      // Peer C: missed 1 of 1 (100% missed, but only one attempted — below min-denominator floor of 5).
      _ <- mgr.recordSlotEligibility(shard0, peerC, epoch0)
      _ <- mgr.recordMissedSlot(shard0, peerC, epoch0)
      slasher = ShardNonParticipationSlasher.make[IO](mgr, defaultSlashingConfig)
      slashed <- slasher.evaluateEpochBoundary(epoch0)
    } yield expect(slashed.isEmpty)
  }

  test("attestation-duty rate also slashes — independent from slot-leader-duty rate") { res =>
    implicit val (h, js) = res
    val peerD = mkPeer("peer-D-att")
    for {
      store <- mkStore
      mgr = ShardNonParticipationStateManager.make[IO](store)
      // Peer D: received 10 checkpoints, missed 4 (40% > 33%). No slot-leader activity at all (denominator 0 → that rate is skipped).
      _ <- (1 to 10).toList.traverse_(_ => mgr.recordCheckpointReceived(shard0, peerD, epoch0))
      _ <- (1 to 4).toList.traverse_(_ => mgr.recordMissedAttestation(shard0, peerD, epoch0))
      slasher = ShardNonParticipationSlasher.make[IO](mgr, defaultSlashingConfig)
      slashed <- slasher.evaluateEpochBoundary(epoch0)
    } yield
      expect.all(
        slashed.contains(peerD),
        slashed.size == 1
      )
  }

  test("cross-shard dedup — peer slashable in both shard 0 and shard 1 surfaces once in the slash list") { res =>
    implicit val (h, js) = res
    val peerE = mkPeer("peer-E-cross")
    for {
      store <- mkStore
      mgr = ShardNonParticipationStateManager.make[IO](store)
      // Peer E is slashable in shard 0 AND shard 1 — slash list must still contain it exactly once.
      _ <- (1 to 10).toList.traverse_(_ => mgr.recordSlotEligibility(shard0, peerE, epoch0))
      _ <- (1 to 6).toList.traverse_(_ => mgr.recordMissedSlot(shard0, peerE, epoch0))
      _ <- (1 to 10).toList.traverse_(_ => mgr.recordSlotEligibility(shard1, peerE, epoch0))
      _ <- (1 to 7).toList.traverse_(_ => mgr.recordMissedSlot(shard1, peerE, epoch0))
      slasher = ShardNonParticipationSlasher.make[IO](mgr, defaultSlashingConfig)
      slashed <- slasher.evaluateEpochBoundary(epoch0)
    } yield
      expect.all(
        slashed == List(peerE)
      )
  }

  test("epoch boundary scopes to the requested epoch — counters from other epochs are ignored") { res =>
    implicit val (h, js) = res
    val peerF = mkPeer("peer-F-epoch-scope")
    for {
      store <- mkStore
      mgr = ShardNonParticipationStateManager.make[IO](store)
      // Peer F is slashable in epoch 1 but not epoch 0. The slasher targets epoch 0 → empty.
      _ <- (1 to 10).toList.traverse_(_ => mgr.recordSlotEligibility(shard0, peerF, epoch1))
      _ <- (1 to 6).toList.traverse_(_ => mgr.recordMissedSlot(shard0, peerF, epoch1))
      slasher = ShardNonParticipationSlasher.make[IO](mgr, defaultSlashingConfig)
      slashedEpoch0 <- slasher.evaluateEpochBoundary(epoch0)
      slashedEpoch1 <- slasher.evaluateEpochBoundary(epoch1)
    } yield
      expect.all(
        slashedEpoch0.isEmpty,
        slashedEpoch1 == List(peerF)
      )
  }

  test("materializeAllForEpoch — returns every (shard, peer) pair touched in that epoch") { res =>
    implicit val (h, js) = res
    val peer1 = mkPeer("peer-mat-1")
    val peer2 = mkPeer("peer-mat-2")
    for {
      store <- mkStore
      mgr = ShardNonParticipationStateManager.make[IO](store)
      _ <- mgr.recordMissedSlot(shard0, peer1, epoch0)
      _ <- mgr.recordMissedSlot(shard1, peer1, epoch0)
      _ <- mgr.recordMissedSlot(shard0, peer2, epoch0)
      // A different epoch entry should NOT surface in the epoch-0 scan.
      _ <- mgr.recordMissedSlot(shard0, peer1, epoch1)
      all <- mgr.materializeAllForEpoch(epoch0)
    } yield
      expect.all(
        all.size == 3,
        all.contains((shard0, peer1)),
        all.contains((shard1, peer1)),
        all.contains((shard0, peer2)),
        // Sanity: every returned counter is actually scoped to epoch0.
        all.values.forall(_.epoch == epoch0)
      )
  }
}
