package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Security regression for the parent-missing catch-up admission gate ([[NakamotoSyncDaemon.verifyCatchUpSnapshot]]).
  *
  * THE VULNERABILITY this guards: the deep-catch-up path adopts a gossiped `(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)` as the
  * node's ENTIRE canonical gl0 state (balances/txRefs/stakes/locks + MPT) WITHOUT the snapshot's parent — so the full
  * `NakamotoSnapshotValidator.validate` (VRF + slot-cert) can't run. Before the fix, that adoption was UNVERIFIED: a single peer gossiping
  * a forged tuple could unilaterally reset the victim's state. `verifyCatchUpSnapshot` closes this with two parent-free, deterministic
  * gates — envelope signature (gate 1) and stateProof-vs-GSI consistency (gate 2) — that BOTH pass for an honest snapshot and BOTH
  * fail-closed for a forged / inconsistent one.
  *
  *   1. '''honest tuple → Accept''' — a correctly-signed snapshot whose `stateProof` was built from the carried GSI is adopted (legitimate
  *      catch-up still recovers).
  *   1. '''forged signature → RejectedInvalidSignature''' — a snapshot whose body was tampered after signing (signature no longer matches
  *      the hash) is rejected by gate 1.
  *   1. '''mismatched GSI → RejectedStateProofMismatch''' — a validly-signed snapshot paired with a DIFFERENT GlobalSnapshotInfo (attacker-
  *      chosen state) is rejected by gate 2.
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
  private val addrB: Address = Address("DAG3wbdB4HtqeSumsA8hDFBBvVXxexAtQXJMrbmt")

  test("honest catch-up tuple (valid signature + GSI matching its committed stateProof) is Accepted") { res =>
    implicit val (ks, j, h, sp, hs) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      info = mkInfo(SortedMap(addrA -> Balance(NonNegLong(100L))))
      snapshot <- mkSnapshot(info)
      signed <- forAsyncHasher(snapshot, keyPair)
      verdict <- NakamotoSyncDaemon.verifyCatchUpSnapshot[IO](signed, info)
    } yield
      verdict match {
        case NakamotoSyncDaemon.CatchUpVerdict.Accept(hashed) =>
          expect(hashed.signed.value.ordinal === SnapshotOrdinal(NonNegLong(1L)))
        case other => failure(s"expected Accept, got $other")
      }
  }

  test("forged snapshot (body tampered after signing → invalid signature) is RejectedInvalidSignature") { res =>
    implicit val (ks, j, h, sp, hs) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      info = mkInfo(SortedMap(addrA -> Balance(NonNegLong(100L))))
      snapshot <- mkSnapshot(info)
      signed <- forAsyncHasher(snapshot, keyPair)
      // Tamper the signed body AFTER signing: the proof is still bound to the original hash, so the recomputed hash
      // over the mutated value no longer verifies. (Mutating `height` leaves the stateProof field untouched, isolating
      // the failure to gate 1.)
      forged = Signed(signed.value.copy(height = Height(NonNegLong(999L))), signed.proofs)
      verdict <- NakamotoSyncDaemon.verifyCatchUpSnapshot[IO](forged, info)
    } yield
      verdict match {
        case NakamotoSyncDaemon.CatchUpVerdict.RejectedInvalidSignature => success
        case other                                                      => failure(s"expected RejectedInvalidSignature, got $other")
      }
  }

  test("validly-signed snapshot paired with a different GlobalSnapshotInfo is RejectedStateProofMismatch") { res =>
    implicit val (ks, j, h, sp, hs) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      honestInfo = mkInfo(SortedMap(addrA -> Balance(NonNegLong(100L))))
      snapshot <- mkSnapshot(honestInfo)
      signed <- forAsyncHasher(snapshot, keyPair)
      // Attacker-chosen state: an extra balance entry ⇒ rebuilt stateProof ≠ the snapshot's committed stateProof.
      // The signature is still valid (snapshot body untouched), so gate 1 passes and gate 2 must catch it.
      tamperedInfo = mkInfo(SortedMap(addrA -> Balance(NonNegLong(100L)), addrB -> Balance(NonNegLong(1L))))
      verdict <- NakamotoSyncDaemon.verifyCatchUpSnapshot[IO](signed, tamperedInfo)
    } yield
      verdict match {
        case NakamotoSyncDaemon.CatchUpVerdict.RejectedStateProofMismatch(hashed) =>
          expect(hashed.signed.value.ordinal === SnapshotOrdinal(NonNegLong(1L)))
        case other => failure(s"expected RejectedStateProofMismatch, got $other")
      }
  }
}
