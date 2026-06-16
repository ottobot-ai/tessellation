package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.currency.schema.globalSnapshotSync.{GlobalSnapshotSync, GlobalSnapshotSyncOrdinal}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.cluster.SessionToken
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.{Signed, signature}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Step-6 determinism core (`docs/nakamoto/COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md`): the producer computes a minimal
  * [[ChangeSet.currencyInfoChangeSet]] over `(S(N), next)` and every gl0 verifier reconstructs `next` from `(S(N), diff)` via
  * [[ChangeSet.reconstructInfoFromDiff]] WITHOUT re-executing the metagraph. This suite is the round-trip proof — `reconstruct(S(N),
  * changeSet(S(N), next)) === next` — over the all-`Some` post-tess3 normal form `reconstructCurrencyInfoFrom` emits. If this holds, a
  * verifier holding the producer's `S(N)` reaches the byte-identical `next` (and thus the same PIN-1 root); the only way to diverge is a
  * different `S(N)` (trim/reorg), which the root-verify rejects.
  *
  * Covers PIN-2 (fieldId-7 `activeAllowSpends` participates in the diff/apply, not just the 8 `Mg*`), PIN-3 (a no-op transition yields an
  * EMPTY diff — minimality), and the accumulation property the run-24/26 fix is about (allow-spends GROW across the diff, never reset).
  */
object CurrencyDiffRoundTripSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val testProofs = NonEmptySet.one(signature.SignatureProof(Id(Hex("")), signature.Signature(Hex(""))))

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private def mkTokenLock(source: Address, label: String): Signed[TokenLock] =
    Signed(
      TokenLock(
        source,
        TokenLockAmount(PosLong(200L)),
        TokenLockFee(NonNegLong(0L)),
        TokenLockReference(TokenLockOrdinal(NonNegLong(0L)), testHash(s"tl-parent-$label")),
        None,
        EpochProgress(NonNegLong(700L)).some,
        None
      ),
      testProofs
    )

  private def mkAllowSpend(source: Address, label: String): Signed[AllowSpend] =
    Signed(
      AllowSpend(
        source = source,
        destination = source,
        currencyId = None,
        amount = SwapAmount(PosLong(100L)),
        fee = AllowSpendFee(NonNegLong(0L)),
        parent = AllowSpendReference(AllowSpendOrdinal(NonNegLong(0L)), testHash(s"as-parent-$label")),
        lastValidEpochProgress = EpochProgress(NonNegLong(500L)),
        approvers = List.empty
      ),
      testProofs
    )

  private def mkCurrencyMessage(addr: Address, mgAddr: Address): Signed[CurrencyMessage] =
    Signed(CurrencyMessage(MessageType.Staking, addr, mgAddr, MessageOrdinal(NonNegLong(0L))), testProofs)

  private def mkSync(parentOrd: Long, globalOrd: Long, label: String): Signed[GlobalSnapshotSync] =
    Signed(
      GlobalSnapshotSync(
        GlobalSnapshotSyncOrdinal(NonNegLong.unsafeFrom(parentOrd)),
        SnapshotOrdinal(NonNegLong.unsafeFrom(globalOrd)),
        testHash(s"gss-$label"),
        SessionToken(Generation(PosLong.unsafeFrom(123L)))
      ),
      testProofs
    )

  private val peerA: PeerId = PeerId(Hex("aa" * 64))
  private val peerB: PeerId = PeerId(Hex("bb" * 64))

  /** [[fullInfo]] but with an explicitly populated per-peer `globalSnapshotSyncView` (the field-32 unrolled partition the cluster m1 freeze
    * diverges on). All other partitions stay populated so the diff/reconstruct exercises the field alongside the rest.
    */
  private def fullInfoSync(
    mgAddr: Address,
    holder: Address,
    syncView: SortedMap[PeerId, Signed[GlobalSnapshotSync]]
  ): CurrencySnapshotInfo =
    fullInfo(mgAddr, holder, SortedSet(mkAllowSpend(holder, "a"))).copy(globalSnapshotSyncView = syncView.some)

  private val emptyInfo: CurrencySnapshotInfo =
    CurrencySnapshotInfo(SortedMap.empty, SortedMap.empty, None, None, None, None, None, None, None)

  /** All-`Some` info (the normal form `reconstructCurrencyInfoFrom` returns) with every unrolled partition + fieldId-7 populated. */
  private def fullInfo(mgAddr: Address, holder: Address, allowSpends: SortedSet[Signed[AllowSpend]]): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap(holder -> TransactionReference(TransactionOrdinal(NonNegLong(1L)), testHash("csi-tx"))),
      balances = SortedMap(holder -> Balance(NonNegLong(555L))),
      lastMessages = SortedMap[MessageType, Signed[CurrencyMessage]](MessageType.Staking -> mkCurrencyMessage(holder, mgAddr)).some,
      lastFeeTxRefs = SortedMap(holder -> TransactionReference(TransactionOrdinal(NonNegLong(2L)), testHash("csi-fee"))).some,
      lastAllowSpendRefs = SortedMap(holder -> AllowSpendReference(AllowSpendOrdinal(NonNegLong(3L)), testHash("csi-as"))).some,
      activeAllowSpends = SortedMap(holder -> allowSpends).some,
      globalSnapshotSyncView = Some(SortedMap.empty[PeerId, Signed[GlobalSnapshotSync]]),
      lastTokenLockRefs = SortedMap(holder -> TokenLockReference(TokenLockOrdinal(NonNegLong(4L)), testHash("csi-tlr"))).some,
      activeTokenLocks = SortedMap(holder -> SortedSet(mkTokenLock(holder, "x"))).some
    )

  private def addrs(implicit sp: SecurityProvider[IO]): IO[(Address, Address)] =
    (KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress), KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)).tupled

  /** The PIN-1 infoRoot PREIMAGE for one MG's info as a structurally-comparable map (`Array[Byte]` is reference-equal, so key to Hex string
    * and value to `Seq[Byte]`). This is exactly the 8 `Mg*` byte set `currencySnapshotFieldRoots` hashes into the infoRoot, so equal
    * preimage ⇔ equal infoRoot; and it is Some/None-invisible (`infoEntryBytes.opt`), so the all-`Some` reconstruction normal form is OK.
    */
  private def bytesKeyed(mgAddr: Address, info: CurrencySnapshotInfo)(implicit h: Hasher[IO]): IO[Map[String, Seq[Byte]]] =
    GlobalStateConverter.infoEntryBytes[IO](mgAddr, info).flatMap {
      _.traverse { case (k, v) => GlobalStateKey.toHex[IO](k).map(_.value -> v.toSeq) }.map(_.toMap)
    }

  /** The CONSENSUS `infoRoot` PREIMAGE — exactly the subset of [[bytesKeyed]] whose `fieldId ∈ infoSubFields` (what
    * `currencySnapshotFieldRoots` / `mptStateProofFromBytes` hash into the per-MG `infoRoot`). Equal preimage ⇔ equal `infoRoot` (the MPT
    * root is a pure function of the hex-key→bytes set). Distinct from [[bytesKeyed]] (the FULL stored set) precisely so a test can assert a
    * field is STORED yet NOT consensus-rooted.
    */
  private def infoRootKeyed(mgAddr: Address, info: CurrencySnapshotInfo)(implicit h: Hasher[IO]): IO[Map[String, Seq[Byte]]] =
    GlobalStateConverter.infoEntryBytes[IO](mgAddr, info).flatMap {
      _.filter { case (k, _) => GlobalStateFieldId.infoSubFields.contains(k.fieldId) }.traverse {
        case (k, v) => GlobalStateKey.toHex[IO](k).map(_.value -> v.toSeq)
      }
        .map(_.toMap)
    }

  test("round-trip core: reconstruct(prior, changeSet(prior, next)) === next (all 8 Mg* + fieldId-7)") { res =>
    implicit val (h, sp, _) = res
    for {
      (mgAddr, holder) <- addrs
      prior = fullInfo(mgAddr, holder, SortedSet(mkAllowSpend(holder, "a")))
      next = fullInfo(mgAddr, holder, SortedSet(mkAllowSpend(holder, "a"), mkAllowSpend(holder, "b")))
      diff <- ChangeSet.currencyInfoChangeSet[IO](mgAddr, prior, next)
      reconstructed <- ChangeSet.reconstructInfoFromDiff[IO](mgAddr, prior, diff)
    } yield expect(clue(reconstructed) == clue(next))
  }

  test("genesis: reconstruct(emptyInfo, changeSet(emptyInfo, next)) === next (empty prior, full upsert)") { res =>
    implicit val (h, sp, _) = res
    for {
      (mgAddr, holder) <- addrs
      next = fullInfo(mgAddr, holder, SortedSet(mkAllowSpend(holder, "a")))
      diff <- ChangeSet.currencyInfoChangeSet[IO](mgAddr, emptyInfo, next)
      reconstructed <- ChangeSet.reconstructInfoFromDiff[IO](mgAddr, emptyInfo, diff)
    } yield
      expect.all(
        clue(reconstructed) == clue(next),
        // genesis diff has no removals and at least one upsert per populated partition
        clue(diff.removals).isEmpty,
        clue(diff.upserts).nonEmpty
      )
  }

  test("PIN-3 minimality: changeSet(x, x) is empty; reconstruct(x, empty) === x") { res =>
    implicit val (h, sp, _) = res
    for {
      (mgAddr, holder) <- addrs
      x = fullInfo(mgAddr, holder, SortedSet(mkAllowSpend(holder, "a")))
      diff <- ChangeSet.currencyInfoChangeSet[IO](mgAddr, x, x)
      reconstructed <- ChangeSet.reconstructInfoFromDiff[IO](mgAddr, x, diff)
    } yield
      expect.all(
        clue(diff).isEmpty,
        clue(reconstructed) == clue(x)
      )
  }

  test("PIN-2 accumulation: allow-spend added at an existing holder rides the diff and accumulates (run-24/26 fix)") { res =>
    implicit val (h, sp, _) = res
    for {
      (mgAddr, holder) <- addrs
      a = mkAllowSpend(holder, "a")
      b = mkAllowSpend(holder, "b")
      prior = fullInfo(mgAddr, holder, SortedSet(a))
      next = fullInfo(mgAddr, holder, SortedSet(a, b))
      diff <- ChangeSet.currencyInfoChangeSet[IO](mgAddr, prior, next)
      reconstructed <- ChangeSet.reconstructInfoFromDiff[IO](mgAddr, prior, diff)
      reAllowSpends = reconstructed.activeAllowSpends.flatMap(_.get(holder)).getOrElse(SortedSet.empty[Signed[AllowSpend]])
    } yield
      expect.all(
        // the fieldId-7 holder entry changed, so the diff carries it (PIN-2) — not silently dropped
        clue(diff.upserts).nonEmpty,
        // both allow-spends survive the apply: A (carried from S(N)) + B (new) — accumulation, never reset to window-only
        clue(reAllowSpends) == clue(SortedSet(a, b)),
        clue(reconstructed) == clue(next)
      )
  }

  test("removal: an allow-spend holder dropped in `next` is removed by the diff (no stale key survives)") { res =>
    implicit val (h, sp, _) = res
    for {
      (mgAddr, holder) <- addrs
      prior = fullInfo(mgAddr, holder, SortedSet(mkAllowSpend(holder, "a")))
      next = prior.copy(activeAllowSpends = SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]].some)
      diff <- ChangeSet.currencyInfoChangeSet[IO](mgAddr, prior, next)
      reconstructed <- ChangeSet.reconstructInfoFromDiff[IO](mgAddr, prior, diff)
    } yield
      expect.all(
        clue(diff.removals).nonEmpty,
        // the holder's fieldId-7 entry is gone after apply
        clue(reconstructed.activeAllowSpends.getOrElse(SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])).isEmpty
      )
  }

  /** PIN-4 prior SYMMETRY (the genesis-seam split guard — the exact divergence Finding 1 fixed).
    *
    * On a metagraph's genesis→first-incremental window the PRODUCER computes the diff against `emptyInfo` (its diff prior is
    * `getCurrencySnapshotInfo`, gated on the fieldId-5 incremental which is absent at genesis — see
    * `ShardCheckpointWiring.reExecDerivationWithDiff`). The gl0 VERIFIER must apply that diff against the byte-IDENTICAL prior, so
    * `GSAM.deriveAdoptedCurrencyState.priorInfoOf` resolves the `Left(genesis)` arm to `emptyInfo` too — NOT the genesis snapshot's
    * embedded `CurrencySnapshotInfo` (which carries the non-empty genesis `balances`).
    *
    * This test models a genesis-funded `holder` FULLY DRAINED in window-0 (absent from `next`). The producer's diff (vs `emptyInfo`) has NO
    * removal for `holder` (its prior was empty). Applying it onto the genesis prior (non-empty `balances` at `holder`) leaves the stale
    * `holder` balance in the reconstruction ⇒ `=!= next` ⇒ PIN-1 infoRoot mismatch ⇒ permanent per-MG freeze. Applying it onto `emptyInfo`
    * (the fix) ⇒ `=== next`. So this asserts BOTH: the correct (empty) prior agrees, and the genesis prior would NOT — pinning the
    * contract.
    */
  test("PIN-4 genesis-seam: apply-prior MUST mirror the producer's emptyInfo diff-prior, not the genesis embedded info") { res =>
    implicit val (h, sp, _) = res
    for {
      (mgAddr, holder) <- addrs
      other <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      // The genesis embedded info the gl0 `Left(genesis)` arm would have used (non-empty genesis allocation at `holder`).
      genesisInfo = CurrencySnapshotInfo(
        lastTxRefs = SortedMap.empty[Address, TransactionReference],
        balances = SortedMap(holder -> Balance(NonNegLong(1000L))),
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
      // window-0 result: `holder` fully drained (absent), value moved to `other`. The producer derives this from its empty diff-prior.
      next = CurrencySnapshotInfo(
        lastTxRefs = SortedMap.empty[Address, TransactionReference],
        balances = SortedMap(other -> Balance(NonNegLong(1000L))),
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
      // Producer's diff: prior = emptyInfo (the documented genesis diff-prior). No removal for `holder` — its prior was empty.
      diff <- ChangeSet.currencyInfoChangeSet[IO](mgAddr, emptyInfo, next)
      // gl0 with the CORRECT (fix) prior — `emptyInfo` — reaches `next`.
      reconstructedFixed <- ChangeSet.reconstructInfoFromDiff[IO](mgAddr, emptyInfo, diff)
      // gl0 with the OLD (buggy) prior — the genesis embedded info — drags the drained `holder` balance forward, diverging from `next`.
      reconstructedBuggy <- ChangeSet.reconstructInfoFromDiff[IO](mgAddr, genesisInfo, diff)
      // The PIN-1 infoRoot preimage: `infoEntryBytes` IS what `currencySnapshotFieldRoots`' infoRoot is computed over (and it is
      // Some/None-INVISIBLE — `opt` maps both None and Some(empty) to no entries, so the all-`Some` reconstruction normal form does not
      // perturb it). Equal preimage ⇒ equal attested-vs-recomputed root (PIN-1 passes); unequal ⇒ root mismatch (PIN-1 drops).
      fixedBytes <- bytesKeyed(mgAddr, reconstructedFixed)
      nextBytes <- bytesKeyed(mgAddr, next)
      buggyBytes <- bytesKeyed(mgAddr, reconstructedBuggy)
    } yield
      expect.all(
        clue(diff.removals).isEmpty, // empty prior ⇒ no removals, only upserts — this is exactly why the genesis prior desyncs
        // the implemented GSAM prior (emptyInfo) is split-safe: same infoRoot preimage as the producer's `next` ⇒ PIN-1 passes
        clue(fixedBytes) == clue(nextBytes),
        // the OLD genesis-info prior WOULD diverge: the drained `holder` balance survives, so its infoRoot preimage differs (regression guard)
        clue(buggyBytes) != clue(nextBytes),
        clue(reconstructedBuggy.balances.get(holder)).contains(Balance(NonNegLong(1000L))) // stale drained key survives under the bug
      )
  }

  /** ENCODING discriminator (the cluster-m1-freeze, field-32 `MgGlobalSnapshotSyncView`). A populated multi-peer `globalSnapshotSyncView`
    * must round-trip byte-identically when the verifier holds the SAME prior the producer diffed against. GREEN here proves the
    * diff/reconstruct ENCODING is faithful for a populated sync-view (so a cluster divergence is a PRIOR mismatch, not a lossy codec); RED
    * would localize the freeze to the encoder itself.
    */
  test("populated sync-view round-trip (identical prior): a new peer's sync rides the diff, reconstruct === next") { res =>
    implicit val (h, sp, _) = res
    for {
      (mgAddr, holder) <- addrs
      prior = fullInfoSync(mgAddr, holder, SortedMap(peerA -> mkSync(1L, 100L, "a")))
      next = fullInfoSync(mgAddr, holder, SortedMap(peerA -> mkSync(1L, 100L, "a"), peerB -> mkSync(1L, 101L, "b")))
      diff <- ChangeSet.currencyInfoChangeSet[IO](mgAddr, prior, next)
      reconstructed <- ChangeSet.reconstructInfoFromDiff[IO](mgAddr, prior, diff)
      rBytes <- bytesKeyed(mgAddr, reconstructed)
      nBytes <- bytesKeyed(mgAddr, next)
    } yield
      expect.all(
        clue(diff.upserts).nonEmpty, // the new peer's sync entry rides the diff
        clue(reconstructed) == clue(next),
        clue(rBytes) == clue(nBytes) // ⇒ identical infoRoot preimage ⇒ PIN-1 passes
      )
  }

  /** The cluster m1 freeze, REPRODUCED (run-2x sharded e2e: gl0 ord 465+, `diff(upserts=1,removals=0)`, attested≠recomputed forever). The
    * producer diffs `next` (peerA's sync UPDATED) against ITS prior {peerA}; the emitted diff is a single sync-view upsert with NO
    * removals. A follower whose prior ALSO carries a STALE peerB (drift the per-field adopt gate at
    * GlobalSnapshotStateChannelEventsProcessor:588 can introduce — committed sync-view is observation-dependent / split across the
    * producer's full-committee vs a re-derive's 2/3 signers, #259) applies that diff and KEEPS peerB ⇒ its reconstructed infoRoot preimage
    * diverges from the attested root ⇒ the MG is withheld every ordinal ⇒ permanent freeze. This is the field-32 analogue of the PIN-4
    * balances guard above.
    */
  test("sync-view freeze repro: a removals-free diff cannot evict a follower-prior-only stale peer ⇒ root diverges") { res =>
    implicit val (h, sp, _) = res
    for {
      (mgAddr, holder) <- addrs
      producerPrior = fullInfoSync(mgAddr, holder, SortedMap(peerA -> mkSync(1L, 100L, "a")))
      next = fullInfoSync(mgAddr, holder, SortedMap(peerA -> mkSync(2L, 102L, "a2")))
      // follower prior == producer prior PLUS a stale peerB (the observation-dependent drift)
      followerPrior = fullInfoSync(mgAddr, holder, SortedMap(peerA -> mkSync(1L, 100L, "a"), peerB -> mkSync(1L, 101L, "b")))
      diff <- ChangeSet.currencyInfoChangeSet[IO](mgAddr, producerPrior, next)
      reconstructed <- ChangeSet.reconstructInfoFromDiff[IO](mgAddr, followerPrior, diff)
      rBytes <- bytesKeyed(mgAddr, reconstructed)
      nBytes <- bytesKeyed(mgAddr, next)
    } yield
      expect.all(
        clue(diff.removals).isEmpty, // exactly the cluster's `removals=0`
        clue(diff.upserts).size == 1, // exactly the cluster's `upserts=1` (peerA's updated sync)
        clue(rBytes) != clue(nBytes), // stale peerB survives ⇒ attested≠recomputed ⇒ the permanent freeze
        clue(reconstructed.globalSnapshotSyncView.flatMap(_.get(peerB))).isDefined
      )
  }

  /** THE FIX (cause-2): `globalSnapshotSyncView` (field-32 `MgGlobalSnapshotSyncView`) is observation-dependent — the producer accumulates
    * it under the full consensus committee, a re-deriving verifier under the 2/3 signers (#259), so honest nodes hold DIFFERENT per-peer
    * maps and no minimal diff reconciles them (proved by the freeze-repro above). gl0 does NOT consume the metagraph's view of gl0-syncs,
    * so it is excluded from the per-MG consensus `infoRoot` (dropped from `GlobalStateFieldId.infoSubFields`) — the #116 pattern
    * (path-dependent state stays STORED but leaves the consensus root). The field is still written/diffed/reconstructed; the metagraph's
    * OWN `CurrencySnapshotInfo.stateProof` still commits to it. This asserts: two infos differing ONLY in the sync-view have an IDENTICAL
    * consensus `infoRoot` preimage (so the m1 freeze cannot occur) while their FULL stored preimage still differs (the field is not dropped
    * from storage). RED before the fix (field-32 in `infoSubFields` ⇒ roots differ), GREEN after.
    */
  test("FIX: sync-view divergence cannot wedge the per-MG infoRoot — field-32 excluded from the consensus root, still stored") { res =>
    implicit val (h, sp, _) = res
    for {
      (mgAddr, holder) <- addrs
      // identical in EVERY partition except globalSnapshotSyncView (a divergent observation-dependent peer set)
      a = fullInfoSync(mgAddr, holder, SortedMap(peerA -> mkSync(1L, 100L, "a")))
      b = fullInfoSync(mgAddr, holder, SortedMap(peerA -> mkSync(1L, 100L, "a"), peerB -> mkSync(1L, 101L, "b")))
      aRoot <- infoRootKeyed(mgAddr, a)
      bRoot <- infoRootKeyed(mgAddr, b)
      aFull <- bytesKeyed(mgAddr, a)
      bFull <- bytesKeyed(mgAddr, b)
    } yield
      expect.all(
        clue(aFull) != clue(bFull), // the sync-view IS still stored (full preimage differs) — not silently dropped from storage
        clue(aRoot) == clue(bRoot) // but the CONSENSUS infoRoot preimage is identical ⇒ no per-MG root divergence ⇒ no freeze
      )
  }
}
