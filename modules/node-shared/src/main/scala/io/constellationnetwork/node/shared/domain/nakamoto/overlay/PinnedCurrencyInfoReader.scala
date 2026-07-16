package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps.GlobalStateReaderTypedOps
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.{Hashed, Hasher}

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Track-1 blocker-2a — a version-retained, exact-reference per-metagraph `CurrencySnapshotInfo` reader.
  *
  * Reads a metagraph's global-committed currency state AT A PINNED ANCHOR `(ordinal, expectedGlobalSnapshotHash)`, i.e. the exact global
  * snapshot recorded in a currency snapshot's `globalSyncView`. It replaces the HEAD read (`lastGlobalSnapshotStorage.getCombined`) that
  * today's per-MG derivation performs — HEAD is non-deterministic across nodes and can run ahead of the pinned view, so a re-executing
  * committee/watchtower (Track-1 re-exec primary) and a byteDiff-adopting follower would read a DIFFERENT prior than the metagraph's own
  * `accept()` saw. Pinning to the anchor makes the prior deterministic and Byzantine-checkable.
  *
  * '''HARD-REJECT contract (returns `None`, NEVER a HEAD fallback):'''
  *   1. no global snapshot resolvable at the requested ordinal;
  *   1. the resolved snapshot's ordinal, hash, parent hash, or committed MPT root differs from the signed execution reference;
  *   1. the pinned snapshot carries no committed `stateProof.mptRoot` (nothing to byte-verify against);
  *   1. no version-retained state bytes at `ordinal` (evicted by the backing store's retention — see below);
  *   1. the retained bytes' consensus `consensusMptRoot` ≠ the pinned snapshot's `stateProof.mptRoot` (bytes do not reproduce the pinned
  *      committed root).
  *
  * A `None` from a clean verify (the metagraph simply had no reconstructible state at the verified anchor) is indistinguishable from a
  * reject ON THE `Option` METHODS by design: a consensus consumer treats every `None` as "no pinned prior — hard reject", never as "read
  * HEAD instead". Checkpoint recreation MUST distinguish that from a VERIFIED anchor at which a brand-new metagraph simply has no committed
  * state and therefore starts at genesis. That consumer uses the THREE-VALUED [[readAtExecutionBaseVerified]]
  * ([[PinnedCurrencyInfoReader.PinnedAnchorRead]]); every other consumer keeps the collapsed `Option` view unchanged.
  *
  * '''Retention (version depth this reader can serve depends ENTIRELY on the injected `byteStore`):'''
  *   - gl0 produce/validate rail: `byteStore` = the `signedBytesStore` (`<mptSnapshotInfoPath>_signed`,
  *     `ContiguousOrdinalCutoff(keepDepthBehindFinalized)`) — a contiguous local k2 = 100*k1 retention window (raised from stale 512),
  *     whose bytes reproduce the signed `mptRoot`. Missing older bytes hard-reject this read; k2 is not an absolute fork-choice/finality
  *     floor. Objective comparison that needs older state must enter RecoveryRequired and reconstruct exact authenticated history.
  *   - follower `createContext` rail (cl0/dl1): `byteStore` = a read-only view over the producer's `mpt_snapshot_info` store, which prunes
  *     with `LogarithmicOrdinalCutoff` — a SPARSE, gappy retention below the head. By-ordinal reads at an arbitrary past ordinal MISS
  *     unless that ordinal happens to sit on the logarithmic ladder ⇒ this rail hard-rejects most anchors. '''Track-3 S2 decision: the
  *     follower contiguous/disk-backed store is DEFERRED''' — followers do not yet consume deep anchors. It is NOT a silent shallow ship:
  *     this reader HARD-REJECTS a deep follower anchor (returns `None`, never a HEAD fallback), so a follower that cannot serve the anchor
  *     defers/re-pulls rather than adopting wrong bytes. Surfaced here + at the `SharedServices` `pinnedByteStore` wiring, not worked
  *     around.
  *
  * The reader is stateless: each `readAt` loads the retained byte map into a throwaway in-memory `MptStore` and reconstructs ONLY the
  * requested metagraph's `CurrencySnapshotInfo` via the exact same unrolled-partition inverse
  * (`GlobalStateReaderOps.getCurrencySnapshotInfo`) every other read path uses — so the result is byte-identical to a live finalized-reader
  * read of the same committed bytes. No trie is built (reads scan the raw byte map), but the whole global byte map is materialized per
  * call; a consumer that reads the same anchor repeatedly (e.g. once per `accept()`) should cache the result.
  */
trait PinnedCurrencyInfoReader[F[_]] {

  /** Read `metagraphId`'s `CurrencySnapshotInfo` at the pinned global anchor. See the class scaladoc for the hard-reject contract. */
  def readAt(
    ordinal: SnapshotOrdinal,
    expectedGlobalSnapshotHash: Hash,
    metagraphId: Address
  ): F[Option[CurrencySnapshotInfo]]

  /** Read `metagraphId`'s gl0-maintained cross-shard `MetagraphSyncDataInfo` (the fieldId-18 hypergraph partition — the set of unapplied
    * global-change ordinals for this metagraph) at the pinned global anchor.
    *
    * This is the SIBLING of [[readAt]] used by Track-1 I-PIN: `CurrencySnapshotAcceptanceManager.accept` must gate which cross-shard
    * `SpendAction`s it folds off the state committed at the RECORDED `globalSyncView`, not off the node-local head. `MetagraphSyncDataInfo`
    * is NOT part of `CurrencySnapshotInfo` (it lives in `GlobalSnapshotInfo.metagraphSyncData`, MPT fieldId 18, keyed per metagraph
    * address), so it needs its own accessor rather than a field of [[readAt]]'s result. Same pin+verify+hard-reject contract as [[readAt]]
    * (returns `None` on any miss — no head fallback); a clean `None` means the metagraph has no unapplied cross-shard state at the verified
    * anchor.
    */
  def readMetagraphSyncDataAt(
    ordinal: SnapshotOrdinal,
    expectedGlobalSnapshotHash: Hash,
    metagraphId: Address
  ): F[Option[MetagraphSyncDataInfo]]

  /** Exact execution-base per-metagraph read. All four signed reference fields and the retained bytes are verified before reconstruction.
    */
  def readAtExecutionBase(
    executionBase: GlobalSnapshotStateRef,
    metagraphId: Address
  ): F[Option[CurrencySnapshotInfo]]

  /** Track-1 execution-base-pin GENESIS SEAM — the THREE-VALUED sibling of [[readAtExecutionBase]], for the byteDiff-ADOPT consumer ONLY
    * (`GlobalSnapshotAcceptanceManager.pinnedPriorInfoOf`). Same exact-reference resolve + verify contract, but the result disambiguates
    * the two outcomes [[readAtExecutionBase]] collapses into one `None`:
    *
    *   - [[PinnedCurrencyInfoReader.PinnedAnchorRead.AnchorUnreadable]] — the ANCHOR step itself failed (no finalized snapshot resolvable
    *     at `ordinal`, no committed `stateProof.mptRoot`, retained bytes evicted/absent, or bytes that do not recompute the pinned root).
    *     The adopter MUST stay FAIL-CLOSED here: never adopt over a wrong/unverifiable prior.
    *   - `AnchorVerified(None)` — the anchor VERIFIED CLEANLY (retained bytes reproduce the pinned committed root) and this metagraph
    *     simply has NO committed currency state there. Absence under a verified pinned root is itself a pinned fact (MPT non-inclusion), so
    *     every honest node derives it identically — the adopter mirrors the producer's `getOrElse(emptyInfo)` genesis seam
    *     (`ShardCheckpointWiring.reExecDerivationAtPinnedBase`), which is what lets a never-before-adopted metagraph's first checkpoint
    *     onboard at `numShards >= 2`.
    *   - `AnchorVerified(Some(info))` — the anchor verified and the pinned per-MG prior reconstructed.
    *
    * [[readAtExecutionBase]] is exactly this method with `.toOption` applied.
    */
  def readAtExecutionBaseVerified(
    executionBase: GlobalSnapshotStateRef,
    metagraphId: Address
  ): F[PinnedCurrencyInfoReader.PinnedAnchorRead[CurrencySnapshotInfo]]

  /** Track-1 execution-base-pin — a version-retained WHOLE-GLOBAL [[GlobalStateReader]] pinned at the exact referenced snapshot. Where
    * [[readAtExecutionBase]] returns one metagraph's `CurrencySnapshotInfo`, this hands back a full reader over the verified retained bytes
    * so committee/watchtower `reExecDerivationAtPinnedBase` can seed both its derivation prior (`getLastIncrementalCurrencySnapshot` /
    * `getLastCurrencySnapshot`) and its diff prior at the SAME pinned base the producer diffed over. Same exact-reference resolve + verify
    * + hard-reject contract — `None` (never a live/head fallback) if the anchor can't be resolved or its retained bytes are evicted below
    * retention.
    */
  def pinnedReaderAt(executionBase: GlobalSnapshotStateRef): F[Option[GlobalStateReader[F]]]
}

object PinnedCurrencyInfoReader {

  /** Signed-byte-store BACKFILL transport (2026-07-09) — the read-time healer for HOLES in the version-retained byte store.
    *
    * '''Why holes exist at all.''' The signed store's writer is finalize-sink promotion from hash-keyed produce/validate staging. A
    * same-ordinal proposal-race loss can leave the winning candidate's bytes unstaged under the finalized hash, creating a hole at an
    * ordinal this node's canonical chain finalized. When a peer then stamps a checkpoint execution base at such an ordinal (IT has the
    * bytes — its newest persisted), every holed node fail-closes `pinned ANCHOR ... unreadable` on EVERY subsequent checkpoint (observed
    * ×106/×101) and that metagraph's per-MG mirror freezes permanently. Closing each creation site individually is whack-a-mole; the
    * READ-time seam catches all of them by construction.
    *
    * '''Determinism contract (why a peer fetch cannot poison the fold).''' The fetch result is ONLY accepted when
    * `consensusMptRoot(fetched) === the LOCALLY-resolved pinned snapshot's committed stateProof.mptRoot` — the identical check retained
    * store bytes must pass at read time. Before staging, the map is STRIPPED to `GlobalStateKey.consensusRootEntries` (exactly the entry
    * set the root commits), so every retained SystemNamespace economic index is root-bound and field-32 sync-view bytes cannot be imported
    * as peer authority.
    *
    * '''ECO-F32 limitation.''' `reExecDerivationAtPinnedBase` does consume the prior `CurrencySnapshotInfo.globalSnapshotSyncView`, so a
    * stripped backfill is not equivalent to a locally staged map when that view was nonempty. The target currency lane must carry the exact
    * replay witness bound to `CurrencySnapshotStateProof.globalSnapshotSync`; until then, a missing field-32 preimage is an unreadable
    * replay base and must defer rather than synthesize `Some(empty)`. A fetch failure / wrong-root response / in-flight duplicate likewise
    * stays FAIL-CLOSED (`AnchorUnreadable`), never a live-base substitute and never slash evidence.
    *
    * '''No self-reference.''' The transport is a plain HTTP by-ordinal pull (`/global-snapshots/<ord>/mpt-entries`); it performs NO pinned
    * read itself, so backfilling N can never recurse into a pinned miss at N.
    */
  trait PinnedByteBackfill[F[_]] {

    /** Fetch a candidate signed byte map for `ordinal` from a peer. Transport-only: the CALLER (the reader) root-verifies against its own
      * pinned committed root before anything is staged or served. `None` = no peer could serve it this round (fail-closed defer). Must
      * never raise.
      */
    def fetch(ordinal: SnapshotOrdinal): F[Option[Map[Hex, Array[Byte]]]]
  }

  object PinnedByteBackfill {

    /** Wrap a transport with a per-ordinal IN-FLIGHT guard: while one fiber is fetching ordinal N, concurrent misses at N return `None`
      * immediately (fail-closed this round — they re-read the store on their next fold, by which time the winner has staged the verified
      * bytes). Bounds network amplification when many per-MG reads miss the same stamped execution-base simultaneously. Also totalizes the
      * underlying fetch (any raised error → `None`).
      */
    def deduplicated[F[_]: Async](
      underlying: SnapshotOrdinal => F[Option[Map[Hex, Array[Byte]]]]
    ): F[PinnedByteBackfill[F]] =
      cats.effect.Ref.of[F, Set[SnapshotOrdinal]](Set.empty).map { inFlight =>
        new PinnedByteBackfill[F] {
          def fetch(ordinal: SnapshotOrdinal): F[Option[Map[Hex, Array[Byte]]]] =
            inFlight.modify(s => if (s.contains(ordinal)) (s, false) else (s + ordinal, true)).flatMap {
              case false => none[Map[Hex, Array[Byte]]].pure[F]
              case true =>
                Async[F]
                  .guarantee(underlying(ordinal), inFlight.update(_ - ordinal))
                  .handleError(_ => none[Map[Hex, Array[Byte]]])
            }
        }
      }
  }

  /** The THREE-VALUED pinned-read result for the byteDiff-ADOPT consumer (see [[PinnedCurrencyInfoReader.readAtExecutionBaseVerified]]).
    *
    * The split is made at the ANCHOR-VERIFY seam: every branch of `withVerifiedAnchorBytes` that rejects BEFORE the retained bytes are
    * proven to reproduce the pinned committed root is [[PinnedAnchorRead.AnchorUnreadable]]; once the bytes verify, the per-partition
    * reconstruction's own `Option` is carried VERBATIM inside [[PinnedAnchorRead.AnchorVerified]] (`None` = the metagraph has no committed
    * state under the VERIFIED root — a pinned fact, not a failure).
    *
    * `toOption` collapses back to the legacy `Option` view (`AnchorUnreadable → None`, `AnchorVerified(r) → r`), which is how the existing
    * `Option`-shaped methods are implemented — their observable semantics are byte-identical to before this ADT existed.
    */
  sealed trait PinnedAnchorRead[+A] {
    def toOption: Option[A] = this match {
      case PinnedAnchorRead.AnchorUnreadable    => None
      case PinnedAnchorRead.AnchorVerified(res) => res
    }
  }

  object PinnedAnchorRead {

    /** The anchor step failed: no snapshot at the ordinal / hash-pin miss / no committed `mptRoot` / bytes evicted / root mismatch. The
      * consumer must FAIL-CLOSED (drop/defer) — the prior is unverifiable.
      */
    case object AnchorUnreadable extends PinnedAnchorRead[Nothing]

    /** The anchor VERIFIED (retained bytes reproduce the pinned committed root); `result` is the per-MG reconstruction over those verified
      * bytes — `None` iff the metagraph has no committed state at the verified anchor (MPT non-inclusion, cluster-uniform).
      */
    final case class AnchorVerified[+A](result: Option[A]) extends PinnedAnchorRead[A]
  }

  /** @param byteStore
    *   the version-retained, per-ordinal state-bytes store this reader reads at the anchor. Its retention (contiguous depth vs logarithmic
    *   gaps) is exactly the version depth `readAt` can serve — see the class scaladoc's retention section.
    * @param getGlobalSnapshotByOrdinal
    *   resolves a candidate global snapshot at an ordinal. Exact-base methods compare its hash, parent, ordinal, and MPT root against the
    *   caller's reference before reading bytes. The caller owns canonical Phase-2 qualification and freshness: GL0 wires its current
    *   exact-ancestry resolver, while followers currently wire `lastNGlobalSnapshot.getByOrdinal` and therefore do not obtain a lease here.
    * @param backfill
    *   optional read-time HOLE healer (see [[PinnedByteBackfill]]). When `byteStore.readState(ordinal)` MISSES but the pinned snapshot at
    *   `ordinal` (and thus its committed `stateProof.mptRoot`) IS locally resolvable, fetch a candidate byte map from a peer, verify
    *   `consensusMptRoot(fetched) === the pinned root`, STRIP to `GlobalStateKey.consensusRootEntries`, persist into `byteStore`, and serve
    *   the read — healing the hole for every subsequent read. Any failure keeps the exact pre-backfill fail-closed behavior
    *   (`AnchorUnreadable`, store untouched). `None` (the default — all pre-existing wirings) = behavior byte-identical to before this
    *   parameter existed. gl0 wires it ONLY for the reader over the SIGNED byte store (`GlobalSnapshotConsensus.gl0PinnedReader`).
    */
  def make[F[_]: Async: Parallel: Hasher: JsonSerializer](
    byteStore: MptStateStorage[F],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    backfill: Option[PinnedByteBackfill[F]] = None
  ): PinnedCurrencyInfoReader[F] = new PinnedCurrencyInfoReader[F] {

    private val logger = Slf4jLogger.getLoggerFromName[F]("PinnedCurrencyInfoReader")

    def readAt(
      ordinal: SnapshotOrdinal,
      expectedGlobalSnapshotHash: Hash,
      metagraphId: Address
    ): F[Option[CurrencySnapshotInfo]] =
      withVerifiedAnchorBytes(ordinal, expectedGlobalSnapshotHash, s"mg=${metagraphId.show}", "CurrencySnapshotInfo")(
        reconstructPerMgInfo(_, metagraphId)
      )

    def readMetagraphSyncDataAt(
      ordinal: SnapshotOrdinal,
      expectedGlobalSnapshotHash: Hash,
      metagraphId: Address
    ): F[Option[MetagraphSyncDataInfo]] =
      withVerifiedAnchorBytes(ordinal, expectedGlobalSnapshotHash, s"mg=${metagraphId.show}", "MetagraphSyncDataInfo")(
        reconstructMetagraphSyncData(_, metagraphId)
      )

    def readAtExecutionBase(
      executionBase: GlobalSnapshotStateRef,
      metagraphId: Address
    ): F[Option[CurrencySnapshotInfo]] =
      readAtExecutionBaseVerified(executionBase, metagraphId).map(_.toOption)

    def readAtExecutionBaseVerified(
      executionBase: GlobalSnapshotStateRef,
      metagraphId: Address
    ): F[PinnedAnchorRead[CurrencySnapshotInfo]] =
      withVerifiedExecutionBaseBytesR(executionBase, s"mg=${metagraphId.show}", "CurrencySnapshotInfo")(
        reconstructPerMgInfo(_, metagraphId)
      )

    def pinnedReaderAt(executionBase: GlobalSnapshotStateRef): F[Option[GlobalStateReader[F]]] =
      withVerifiedExecutionBaseBytesR(executionBase, "(pinned whole-global reader)", "GlobalStateReader")(bytes =>
        mkPinnedReader(bytes).map(_.some)
      ).map(_.toOption)

    private def withVerifiedExecutionBaseBytesR[A](
      executionBase: GlobalSnapshotStateRef,
      ctx: String,
      what: String
    )(reconstruct: Map[Hex, Array[Byte]] => F[Option[A]]): F[PinnedAnchorRead[A]] = {
      val unreadable: F[PinnedAnchorRead[A]] = (PinnedAnchorRead.AnchorUnreadable: PinnedAnchorRead[A]).pure[F]

      getGlobalSnapshotByOrdinal(executionBase.ordinal).flatMap {
        case Some(snapshot)
            if snapshot.hash === executionBase.hash &&
              snapshot.signed.value.ordinal === executionBase.ordinal &&
              snapshot.signed.value.lastSnapshotHash === executionBase.parentHash &&
              snapshot.signed.value.stateProof.mptRoot.exists(_ === executionBase.mptRoot.value) =>
          withVerifiedAnchorBytesR(executionBase.ordinal, executionBase.hash, ctx, what)(reconstruct)
        case _ =>
          logger.debug(
            s"[execution-base-pin] exact execution base unavailable/mismatched ord=${executionBase.ordinal.show} " +
              s"hash=${executionBase.hash.show} parent=${executionBase.parentHash.show} root=${executionBase.mptRoot.show} — " +
              s"hard-reject $what for $ctx"
          ) >> unreadable
      }
    }

    /** Pin an ordinal+hash request to the exact resolved snapshot, verify that retained bytes reproduce its committed `mptRoot`, and hand
      * them to `reconstruct` — the collapsed `Option` view of [[withVerifiedAnchorBytesR]]. Returns `None` (NEVER a HEAD fallback) on any
      * miss, including both anchor failures and a clean-verify-but-absent reconstruction. [[readAt]] and [[readMetagraphSyncDataAt]] use
      * this legacy two-field entry point. Until the live Phase-2 lease and post-replay `commitIfCurrent` exist, the four-field
      * execution-base entry points deliberately re-enter here after their initial exact-reference validation so a changed or unavailable
      * canonical resolver fails closed before retained bytes are accepted.
      */
    private def withVerifiedAnchorBytes[A](
      ordinal: SnapshotOrdinal,
      expectedGlobalSnapshotHash: Hash,
      ctx: String,
      what: String
    )(reconstruct: Map[Hex, Array[Byte]] => F[Option[A]]): F[Option[A]] =
      withVerifiedAnchorBytesR(ordinal, expectedGlobalSnapshotHash, ctx, what)(reconstruct).map(_.toOption)

    /** THREE-VALUED core of the pin+verify contract — the anchor-vs-absent split is made HERE. Every reject BEFORE the retained bytes are
      * proven to reproduce the pinned committed root is `AnchorUnreadable` (no resolvable snapshot at `ordinal`, hash ≠ the pinned hash — a
      * fork's snapshot or one this node can't resolve, no committed `mptRoot` (BFT/pre-MPT), retained bytes evicted/absent, or retained
      * bytes that do not recompute the pinned root); once the bytes VERIFY, the reconstruction's own `Option` is carried verbatim as
      * `AnchorVerified(_)` (`None` = nothing committed for the requested partition under the VERIFIED root — a pinned fact). NEVER a HEAD
      * fallback on any path. `ctx` is a human label for the log lines.
      */
    private def withVerifiedAnchorBytesR[A](
      ordinal: SnapshotOrdinal,
      expectedGlobalSnapshotHash: Hash,
      ctx: String,
      what: String
    )(reconstruct: Map[Hex, Array[Byte]] => F[Option[A]]): F[PinnedAnchorRead[A]] = {
      val unreadable: F[PinnedAnchorRead[A]] = (PinnedAnchorRead.AnchorUnreadable: PinnedAnchorRead[A]).pure[F]
      getGlobalSnapshotByOrdinal(ordinal).flatMap {
        case Some(snap) if snap.hash === expectedGlobalSnapshotHash =>
          snap.signed.value.stateProof.mptRoot match {
            case None =>
              logger.debug(
                s"[2a] pinned snapshot ord=${ordinal.show} carries no committed mptRoot (BFT/pre-MPT) — hard-reject $what for $ctx"
              ) >> unreadable
            case Some(expectedMptRoot) => withVerifiedRetainedBytesR(ordinal, expectedMptRoot, ctx, what)(reconstruct)
          }
        case _ =>
          logger.debug(
            s"[2a] no canonical snapshot at pinned ord=${ordinal.show} matching hash=${expectedGlobalSnapshotHash.show} — hard-reject $what for $ctx"
          ) >> unreadable
      }
    }

    /** Verify retained bytes against the root from the immediately preceding hash-matching resolver observation. The execution-base path
      * currently reaches this only after its initial four-field check and a second hash-currentness check. Do not collapse those
      * observations until replay has a mandatory post-work `commitIfCurrent`; otherwise a density replacement can turn an orphaned base
      * into a signature.
      */
    private def withVerifiedRetainedBytesR[A](
      ordinal: SnapshotOrdinal,
      expectedMptRoot: Hash,
      ctx: String,
      what: String
    )(reconstruct: Map[Hex, Array[Byte]] => F[Option[A]]): F[PinnedAnchorRead[A]] = {
      val unreadable: F[PinnedAnchorRead[A]] = (PinnedAnchorRead.AnchorUnreadable: PinnedAnchorRead[A]).pure[F]

      byteStore.readState(ordinal).flatMap {
        case None =>
          // Retained state bytes evicted/absent at the anchor (store retention doesn't reach this depth, or a creation-side
          // staging race left a HOLE at a locally-finalized ordinal). Before hard-rejecting, try the read-time peer BACKFILL —
          // the pinned snapshot at `ordinal` DID resolve (we hold `expectedMptRoot`, the local verification anchor), so a
          // peer-served byte map is acceptable iff it reproduces that committed root. See [[PinnedByteBackfill]] for the
          // determinism contract. No backfill wired / fetch miss / wrong root ⇒ the exact pre-existing fail-closed reject.
          backfill match {
            case None =>
              logger.debug(
                s"[2a] no retained state bytes at pinned ord=${ordinal.show} (evicted/absent) — hard-reject $what for $ctx"
              ) >> unreadable
            case Some(bf) =>
              bf.fetch(ordinal).flatMap {
                case None =>
                  logger.debug(
                    s"[2a] no retained state bytes at pinned ord=${ordinal.show} and peer backfill unavailable — " +
                      s"hard-reject $what for $ctx (fail-closed defer)"
                  ) >> unreadable
                case Some(fetched) =>
                  // STRIP to the consensus entry set FIRST: the root is computed exactly over `consensusRootEntries`, so the
                  // verify below covers every byte we would persist — no root-excluded peer byte can survive.
                  val stripped = io.constellationnetwork.schema.mpt.GlobalStateKey.consensusRootEntries(fetched)
                  GlobalSnapshotInfo.consensusMptRoot[F](stripped).flatMap { fetchedRoot =>
                    if (fetchedRoot =!= expectedMptRoot)
                      // Peer bytes do NOT reproduce the locally-pinned committed root (fork / corrupt / stale peer) —
                      // hard-reject and leave the store UNTOUCHED (never stage unverified bytes).
                      logger.warn(
                        s"[2a][backfill] peer bytes at pinned ord=${ordinal.show} recompute mptRoot=${fetchedRoot.show} ≠ " +
                          s"pinned stateProof.mptRoot=${expectedMptRoot.show} — REJECTED (store untouched), hard-reject $what for $ctx"
                      ) >> unreadable
                    else
                      // VERIFIED against the local anchor — persist the stripped (root-determined, cluster-uniform) map so
                      // every subsequent read at this ordinal is a plain store hit, then serve THIS read from it.
                      byteStore.writeState(ordinal, stripped) >>
                        logger.info(
                          s"[2a][backfill] HEALED hole at pinned ord=${ordinal.show}: peer bytes verified === committed " +
                            s"mptRoot=${expectedMptRoot.show.take(12)} (${stripped.size} consensus entries staged) — serving $what for $ctx"
                        ) >>
                        reconstruct(stripped).map(PinnedAnchorRead.AnchorVerified(_): PinnedAnchorRead[A])
                  }
              }
          }
        case Some(bytes) =>
          GlobalSnapshotInfo.consensusMptRoot[F](bytes).flatMap { computedRoot =>
            if (computedRoot =!= expectedMptRoot)
              // Retained bytes do NOT reproduce the pinned snapshot's committed root (wrong branch / corrupt) — hard-reject.
              logger.debug(
                s"[2a] retained bytes at pinned ord=${ordinal.show} recompute mptRoot=${computedRoot.show} ≠ pinned " +
                  s"stateProof.mptRoot=${expectedMptRoot.show} — hard-reject $what for $ctx"
              ) >> unreadable
            else
              // ANCHOR VERIFIED — the reconstruction's own Option is now a pinned fact (Some = the committed value, None = the
              // requested partition has nothing committed under this verified root), NOT a failure.
              reconstruct(bytes).map(PinnedAnchorRead.AnchorVerified(_): PinnedAnchorRead[A])
          }
      }
    }

    /** Load the verified byte map into a throwaway in-memory store and expose it as a whole-global [[GlobalStateReader]] — the SAME
      * `fromMptStore` adapter every finalized-reader path uses, so a `reExecDerivationAtPinnedBase` replay seeded from this reader is
      * byte-identical to one seeded from the live finalized base at the same ordinal.
      */
    private def mkPinnedReader(bytes: Map[Hex, Array[Byte]]): F[GlobalStateReader[F]] =
      for {
        producer <- InMemoryMerklePatriciaProducer.make[F](bytes)
        store <- MptStore.make[F, GlobalStateKey](producer, GlobalStateKey.toHex[F])
      } yield GlobalStateReader.fromMptStore[F](store)

    /** Load the verified byte map into a throwaway in-memory store and reconstruct ONLY `metagraphId`'s `CurrencySnapshotInfo`. Reuses the
      * exact unrolled-partition inverse every finalized-reader path uses, so the result is byte-identical to a live read of these bytes.
      */
    private def reconstructPerMgInfo(
      bytes: Map[Hex, Array[Byte]],
      metagraphId: Address
    ): F[Option[CurrencySnapshotInfo]] =
      for {
        producer <- InMemoryMerklePatriciaProducer.make[F](bytes)
        store <- MptStore.make[F, GlobalStateKey](producer, GlobalStateKey.toHex[F])
        info <- GlobalStateReader.fromMptStore[F](store).getCurrencySnapshotInfo(metagraphId)
      } yield info

    /** Load the verified byte map into a throwaway in-memory store and read ONLY `metagraphId`'s fieldId-18 `MetagraphSyncDataInfo`
      * (`GlobalStateConverter.syntax`'s `getMetagraphSyncData` — the exact hypergraph-partition accessor every finalized reader uses), so
      * the result is byte-identical to a live read of these bytes.
      */
    private def reconstructMetagraphSyncData(
      bytes: Map[Hex, Array[Byte]],
      metagraphId: Address
    ): F[Option[MetagraphSyncDataInfo]] =
      for {
        producer <- InMemoryMerklePatriciaProducer.make[F](bytes)
        store <- MptStore.make[F, GlobalStateKey](producer, GlobalStateKey.toHex[F])
        info <- store.getMetagraphSyncData(metagraphId)
      } yield info
  }
}
