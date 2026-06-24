package io.constellationnetwork.currency.l1.domain.snapshot.programs

import cats.Parallel
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.Async
import cats.effect.kernel.Ref
import cats.effect.std.Random
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.dag.l1.domain.address.storage.AddressStorage
import io.constellationnetwork.dag.l1.domain.block.BlockStorage
import io.constellationnetwork.dag.l1.domain.snapshot.programs.DAGSnapshotProcessor.FollowSliceVerificationError
import io.constellationnetwork.dag.l1.domain.snapshot.programs.SnapshotProcessor
import io.constellationnetwork.dag.l1.domain.snapshot.programs.SnapshotProcessor._
import io.constellationnetwork.dag.l1.domain.transaction.{ContextualTransactionValidator, TransactionLimitConfig, TransactionStorage}
import io.constellationnetwork.dag.l1.infrastructure.address.storage.AddressStorage
import io.constellationnetwork.json.{JsonBrotliBinarySerializer, JsonSerializer}
import io.constellationnetwork.node.shared.config.types.{AllowSpendsConfig, LastGlobalSnapshotsSyncConfig, TokenLocksConfig}
import io.constellationnetwork.node.shared.domain.globalAlignment.GlobalL0AlignmentStorage
import io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowMirrorVerifier
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.node.shared.domain.snapshot.Validator
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage._
import io.constellationnetwork.node.shared.domain.swap.{AllowSpendStorage, ContextualAllowSpendValidator}
import io.constellationnetwork.node.shared.domain.tokenlock.{ContextualTokenLockValidator, TokenLockStorage}
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.follow._
import io.constellationnetwork.schema.swap.{AllowSpendReference, CurrencyId}
import io.constellationnetwork.schema.tokenLock.TokenLockReference
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.InvalidSignatureForHash
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}

import io.circe.Json
import org.typelevel.log4cats.slf4j.Slf4jLogger

sealed abstract class CurrencySnapshotProcessor[F[_]: Async: Parallel: SecurityProvider: JsonSerializer]
    extends SnapshotProcessor[
      F,
      CurrencySnapshotStateProof,
      CurrencyIncrementalSnapshot,
      CurrencySnapshotInfo
    ]

object CurrencySnapshotProcessor {

  def make[F[_]: Async: Parallel: Random: JsonSerializer: SecurityProvider](
    identifier: Address,
    addressStorage: AddressStorage[F],
    blockStorage: BlockStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastCurrencySnapshotStorage: LastSnapshotStorage[F, CurrencyIncrementalSnapshot, CurrencySnapshotInfo],
    transactionStorage: TransactionStorage[F],
    transactionLimitConfig: TransactionLimitConfig,
    allowSpendsConfig: AllowSpendsConfig,
    tokenLocksConfig: TokenLocksConfig,
    txHasher: Hasher[F],
    allowSpendStorage: AllowSpendStorage[F],
    tokenLockStorage: TokenLockStorage[F],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    l0Service: GlobalL0Service[F],
    globalL0AlignmentStorage: GlobalL0AlignmentStorage[F],
    mptStore: MptStore[F, GlobalStateKey],
    // CUTOVER (mirrors gl1's `DAGSnapshotProcessor`): the follower's verified-mirror Ref `(lastVerifiedTip, ConsumedFieldState)`
    // for the gl0 own-slice follow path (#287 "send diffs"). Created ONCE at the cl1/dl1 construction site (`CurrencyL1App`).
    // `None` on cold start ⇒ next tick fetches the FULL slice; thereafter the incremental diff since the held tip. ANY verify
    // failure RESETS it to `None` so the next tick re-fetches a full from-empty slice — never regresses the safety invariant.
    followMirrorRef: Ref[F, Option[(SnapshotOrdinal, ConsumedFieldState)]]
  )(
    // Captured in the processor instance so `applyGlobalSnapshotFn` can build the cl1/dl1 6th-field (`lastCurrencySnapshots`)
    // check closure, which needs it for `CurrencyIncrementalSnapshot.fromCurrencySnapshot` (genesis `Left` case). The trait's
    // `applyGlobalSnapshotFn` override signature is fixed at `(implicit hasher)`, so the selector is threaded via the closure
    // environment here rather than the method's implicit list.
    implicit globalStateProofSelector: GlobalStateProofSelector
  ): CurrencySnapshotProcessor[F] =
    new CurrencySnapshotProcessor[F] {
      def process(
        snapshot: Either[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo), Hashed[GlobalIncrementalSnapshot]]
      )(
        implicit hasher: Hasher[F],
        stateProofSelector: StateProofSelector,
        withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
      ): F[SnapshotProcessingResult] =
        snapshot match {
          case Left((globalSnapshot, globalState)) =>
            val globalSnapshotReference = SnapshotReference.fromHashedSnapshot(globalSnapshot)
            lastGlobalSnapshotStorage.getCombined.flatMap {
              case None =>
                // COLD BOOTSTRAP. The MPT is seeded by `processCurrencySnapshots` from gl0's SIGNED MPT BYTES (`loadBytes`, PURE) —
                // NOT from a materialized GSI — inside its `Some(Valid(hashedSnapshots))` branch, BEFORE the adopt's `gl0CurrencyView`
                // MPT read AND before `processAlignment` reads the MPT during `checkAlignment`/`DownloadNeeded`. That single seed point
                // (`seedMptFromSignedBytesForAdopt`) covers this cold-bootstrap path AND the steady-state path uniformly. On a
                // sparse-ordinal 404 the seed IDLES (no `syncFromGlobalSnapshotInfo` fallback) and the global follow still advances; the
                // first currency-carrying non-sparse ordinal seeds the MPT and the adopt lands then. Post-cutover the currency path no
                // longer runs `accept()`/`createContext` (re-execution was removed), so the old "accept() reads an empty MPT →
                // StateProofMismatch at ordinal 2" concern does not apply to currency — the adopt only ever READS the loadBytes-populated
                // MPT via the finalized `GlobalStateReader`.
                val setGlobalSnapshot = lastGlobalSnapshotStorage
                  .setInitial(globalSnapshot, globalState)
                  .as[SnapshotProcessingResult](DownloadPerformed(globalSnapshotReference, Set.empty, Set.empty))
                val setNGlobalSnapshots = lastNGlobalSnapshotStorage
                  .setInitialFetchingGL0(globalSnapshot, globalState, l0Service.asLeft.some, none)
                  .as[SnapshotProcessingResult](DownloadPerformed(globalSnapshotReference, Set.empty, Set.empty))

                processCurrencySnapshots(
                  globalSnapshot,
                  globalState,
                  globalSnapshotReference,
                  setGlobalSnapshot,
                  setNGlobalSnapshots,
                  getGlobalSnapshotByOrdinal
                )

              case _ => (new Throwable("unexpected state")).raiseError[F, SnapshotProcessingResult]
            }
          case Right(globalSnapshot) =>
            val globalSnapshotReference = SnapshotReference.fromHashedSnapshot(globalSnapshot)
            lastGlobalSnapshotStorage.getCombined.flatMap {
              case Some((lastGlobalSnapshot, lastGlobalState)) =>
                Validator.compare(lastGlobalSnapshot, globalSnapshot.signed.value) match {
                  case _: Validator.Next =>
                    applyGlobalSnapshotFn(
                      lastGlobalState,
                      lastGlobalSnapshot.signed,
                      globalSnapshot.signed,
                      getGlobalSnapshotByOrdinal
                    ).flatMap { state =>
                      val setGlobalSnapshot = lastGlobalSnapshotStorage
                        .set(globalSnapshot, state)
                        .as[SnapshotProcessingResult](Aligned(globalSnapshotReference, Set.empty))

                      val setNGlobalSnapshots = lastNGlobalSnapshotStorage
                        .set(globalSnapshot, state)
                        .as[SnapshotProcessingResult](DownloadPerformed(globalSnapshotReference, Set.empty, Set.empty))

                      processCurrencySnapshots(
                        globalSnapshot,
                        state,
                        globalSnapshotReference,
                        setGlobalSnapshot,
                        setNGlobalSnapshots,
                        getGlobalSnapshotByOrdinal
                      )

                    }

                  case Validator.NotNext =>
                    // FOLLOW RESYNC-TO-CANONICAL (replaces the old destructive `clear` — the #122 NotNext loop).
                    // The stored snapshot's hash does not chain to the incoming finalized one (the snapshot-rehash
                    // chain is brittle vs gl0's recorded `lastSnapshotHash`). The OLD behavior CLEARED
                    // lastGlobalSnapshotStorage, which left `getOrdinal` = None → BlockService raised
                    // `SnapshotOrdinalUnavailable` → currency block acceptance died → txns stuck WaitingTx →
                    // `getLastProcessedTransaction` (the /transactions/last-reference the harness polls) frozen at
                    // genesis → e2e hung at the L0-token transfer (observed: cl1 0 Aligned / 119 SnapshotIgnored,
                    // perpetual clear→re-download loop). Instead we ADOPT-FORWARD onto the canonical chain, exactly
                    // as ml0's `StateChannel.resyncToCanonical`: pull gl0's authoritative latest (the FULL GSI — never
                    // a partial slice, so the follower lands on COMPLETE state), VERIFY the recomputed MPT root EQUALS
                    // the snapshot's SIGNED `stateProof.mptRoot` (the deterministic trust anchor — `pullLatestSnapshot`
                    // only checks the snapshot hash vs majority, NOT that the served GSI reproduces the signed root),
                    // then `setForRecovery` which keeps storage POPULATED (unlike `clear`, so acceptance stays live).
                    // We NEVER adopt unverified state: on root mismatch we re-pull (bounded), else leave storage
                    // untouched and idle (next tick retries). This bypasses the brittle `Validator.compare` hash-chain
                    // in favor of the mptRoot anchor — symmetric with how ml0 / gl1 already follow. Snapshots already
                    // at-or-below our stored tip are skipped WITHOUT a resync so one batch triggers at most ONE jump.
                    if (globalSnapshot.signed.value.ordinal <= lastGlobalSnapshot.ordinal)
                      Async[F].pure[SnapshotProcessingResult](SnapshotIgnored(globalSnapshotReference))
                    else {
                      val maxResyncPullAttempts = 3
                      Slf4jLogger
                        .getLogger[F]
                        .warn(
                          s"cl1 NotNext on finalized snapshot (#122) at incoming ord=${globalSnapshotReference.ordinal.show} " +
                            s"(stored ord=${lastGlobalSnapshot.ordinal.show}) — resync-to-canonical adopt-forward (setForRecovery, mptRoot-verified)"
                        ) >>
                        1.tailRecM[F, SnapshotProcessingResult] { attempt =>
                          for {
                            // 3c-A: pull gl0's SIGNED MPT byte map (+ snapshot + GSI) and store the bytes VERBATIM via
                            // `loadBytes` (no `syncFromGlobalSnapshotInfo` re-encode → no `recomputed ≠ signed` drift). The
                            // GSI rides along ONLY for `setForRecovery`. The verify gate STAYS as the corruption backstop;
                            // it is folded onto `sidecarFreeMptRoot` (the SAME sidecar-free recompute the signed root uses)
                            // instead of `getRootHashForOrdinal` (which includes the path-dependent SystemNamespace
                            // sidecars), so the compare is apples-to-apples with the signed `stateProof.mptRoot`.
                            canonical <- l0Service.pullLatestMptEntries
                            (canonicalSnapshot, canonicalState, canonicalEntries) = canonical
                            canonicalRef = SnapshotReference.fromHashedSnapshot(canonicalSnapshot)
                            // The bytes are OPTIONAL: gl0's byte route 404s at a sparse combined-checkpoint ordinal, in which
                            // case `pullLatestMptEntries` already degraded to legacy `pullLatestSnapshot` and returns `None`.
                            // On `Some` load the SIGNED bytes VERBATIM (gate below passes by construction); on `None` fall back
                            // to legacy `syncFromGlobalSnapshotInfo`. The verify gate BELOW stays unchanged as the corruption
                            // backstop for the legacy recompute.
                            _ <- canonicalEntries match {
                              case Some(bytes) => mptStore.loadBytes(bytes, canonicalSnapshot.ordinal)
                              case None        => mptStore.syncFromGlobalSnapshotInfo(canonicalState, canonicalSnapshot.ordinal)
                            }
                            afterBytes <- mptStore.underlying.entries
                            recomputedRoot <- GlobalSnapshotInfo.sidecarFreeMptRoot[F](afterBytes).map(_.some)
                            signedRoot = canonicalSnapshot.signed.value.stateProof.mptRoot
                            result <-
                              if (recomputedRoot === signedRoot)
                                lastGlobalSnapshotStorage.setForRecovery(canonicalSnapshot, canonicalState) >>
                                  lastNGlobalSnapshotStorage.setForRecovery(canonicalSnapshot, canonicalState) >>
                                  followMirrorRef.set(none) >>
                                  Slf4jLogger
                                    .getLogger[F]
                                    .info(
                                      s"cl1 resynced to canonical ord=${canonicalSnapshot.ordinal.show}; resuming forward follow next tick"
                                    )
                                    .as((DownloadPerformed(canonicalRef, Set.empty, Set.empty): SnapshotProcessingResult).asRight[Int])
                              else if (attempt < maxResyncPullAttempts)
                                Slf4jLogger
                                  .getLogger[F]
                                  .error(
                                    s"cl1 resync-to-canonical: gl0 served GSI inconsistent with its OWN signed mptRoot at ord=${canonicalSnapshot.ordinal.show} " +
                                      s"(recomputed=${recomputedRoot.map(_.show.take(12)).getOrElse("none")} ≠ signed=${signedRoot.map(_.show.take(12)).getOrElse("none")}); " +
                                      s"NOT adopting, re-pulling (attempt ${attempt + 1}/$maxResyncPullAttempts)"
                                  )
                                  .as((attempt + 1).asLeft[SnapshotProcessingResult])
                              else
                                Slf4jLogger
                                  .getLogger[F]
                                  .error(
                                    s"cl1 resync-to-canonical: gl0 served an inconsistent GSI on all $maxResyncPullAttempts attempts at " +
                                      s"ord=${canonicalSnapshot.ordinal.show}; leaving storage UNCHANGED, idling — next tick retries"
                                  )
                                  .as((SnapshotIgnored(globalSnapshotReference): SnapshotProcessingResult).asRight[Int])
                          } yield result
                        }
                    }
                }
              case None =>
                // Storage was cleared by the NotNext branch above (or by recoverFromOrphan + a
                // subsequent NotNext). The remaining snapshots in this batch can't process —
                // there's no `lastGlobalSnapshot` to validate against. Drain them as
                // SnapshotIgnored so the next `pullGlobalSnapshots` tick bootstraps fresh via
                // the Left branch (line 79). Throwing here stalls dl1 entirely: the
                // performSnapshotsBatchProcessing catch-all logs "Failed to process snapshot,
                // skipping" but lastGlobalSnapshot stays None forever, so /data POSTs return
                // 500 "Last Global Snapshot ordinal not available" and downstream tests fail.
                Async[F].pure[SnapshotProcessingResult](SnapshotIgnored(globalSnapshotReference))
            }
        }

      private val followLogger = Slf4jLogger.getLoggerFromName[F]("io.constellationnetwork.currency.l1.GlobalFollow")

      // CUTOVER (mirrors gl1's `DAGSnapshotProcessor.applyGlobalSnapshotFn`, S3b′): cl1/dl1 no longer re-execute the finalized gl0
      // snapshot (`globalSnapshotContextFns.createContext → GSAM.accept`) to derive the global state. That full re-derivation
      // re-hashes the whole signed `GlobalIncrementalSnapshot` and re-walks the entire global MPT — the path that DEADLOCKED at
      // ordinal 256 with `Signed$InvalidSignatureForHash` (the PULL re-hash) / stormed on `StateProofMismatch` over fields cl1/dl1
      // never read.
      //
      // Instead it FETCHES gl0's latest-finalized consumed-field SLICE (`l0Service.getLatestFollowSlice` / the #287 incremental
      // `getFollowSliceSince`), VERIFIES it by field-root equality against the signed snapshot AT THE SLICE'S OWN ordinal (resolved
      // from cl1/dl1's OWN local lastN store — no peer re-pull), and BUILDS a partial `GlobalSnapshotInfo` populated with the SIX
      // fields cl1/dl1 read: the five Address-keyed consumed fields (balances, lastTxRefs, lastAllowSpendRefs, lastTokenLockRefs,
      // activeTokenLocks) PLUS `lastCurrencySnapshots`. This partial GSI is the GLOBAL-FOLLOW `state` — it is written to the global
      // storage sink (`lastGlobalSnapshotStorage.set` / `lastNGlobalSnapshotStorage.set`), NOT into the MPT. Post-cutover the MPT used by
      // the currency adopt is seeded SEPARATELY and PURELY from gl0's SIGNED MPT BYTES (`processCurrencySnapshots` →
      // `seedMptFromSignedBytesForAdopt` → `loadBytes`), so the per-MG `Mg*` + fieldId-5 currency partitions come from gl0's signed bytes,
      // NOT from this partial GSI (no `syncFromGlobalSnapshotInfo` transit). The 6th field stays PRESENT here only so the global storage
      // sink and any global-state consumer carry the field-root-verified currency map; the currency adopt never reads it. All other GSI
      // fields stay empty.
      //
      // The 6th field carries its SIGNED per-field roots in the live MPT-format `GlobalSnapshotStateProof` field-4 slot
      // (`lastCurrencySnapshotsProof`, the two MPT subtree roots — `LastIncrementalCurrencySnapshots` + `LastCurrencySnapshotInfo`, also
      // covered transitively by the global `mptRoot`). It is verified by a recompute against that SIGNED anchor:
      // `FollowVerifyCore.currencySnapshotsCheck` recomputes both subtree roots of the applied post-state via gl0's EXACT
      // `GlobalStateConverter.currencySnapshotFieldRoots` and asserts delta-application reproduced the signed roots. The five Hash-rooted fields keep their full
      // signed-root cryptographic match (`GlobalFollowMirrorVerifier`); the currency-genesis state's cryptographic trust additionally
      // rests on its downstream `toHashedWithSignatureCheck` (`fetchCurrencySnapshots`) + finalized-GSI sourcing.
      //
      // A verify failure / unavailable slice raises the retryable `FollowSliceVerificationError`, which the batch loop logs + skips
      // WITHOUT `shouldRedownload` (no recovery storm) — cl1/dl1 retry next tick. Same alignment/catch-up semantics as gl1.
      def applyGlobalSnapshotFn(
        lastGlobalState: GlobalSnapshotInfo,
        lastGlobalSnapshot: Signed[GlobalIncrementalSnapshot],
        globalSnapshot: Signed[GlobalIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[GlobalSnapshotInfo] = {
        val verifier = GlobalFollowMirrorVerifier.make[F]

        // The signed per-field roots for the snapshot at the slice's own ordinal, resolved from cl1/dl1's OWN LOCAL state (no peer
        // re-pull): if the slice is at the ordinal being processed (N == M) use the in-hand snapshot's stateProof; otherwise look up
        // the local finalized snapshot at the slice ordinal in `lastNGlobalSnapshotStorage` and take ITS stateProof. `None` (M not
        // yet followed) defers this tick and resolves once the follow reaches M.
        def signedRootsAt(sliceOrdinal: SnapshotOrdinal): F[Option[GlobalSnapshotStateProof]] =
          if (sliceOrdinal === globalSnapshot.value.ordinal)
            globalSnapshot.value.stateProof.some.pure[F]
          else
            lastNGlobalSnapshotStorage.getByOrdinal(sliceOrdinal).map(_.map(_.signed.value.stateProof))

        followMirrorRef.get.flatMap { mirror =>
          val fetch: F[Option[GlobalFollowSliceResponse]] = mirror match {
            case Some((heldTip, _)) => l0Service.getFollowSliceSince(heldTip)
            case None               => l0Service.getLatestFollowSlice
          }
          fetch.flatMap {
            case None =>
              FollowSliceVerificationError(
                s"no follow slice available while processing ordinal=${globalSnapshot.value.ordinal.show}"
              ).raiseError[F, GlobalSnapshotInfo]
            case Some(GlobalFollowSliceResponse(sliceOrdinal, slice, baseOrdinal)) =>
              val prior = mirror match {
                case Some((heldTip, st)) if baseOrdinal.contains(heldTip) => st
                case _                                                    => ConsumedFieldState.empty
              }
              signedRootsAt(sliceOrdinal).flatMap {
                case None =>
                  FollowSliceVerificationError(
                    s"local finalized snapshot at slice ordinal=${sliceOrdinal.show} not yet followed " +
                      s"(processing ordinal=${globalSnapshot.value.ordinal.show}) — deferring slice verify"
                  ).raiseError[F, GlobalSnapshotInfo]
                case Some(stateProof) =>
                  verifier
                    .verifyByFieldRoot(
                      prior,
                      sliceOrdinal,
                      slice,
                      signedFieldRoots(stateProof),
                      // The 6th field is now anchored to the SIGNED currency-snapshots roots in the snapshot's stateProof
                      // (`lastCurrencySnapshotsProof`, the field-4 currency MPT partition roots) — a TRUE Byzantine anchor symmetric with
                      // the five Hash fields, instead of recompute-vs-producer-served-map. `None` (empty currency map) ⇒ both expected roots
                      // are `Hash.empty`.
                      FollowVerifyCore.currencySnapshotsCheck[F](stateProof.lastCurrencySnapshotsProof).some
                    )
                    .flatMap {
                      case Right(verified) =>
                        followMirrorRef.set((sliceOrdinal, verified.value).some) >>
                          consumedFieldsToGlobalSnapshotInfo(verified.value).pure[F]
                      case Left(err) =>
                        followMirrorRef.set(none) >>
                          followLogger.warn(
                            s"follow-slice verify FAILED at sliceOrdinal=${sliceOrdinal.show} " +
                              s"(processing ordinal=${globalSnapshot.value.ordinal.show}, base=${baseOrdinal.map(_.show).getOrElse("none")}): " +
                              s"${describe(err)} — resetting follow mirror, not advancing"
                          ) >>
                          FollowSliceVerificationError(
                            s"follow-slice verify failed at ordinal=${sliceOrdinal.show}: ${describe(err)}"
                          ).raiseError[F, GlobalSnapshotInfo]
                    }
              }
          }
        }
      }

      // CUTOVER (mirrors gl1's `DAGSnapshotProcessor.applyGlobalSnapshotFn` for the CURRENCY-snapshot path): cl1/dl1 no longer
      // RE-EXECUTE each finalized currency snapshot (`currencySnapshotContextFns.createContext → CurrencySnapshotValidator`) to
      // re-derive its `CurrencySnapshotInfo`. That node-local re-derivation could DIVERGE from the metagraph-signed result (a
      // reward-timing balance recompute → `CannotCreateContext` → the gl0 ordinal carrying that currency ordinal is skipped →
      // permanent NotNext freeze → `globalSyncView` stuck at genesis → allow-spends rejected `TooFarLastValidEpochProgress`).
      //
      // Instead `processCurrencySnapshots` ADOPTS the AUTHORITATIVE currency state from the MPT (PURE MPT-as-primary — NEVER from a
      // materialized `GlobalSnapshotInfo`): it seeds the MPT VERBATIM from gl0's SIGNED MPT BYTES (`pullLatestMptEntries` → `loadBytes`,
      // `seedMptFromSignedBytesForAdopt`) — NO `syncFromGlobalSnapshotInfo` — then reads the metagraph-SIGNED `CurrencySnapshotInfo` +
      // signed incremental from THAT MPT via the finalized `GlobalStateReader` (`getCurrencySnapshotInfo` /
      // `getLastIncrementalCurrencySnapshot`). The adopted state is already
      // (1) finality-gated (#122 — `pullLatestMptEntries` is gl0's depth-k-finalized signed store) and
      // (2) verified-by-construction: `loadBytes` stores the signed bytes verbatim and the seed asserts
      //     `sidecarFreeMptRoot(entries) === signed stateProof.mptRoot` (the corruption backstop). The currency binaries themselves are
      //     metagraph-signature-checked in `fetchCurrencySnapshots` (`toHashedWithSignatureCheck`).
      // So cl1's follower currency state is no longer re-computed but trusted-and-adopted from the signed-bytes MPT, exactly as the
      // global cutover (and symmetric with how ml0/dl1 already loadBytes on the resync path).
      //
      // `applySnapshotFn` (the trait re-execution seam, invoked only by `checkAlignment`'s `NextSubHeight`/`NextHeight` branch)
      // is therefore DEAD on cl1's flow: the rewritten `processCurrencySnapshots` routes every adopt through the
      // download/`setInitial` path (`checkAlignment` with a `Left((snapshot, authoritativeInfo))`), never `checkAlignment(Right)`.
      // It is kept only to satisfy the abstract trait member and raises the same retryable, no-recovery-storm error the global
      // follow uses if it is ever reached (it must not be) — never silently re-executing or fabricating state.
      def applySnapshotFn(
        lastState: CurrencySnapshotInfo,
        lastSnapshot: Signed[CurrencyIncrementalSnapshot],
        snapshot: Signed[CurrencyIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[CurrencySnapshotInfo] =
        FollowSliceVerificationError(
          s"cl1 applySnapshotFn (currency re-execution) is disabled post-cutover but was reached at currency ord=" +
            s"${snapshot.value.ordinal.show}; currency state is adopted authoritatively in processCurrencySnapshots — investigate"
        ).raiseError[F, CurrencySnapshotInfo]

      override def onDownload(snapshot: Hashed[CurrencyIncrementalSnapshot], state: CurrencySnapshotInfo): F[Unit] =
        allowSpendStorage.initByRefs(state.lastAllowSpendRefs.map(_.toMap).getOrElse(Map.empty), snapshot.ordinal) >>
          tokenLockStorage.initByRefs(state.lastTokenLockRefs.map(_.toMap).getOrElse(Map.empty), snapshot.ordinal)

      override def onRedownload(snapshot: Hashed[CurrencyIncrementalSnapshot], state: CurrencySnapshotInfo): F[Unit] =
        // Defense-in-depth (#122): cl1 consumes gl0 snapshots via finality-gating in
        // GlobalSnapshotAlignment, so RedownloadNeeded on a finalized gl0 snapshot should be
        // unreachable on the happy path. If we hit this, either the local cl1 fork-detected
        // against a finalized state (genuine bug) or finality went backwards (also a bug).
        // Destructive replaceByRefs preserved for cluster self-heal; WARN surfaces the anomaly.
        Slf4jLogger
          .getLogger[F]
          .warn(
            s"cl1 onRedownload firing for currency snapshot ord=${snapshot.ordinal.show} — destructive replaceByRefs path " +
              s"should be unreachable under finality-gating (#122); investigate"
          ) >>
          // CUTOVER: a redownload/recovery rebuilds the follower's consumed-field storage, so the diff mirror's held
          // `(tip, state)` is no longer a valid base — RESET it so the next follow tick re-fetches a FULL from-empty slice.
          followMirrorRef.set(none) >>
          allowSpendStorage.replaceByRefs(state.lastAllowSpendRefs.map(_.toMap).getOrElse(Map.empty), snapshot.ordinal) >>
          tokenLockStorage.replaceByRefs(state.lastTokenLockRefs.map(_.toMap).getOrElse(Map.empty), snapshot.ordinal)

      // PURE MPT-AS-PRIMARY seed (3c-A): populate the global MPT for the currency adopt from gl0's SIGNED MPT BYTES — NEVER from a
      // materialized `GlobalSnapshotInfo` (`syncFromGlobalSnapshotInfo`). Pulls gl0's finalized `(snapshot, GSI, signed-byte-map)` via
      // `pullLatestMptEntries` and, when the bytes are served, stores them VERBATIM with `loadBytes` so the recomputed
      // `sidecarFreeMptRoot(entries)` equals the producer's signed `mptRoot` BY CONSTRUCTION (no re-encode drift). The signed byte map is
      // gl0's FULL `mpt_snapshot_info_signed/<ordinal>` store — it includes the per-MG `Mg*` currency partitions + the fieldId-5
      // `LastIncrementalCurrencySnapshots` signed snapshot — so `getCurrencySnapshotInfo` / `getLastIncrementalCurrencySnapshot` read it back
      // after `loadBytes`. Returns the canonical ordinal that was loaded on success; `None` when the MPT was NOT seeded this tick, in which
      // case the caller IDLES the currency adopt (advancing only the global follow) — purity over a one-tick delay; the next non-sparse
      // finalized ordinal carries bytes. The two idle cases:
      //   - byte route 404'd at a sparse combined-checkpoint ordinal ⇒ `pullLatestMptEntries` degraded to `pullLatestSnapshot` and returned
      //     `None` for the bytes. We DO NOT fall back to `syncFromGlobalSnapshotInfo` here (that is the materialized-GSI transit the
      //     MPT-as-primary rule forbids) — we idle.
      //   - bytes were served but the recomputed root ≠ the signed `stateProof.mptRoot` (corrupt/truncated transfer): NEVER adopt
      //     inconsistent state — idle and re-pull next tick (the gate is the corruption backstop, identical to the NotNext resync gate).
      // `globalState` is intentionally UNUSED: the MPT is sourced ONLY from gl0's signed bytes, never the materialized GSI of THIS gl0
      // snapshot. The global-follow sink (`lastGlobalSnapshotStorage`, written via the captured `setGlobalSnapshot`/`setNGlobalSnapshot`
      // thunks) is a SEPARATE store and is left to carry `globalState`; `loadBytes` only writes the MPT.
      private def seedMptFromSignedBytesForAdopt(implicit hasher: Hasher[F]): F[Option[SnapshotOrdinal]] =
        l0Service.pullLatestMptEntries.flatMap {
          case (canonicalSnapshot, _, canonicalEntries) =>
            canonicalEntries match {
              case None =>
                Slf4jLogger
                  .getLogger[F]
                  .info(
                    s"cl1 currency-adopt: gl0 served no signed MPT bytes at canonical ord=${canonicalSnapshot.ordinal.show} " +
                      s"(sparse combined-checkpoint 404) — IDLING the currency adopt this tick (NO syncFromGlobalSnapshotInfo); next non-sparse ordinal seeds the MPT"
                  )
                  .as(none[SnapshotOrdinal])
              case Some(bytes) =>
                for {
                  _ <- mptStore.loadBytes(bytes, canonicalSnapshot.ordinal)
                  afterBytes <- mptStore.underlying.entries
                  recomputedRoot <- GlobalSnapshotInfo.sidecarFreeMptRoot[F](afterBytes).map(_.some)
                  signedRoot = canonicalSnapshot.signed.value.stateProof.mptRoot
                  result <-
                    if (recomputedRoot === signedRoot)
                      canonicalSnapshot.ordinal.some.pure[F]
                    else
                      Slf4jLogger
                        .getLogger[F]
                        .error(
                          s"cl1 currency-adopt: gl0 served signed MPT bytes inconsistent with its OWN signed mptRoot at " +
                            s"ord=${canonicalSnapshot.ordinal.show} (recomputed=${recomputedRoot.map(_.show.take(12)).getOrElse("none")} " +
                            s"≠ signed=${signedRoot.map(_.show.take(12)).getOrElse("none")}); NOT adopting — IDLING, re-pull next tick"
                        )
                        .as(none[SnapshotOrdinal])
                } yield result
            }
        }

      private def processCurrencySnapshots(
        globalSnapshot: Hashed[GlobalIncrementalSnapshot],
        globalState: GlobalSnapshotInfo,
        globalSnapshotReference: SnapshotReference,
        setGlobalSnapshot: F[SnapshotProcessingResult],
        setNGlobalSnapshot: F[SnapshotProcessingResult],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(
        implicit hasher: Hasher[F],
        stateProofSelector: StateProofSelector,
        withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
      ): F[SnapshotProcessingResult] =
        // MPT-AS-PRIMARY (PURE): the MPT is NO LONGER seeded every tick via `syncFromGlobalSnapshotInfo(globalState, …)` — that routed the
        // adopt through a MATERIALIZED `GlobalSnapshotInfo`, the transit the MPT-as-primary rule forbids. Instead we seed the MPT from gl0's
        // SIGNED MPT BYTES (`loadBytes`) ONLY when this gl0 snapshot actually carries currency activity for THIS metagraph — i.e. only inside
        // the `Some(Valid(hashedSnapshots))` branch below (`fetchCurrencySnapshots` is computed independently of the MPT, so the decision is
        // made without reading the stale MPT). `loadBytes` is FULL (no incremental signed-bytes path exists), so this bounds the cost: it
        // fires on a currency-carrying tick, NOT every ~10s global tick. The seed happens BEFORE the `gl0CurrencyView*` MPT reads and BEFORE
        // `processAlignment` reads the MPT during `checkAlignment`/`DownloadNeeded`, so both the steady-state and cold-bootstrap (the `process`
        // Left branch funnels here too) adopts read the just-loaded signed bytes. In steady state nothing else seeds cl1's global MPT (the
        // currency `processAlignment` is a no-op for the global MPT: its `state` is a `CurrencySnapshotInfo`, not a `GlobalSnapshotInfo`, so
        // its `updateMptStorage` type-match hits the `case _ => unit` branch). On a sparse-ordinal 404 (or a corrupt-byte verify mismatch) the
        // seed returns `None` and we IDLE the currency adopt this tick (advancing only the global follow) — see `seedMptFromSignedBytesForAdopt`.
        fetchCurrencySnapshots(globalSnapshot).flatMap {
          case Some(Validated.Valid(hashedSnapshots)) =>
            seedMptFromSignedBytesForAdopt.flatMap {
              case None =>
                // gl0's signed MPT bytes are unavailable (sparse-ordinal 404) or failed the verify gate this tick. Do NOT fall back to
                // `syncFromGlobalSnapshotInfo` (materialized-GSI transit forbidden). IDLE the currency adopt — still ADVANCE the global
                // follow so the follow pointer never stalls; the next non-sparse finalized ordinal seeds the MPT and the forward-only adopt
                // catches up (the authoritative currency tip advances monotonically, so a one-tick delay loses nothing).
                setGlobalSnapshot >> setNGlobalSnapshot
              case Some(_) =>
                prepareIntermediateStorages(
                  addressStorage,
                  blockStorage,
                  lastCurrencySnapshotStorage,
                  transactionStorage,
                  allowSpendStorage,
                  tokenLockStorage
                ).flatMap {
                  // Only the block + last-snapshot intermediate copies (`bs`, `lcss`) are needed to COMPUTE the `DownloadNeeded`
                  // alignment; the adopt is then committed against the REAL storages in the outer `processAlignment` below. The
                  // address / transaction / allow-spend / token-lock copies are unused on this adopt-only path (the prior
                  // re-execution loop applied intermediate alignments to them; the cutover does not).
                  case (_, bs, lcss, _, _, _) =>
                    type Success = NonEmptyList[Alignment]

                    val snapshotToDownload = hashedSnapshots.last

                    // MPT-AS-PRIMARY (PURE): gl0's finalized, field-root-VERIFIED view of THIS metagraph's AUTHORITATIVE latest currency
                    // state, with the authoritative ordinal it sits at (paired so the adopt is strictly forward-only). Sourced from the MPT
                    // just seeded VERBATIM from gl0's SIGNED MPT BYTES (`seedMptFromSignedBytesForAdopt` → `loadBytes`) via the finalized
                    // `GlobalStateReader` — NOT from any materialized `GlobalSnapshotInfo` (the eliminated `globalState.lastCurrencySnapshots`
                    // read). Two cases:
                    //   - non-genesis: `getLastIncrementalCurrencySnapshot` gives the metagraph-signed `Signed[CurrencyIncrementalSnapshot]`
                    //     whose `.value.ordinal` IS the authoritative ordinal, and `getCurrencySnapshotInfo` reconstructs the matching
                    //     `CurrencySnapshotInfo` from the unrolled `Mg*` partitions (SAME ordinal — both read the just-loaded signed-bytes MPT,
                    //     fixing the latent old-code bug of pairing the info with `hashedSnapshots.last`, a possibly-different ordinal).
                    //   - genesis: gl0's signed bytes carry a metagraph at its genesis (`Left`) in the SAME fieldId-5 incremental partition
                    //     (`fromCurrencySnapshot`), so `getLastIncrementalCurrencySnapshot` already returns `Some` at the genesis ordinal and
                    //     `getCurrencySnapshotInfo` returns `Some`; the explicit `getLastCurrencySnapshot.info.toCurrencySnapshotInfo` fallback
                    //     below covers the pre-incremental window where only the `LastCurrencySnapshots` Left key exists.
                    // `None` from every reader ⇒ the metagraph isn't in the MPT at this gl0 ordinal yet — NOT an error (retry next tick).
                    val reader: GlobalStateReader[F] = GlobalStateReader.finalized[F](mptStore)
                    val gl0CurrencyViewF: F[Option[(SnapshotOrdinal, CurrencySnapshotInfo)]] =
                      (reader.getLastIncrementalCurrencySnapshot(identifier), reader.getCurrencySnapshotInfo(identifier)).tupled.flatMap {
                        case (Some(signedIncremental), Some(stateToAdopt)) =>
                          (signedIncremental.value.ordinal, stateToAdopt).some.pure[F]
                        case _ =>
                          reader
                            .getLastCurrencySnapshot(identifier)
                            .map(_.map { genesisFullSnapshot =>
                              (genesisFullSnapshot.value.ordinal, genesisFullSnapshot.value.info.toCurrencySnapshotInfo)
                            })
                      }

                    // Build a download alignment that ADOPTS the authoritative currency state carried in gl0's finalized view onto
                    // `snapshotToDownload` — NO re-execution. `checkAlignment` on an EMPTY `lcss` (the intermediate copy) yields
                    // `DownloadNeeded`, whose `processAlignment` calls `setInitial` + rebuilds balances / lastTxRefs / allow-spend /
                    // token-lock refs from the adopted `CurrencySnapshotInfo`. `lcss.clear` empties ONLY the intermediate copy; the
                    // real `lastCurrencySnapshotStorage` is committed by the outer `processAlignment` (line below) once the
                    // `DownloadNeeded` is produced.
                    //
                    // FIX B (ActiveTipAddingError wedge): the produced `DownloadNeeded`'s `processAlignment` calls
                    // `blockStorage.adjustToMajority(activeTipsToAdd/deprecatedTipsToAdd = snapshot.tips, …)` on the REAL `blockStorage`.
                    // `addActiveTips`/`addDeprecatedTips` accept a tip only when its hash is `WaitingBlock`/`PostponedBlock`/`None` —
                    // but on a running L1 the genesis block (and any cl1-produced blocks) are already `MajorityBlock`, so re-adding the
                    // genesis tip throws `BlockStorage$ActiveTipAddingError` (observed 31× on cl1-m0 + dl1-m0) → the alignment is
                    // skipped → the follow wedges → L0-token send fails. The cutover already clears `lastCurrencySnapshotStorage` so its
                    // `setInitial` lands; the block storage was the missing reset. Clearing it here restores the COLD-BOOTSTRAP
                    // precondition (empty block storage) the `DownloadNeeded` path assumes. In-flight self-produced blocks are dropped —
                    // acceptable: they live on ml0 and the follower re-syncs to the authoritative tip carried by `snapshot.tips` (which
                    // `adjustToMajority` re-adds as the fresh `MajorityBlock` set). The intermediate `bs` copy is untouched (a non-empty
                    // `bs` only contributes to `obsoleteToRemove`/`postponedToWaiting`, both empty for a genesis-tip-only reconciliation).
                    def adoptForwardTo(stateToAdopt: CurrencySnapshotInfo): F[Option[Success]] = {
                      val toPass = (snapshotToDownload, stateToAdopt).asLeft[Hashed[CurrencyIncrementalSnapshot]]

                      blockStorage.clear >>
                        lcss.clear >>
                        checkAlignment(
                          toPass,
                          bs,
                          lcss,
                          txHasher,
                          getGlobalSnapshotByOrdinal,
                          globalL0AlignmentStorage
                        ).map(alignment => NonEmptyList.one(alignment).some)
                    }

                    gl0CurrencyViewF.flatMap { gl0CurrencyView =>
                      lastCurrencySnapshotStorage.getCombined.flatMap {
                        case None =>
                          // Bootstrap (cl1 has no local currency snapshot yet): adopt the metagraph's authoritative state from gl0's
                          // (MPT-sourced) view. `None` (metagraph not yet in the MPT at this gl0 ordinal) is NOT an error — return `none`
                          // so the stream retries next gl0 snapshot (a `raiseError` here would restart `globalSnapshotProcessing` and the
                          // first metagraph tx send would die before cl1 ever establishes a currency snapshot).
                          gl0CurrencyView match {
                            case Some((_, stateToAdopt)) => adoptForwardTo(stateToAdopt)
                            case None                    => none[Success].pure[F]
                          }

                        case Some((lastCurrencySnapshot, _)) =>
                          // CUTOVER normal path: ADOPT gl0's authoritative currency view forward, monotonically — NO re-execution, NO
                          // chain-link `Validator.compare` (the brittle check that, with the old re-execution, produced
                          // `CannotCreateContext` → skip → permanent NotNext freeze). We adopt ONLY when gl0's authoritative ordinal is
                          // strictly AHEAD of our local tip (forward-only / never regress). Because gl0 advances the currency tip
                          // monotonically per metagraph (chain-linked at acceptance) and most ~10s gl0 ticks carry NO new currency
                          // ordinal, the common case is "no new ordinal ⇒ ignore" — no clear/re-download churn — and a fresh authoritative
                          // ordinal triggers exactly one forward adopt (full `setInitial` rebuild; acceptable at the slow finalized cadence).
                          gl0CurrencyView match {
                            case Some((authoritativeOrdinal, stateToAdopt)) if authoritativeOrdinal > lastCurrencySnapshot.ordinal =>
                              Slf4jLogger
                                .getLogger[F]
                                .info(
                                  s"cl1 adopting authoritative currency state ord=${authoritativeOrdinal.show} " +
                                    s"(local tip ord=${lastCurrencySnapshot.ordinal.show}, gl0 ord=${globalSnapshot.ordinal.show}) " +
                                    s"— MPT-sourced, field-root-verified, no re-execution"
                                ) >>
                                // Clear the REAL tip so the produced `DownloadNeeded` lands via `setInitial` (requires empty storage)
                                // when the outer `processAlignment` commits it (block storage is reset inside `adoptForwardTo`).
                                lastCurrencySnapshotStorage.clear >>
                                adoptForwardTo(stateToAdopt)

                            // gl0 carries no NEW currency ordinal (at/behind our tip) — nothing to adopt this tick. Leave the tip
                            // untouched and idle (no churn); a later gl0 snapshot with a fresh currency ordinal will advance us.
                            case _ => none[Success].pure[F]
                          }
                      }
                    }
                }.flatMap {
                  case Some(alignments) =>
                    alignments.traverse { alignment =>
                      processAlignment(
                        alignment,
                        blockStorage,
                        transactionStorage,
                        allowSpendStorage,
                        tokenLockStorage,
                        lastCurrencySnapshotStorage,
                        addressStorage,
                        mptStore
                      )
                    }.flatMap { results =>
                      setGlobalSnapshot >>
                        setNGlobalSnapshot
                          .map(BatchResult(_, results))
                    }
                  // No currency alignment this tick (nothing new to adopt for this metagraph — gl0's authoritative currency view is
                  // at/behind cl1's local tip). This is NOT a stall: still ADVANCE the global follow pointer (`setGlobalSnapshot` /
                  // `setNGlobalSnapshot`), exactly as the "gl0 snapshot carried no currency binaries" branch below. Decoupling global
                  // advancement from currency work is REQUIRED post-cutover — otherwise a gl0 snapshot that re-serves an already-adopted
                  // currency ordinal would freeze the global follow (the re-execution path masked this by always producing an alignment
                  // on a clean forward chain).
                  case None =>
                    setGlobalSnapshot >>
                      setNGlobalSnapshot
                }
            } // end seedMptFromSignedBytesForAdopt.flatMap (Some(_) currency-activity branch)

          case Some(Validated.Invalid(_)) =>
            Slf4jLogger
              .getLogger[F]
              .warn(s"Not all currency snapshots are signed correctly! Ignoring global snapshot: $globalSnapshotReference")
              .as(SnapshotIgnored(globalSnapshotReference))

          case None =>
            setGlobalSnapshot >>
              setNGlobalSnapshot
        }

      private def prepareIntermediateStorages(
        addressStorage: AddressStorage[F],
        blockStorage: BlockStorage[F],
        lastCurrencySnapshotStorage: LastSnapshotStorage[F, CurrencyIncrementalSnapshot, CurrencySnapshotInfo],
        transactionStorage: TransactionStorage[F],
        allowSpendStorage: AllowSpendStorage[F],
        tokenLockStorage: TokenLockStorage[F]
      )(implicit hasher: Hasher[F]): F[
        (
          AddressStorage[F],
          BlockStorage[F],
          LastSnapshotStorage[F, CurrencyIncrementalSnapshot, CurrencySnapshotInfo],
          TransactionStorage[F],
          AllowSpendStorage[F],
          TokenLockStorage[F]
        )
      ] = {
        val bs = blockStorage.getState().flatMap(BlockStorage.make[F](_))
        val lcss =
          lastCurrencySnapshotStorage.getCombined.flatMap(LastSnapshotStorage.make[F, CurrencyIncrementalSnapshot, CurrencySnapshotInfo](_))
        val as = addressStorage.getState.flatMap { state =>
          val (balances) = state
          AddressStorage.make(balances)
        }
        val cv = ContextualTransactionValidator.make(transactionLimitConfig, None)
        val capv = ContextualAllowSpendValidator.make(CurrencyId(identifier).some, None, allowSpendsConfig)
        val ctlv = ContextualTokenLockValidator.make(None, tokenLocksConfig, CurrencyId(identifier).some)
        val ts =
          transactionStorage.getState.flatMap {
            case (lastTxs) =>
              TransactionReference.emptyCurrency(identifier).flatMap {
                TransactionStorage.make(lastTxs, _, cv)
              }
          }

        val als =
          allowSpendStorage.getState.flatMap {
            case (lastTxs) =>
              AllowSpendReference.emptyCurrency(identifier).flatMap {
                AllowSpendStorage.make(lastTxs, _, capv)
              }
          }

        val tls =
          tokenLockStorage.getState.flatMap {
            case (lastTxs) =>
              TokenLockReference.emptyCurrency(identifier).flatMap {
                TokenLockStorage.make(lastTxs, _, ctlv)
              }
          }
        (as, bs, lcss, ts, als, tls).mapN((_, _, _, _, _, _))
      }

      // We are extracting all currency snapshots, but we don't assume that all the state channel binaries need to be
      // currency snapshots. Binary that fails to deserialize as currency snapshot are ignored here.
      private def fetchCurrencySnapshots(
        globalSnapshot: GlobalIncrementalSnapshot
      )(
        implicit hasher: Hasher[F]
      ): F[Option[ValidatedNel[InvalidSignatureForHash[CurrencyIncrementalSnapshot], NonEmptyList[Hashed[CurrencyIncrementalSnapshot]]]]] =
        globalSnapshot.stateChannelSnapshots
          .get(identifier) match {
          case Some(snapshots) =>
            snapshots.toList.traverse { binary =>
              JsonSerializer[F].deserialize[Signed[CurrencyIncrementalSnapshot]](binary.content)
            }
              .map(_.flatMap(_.toOption))
              .map(NonEmptyList.fromList)
              .map(_.map(_.sortBy(_.value.ordinal)))
              .flatMap(_.map(_.traverse(_.toHashedWithSignatureCheck)).sequence)
              .map(_.map(_.traverse(_.toValidatedNel)))
          case None => Async[F].pure(none)
        }
    }

  /** Map the snapshot's `stateProof` per-field roots onto the five uniform-`Hash` [[GlobalStateFieldId]]s, in the shape
    * [[GlobalFollowMirrorVerifier.verifyByFieldRoot]] expects. Identical to gl1's `DAGSnapshotProcessor.signedFieldRoots`: `Balances` /
    * `LastTxRefs` are always-present; the three `Option[Hash]` fields omit `None` (the verifier treats an absent field as [[Hash.empty]],
    * matching gl0's `getOrElse(_, Hash.empty)`). The 6th field (`lastCurrencySnapshots`) is NOT here — it has no per-field root slot and is
    * verified by the injected `currencySnapshotsCheck` recompute instead.
    */
  private def signedFieldRoots(stateProof: GlobalSnapshotStateProof): SortedMap[GlobalStateFieldId, Hash] =
    FollowVerifyCore.signedFieldRoots(stateProof)

  /** Build a `GlobalSnapshotInfo` populated with the SIX fields cl1/dl1 consume from a verified [[ConsumedFieldState]]; every other field
    * stays at `GlobalSnapshotInfo.empty`'s value. The five Address-keyed consumed fields match gl1's
    * `DAGSnapshotProcessor.consumedFieldsToGlobalSnapshotInfo`; the cl1/dl1 ADDITION is `lastCurrencySnapshots`. This partial GSI is the
    * GLOBAL-FOLLOW `state`: it feeds the global storage sink (`lastGlobalSnapshotStorage`), NOT the MPT. Post-cutover the currency adopt's
    * MPT is seeded PURELY from gl0's SIGNED MPT BYTES (`processCurrencySnapshots` → `loadBytes`), so the per-MG `Mg*` + fieldId-5 currency
    * partitions come from gl0's signed bytes — there is no `syncFromGlobalSnapshotInfo` transit of this partial GSI. The 6th field stays
    * populated here only so the global storage carries the field-root-verified currency map; the currency adopt never reads it.
    * `lastCurrencySnapshotsProofs` is left empty — the MPT-format proof carries no such field root and cl1/dl1 never read the proofs map.
    */
  private def consumedFieldsToGlobalSnapshotInfo(state: ConsumedFieldState): GlobalSnapshotInfo =
    // Five UNIFORM consumed fields single-sourced via the registry; the 6th (`lastCurrencySnapshots`, the bespoke
    // outlier deliberately NOT in the registry) is layered on top — byte-identical to the prior 6-field literal.
    FollowVerifyCore.toGlobalSnapshotInfo(state).copy(lastCurrencySnapshots = state.lastCurrencySnapshots)

  private def describe(err: FollowVerificationError): String = err match {
    case FollowVerificationError.CommittedRootMismatch(expected, got) =>
      s"CommittedRootMismatch(expected=${expected.show.take(12)}, got=${got.show.take(12)})"
    case FollowVerificationError.RangeProofInvalid(field, _)  => s"RangeProofInvalid($field)"
    case FollowVerificationError.ValueBindingFailed(field, _) => s"ValueBindingFailed($field)"
    case FollowVerificationError.FieldRootMismatch(field, expected, got) =>
      s"FieldRootMismatch($field, expected=${expected.show.take(12)}, got=${got.show.take(12)})"
  }
}
