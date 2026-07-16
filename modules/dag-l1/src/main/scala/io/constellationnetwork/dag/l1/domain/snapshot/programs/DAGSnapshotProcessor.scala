package io.constellationnetwork.dag.l1.domain.snapshot.programs

import cats.Parallel
import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.dag.l1.domain.address.storage.AddressStorage
import io.constellationnetwork.dag.l1.domain.block.BlockStorage
import io.constellationnetwork.dag.l1.domain.transaction.TransactionStorage
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.globalAlignment.GlobalL0AlignmentStorage
import io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowMirrorVerifier
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.domain.swap.AllowSpendStorage
import io.constellationnetwork.node.shared.domain.tokenlock.TokenLockStorage
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.follow._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}

import org.typelevel.log4cats.slf4j.Slf4jLogger

object DAGSnapshotProcessor {

  def make[F[_]: Async: Parallel: SecurityProvider: JsonSerializer](
    addressStorage: AddressStorage[F],
    blockStorage: BlockStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    transactionStorage: TransactionStorage[F],
    allowSpendStorage: AllowSpendStorage[F],
    tokenLockStorage: TokenLockStorage[F],
    txHasher: Hasher[F],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    l0Service: GlobalL0Service[F],
    globalL0AlignmentStorage: GlobalL0AlignmentStorage[F],
    mptStore: MptStore[F, GlobalStateKey],
    // #287 "send diffs": the follower's verified-mirror state across ticks — `Some((lastVerifiedTip, state))` or
    // `None` (bootstrap / post-reset). Created ONCE at the dag-l1 construction site (`Main`) and threaded here.
    // When `Some`, this tick requests the INCREMENTAL slice since `lastVerifiedTip` and applies the returned delta
    // on `state` (only if the response's `baseOrdinal` matches `lastVerifiedTip`); otherwise it requests the FULL
    // latest slice and applies from empty. ANY verify failure RESETS the mirror to `None`, so the next tick
    // re-fetches a full from-empty slice — the optimization can never regress the safety invariant (field-root
    // equality already rejects a wrong/stale diff with `FieldRootMismatch`). `getByOrdinal`-defer (M not yet
    // followed) leaves the mirror untouched so it resolves once the follow reaches M.
    followMirrorRef: Ref[F, Option[(SnapshotOrdinal, ConsumedFieldState)]]
  ): SnapshotProcessor[F, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new SnapshotProcessor[F, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {

      import SnapshotProcessor._

      private val followLogger = Slf4jLogger.getLoggerFromName[F]("io.constellationnetwork.dag.l1.GlobalFollow")

      override def onDownload(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        allowSpendStorage.initByRefs(state.lastAllowSpendRefs.map(_.toMap).getOrElse(Map.empty), snapshot.ordinal) >>
          tokenLockStorage.initByRefs(state.lastTokenLockRefs.map(_.toMap).getOrElse(Map.empty), snapshot.ordinal)

      override def onRedownload(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        // Defense-in-depth (#122): with finality-gating, followers consume only depth-k-finalized
        // gl0 snapshots, so RedownloadNeeded should never fire on the happy path. If we hit this,
        // either gl0 finality went backwards (genuine bug) or the local node's chain mismatch
        // detection is firing on a finalized snapshot (also a bug). The destructive replaceByRefs
        // path is preserved so the cluster can self-heal, but the WARN log surfaces the anomaly.
        org.typelevel.log4cats.slf4j.Slf4jLogger
          .getLogger[F]
          .warn(
            s"dl1 onRedownload firing for finalized snapshot ord=${snapshot.ordinal.show} — destructive replaceByRefs path " +
              s"should be unreachable under finality-gating (#122); investigate"
          ) >>
          // #287: a redownload/recovery rebuilds the follower's consumed-field storage from scratch, so the diff
          // mirror's held `(tip, state)` is no longer a valid base — RESET it so the next follow tick re-fetches a
          // FULL from-empty slice instead of diffing against a stale tip.
          followMirrorRef.set(none) >>
          allowSpendStorage.replaceByRefs(state.lastAllowSpendRefs.map(_.toMap).getOrElse(Map.empty), snapshot.ordinal) >>
          tokenLockStorage.replaceByRefs(state.lastTokenLockRefs.map(_.toMap).getOrElse(Map.empty), snapshot.ordinal)

      override def setInitialLastNSnapshots(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        lastNGlobalSnapshotStorage.setInitialFetchingGL0(
          snapshot,
          state,
          l0Service.asLeft.some,
          none
        )

      override def setLastNSnapshots(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        lastNGlobalSnapshotStorage.set(snapshot, state)

      def process(
        snapshot: Either[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo), Hashed[GlobalIncrementalSnapshot]]
      )(
        implicit hasher: Hasher[F],
        stateProofSelector: StateProofSelector,
        withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
      ): F[SnapshotProcessingResult] =
        checkAlignment(
          snapshot,
          blockStorage,
          lastGlobalSnapshotStorage,
          txHasher,
          getGlobalSnapshotByOrdinal,
          globalL0AlignmentStorage
        )
          .flatMap(
            processAlignment(
              _,
              blockStorage,
              transactionStorage,
              allowSpendStorage,
              tokenLockStorage,
              lastGlobalSnapshotStorage,
              addressStorage,
              mptStore
            )
          )

      def applySnapshotFn(
        lastState: GlobalSnapshotInfo,
        lastSnapshot: Signed[GlobalIncrementalSnapshot],
        snapshot: Signed[GlobalIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[GlobalSnapshotInfo] =
        applyGlobalSnapshotFn(lastState, lastSnapshot, snapshot, getGlobalSnapshotByOrdinal)

      // CUTOVER (S3b′ — `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`): gl1 no longer re-executes the
      // finalized gl0 snapshot (`GlobalSnapshotContextFunctions.createContext → GSAM.accept`), which re-derived the
      // FULL global MPT and stormed on `StateProofMismatch` over fields gl1 never reads (the `historicalStakeSnapshots`
      // carry-forward divergence at the ordinal after an eta boundary — the 2026-05-28 gl1-formation blocker).
      //
      // Instead it FETCHES gl0's latest-finalized consumed-field SLICE (`l0Service.getLatestFollowSlice`), VERIFIES it
      // by field-root equality against the signed snapshot AT THE SLICE'S OWN ordinal (recompute-and-match — completeness
      // correct-by-design for a full-content holder; see `GlobalFollowMirrorVerifier`), and BUILDS a `GlobalSnapshotInfo`
      // populated with ONLY the five Address-keyed consumed fields gl1 actually reads
      // (`balances`, `lastTxRefs`, `lastAllowSpendRefs`, `lastTokenLockRefs`, `activeTokenLocks`); all ~12 other GSI fields are left at
      // their empty/default values. The 5-field GSI flows downstream UNCHANGED — `lastSnapshotStorage.set`, `extractMajority*`,
      // `mptStore.syncFromGlobalSnapshotInfo` (which tolerates a partial GSI: `store.clear` + per-field inserts where empty
      // maps are no-op), `Collateral`/`CollateralDaemon`, `TransactionService` tx-validation (reads `si.balances`), and
      // `TokenLockService` token-lock-replacement validation (reads `si.getActiveTokenLocks` + `si.balances`).
      //
      // ALIGNMENT (slice ordinal M vs processed ordinal N — see the file-level note): the slice producer serves only the
      // LATEST-finalized GSI (the `MptOverlay` cannot read value-bytes at a historical ordinal — S2a finding — and gl1 only
      // needs the latest finalized state for tx validation, not per-ordinal replay). M is therefore a moving target, always
      // >= N: during catch-up (and even in steady-state lag) gl1's processing ordinal N trails the latest-finalized slice
      // ordinal M. So the verify is ALWAYS anchored to the snapshot at the slice's OWN ordinal M (`signedFieldRoots` taken
      // from THAT snapshot's `stateProof`, never the ordinal-N snapshot being walked), which makes the field-root match
      // cryptographically self-consistent and never a cross-ordinal mismatch.
      //
      // CATCH-UP FIX (gl1-formation, 2026-05-29): the snapshot at M is resolved from gl1's OWN LOCAL store — the lastN
      // finalized signed snapshots `lastNGlobalSnapshotStorage` retains by ordinal — NOT re-pulled from a peer. Because gl1
      // follows the FINALIZED chain and the slice IS the latest finalized, gl1 already holds M's signed snapshot locally once
      // its follow reaches M (it stored it via `setLastNSnapshots` when it finalized M); that is the SAME signed snapshot gl0
      // projected the slice from, so its `stateProof` per-field roots are byte-identical to what the slice recompute-matches.
      // This decouples the verify from the per-ordinal "processing N == M" constraint: the old code only avoided the re-pull
      // when N == M (which rarely/never holds, since the slice is always the latest finalized), so the M-re-pull fell through
      // to a peer fetch that returned None during catch-up -> "could not resolve" -> defer forever (0 Verified). If gl1 has
      // NOT yet followed to M (M not in its lastN window) the verify simply defers THIS tick and resolves as soon as gl1's
      // follow reaches M. Net effect: gl1's consumed-field state = the latest verified finalized slice, verified against
      // gl1's local copy of that finalized snapshot; as M advances gl1 follows to M and re-verifies, so steady-state lag no
      // longer blocks verification. On the happy-path tail N == M and state+chain-pointer coincide exactly.
      //
      // No `StateProofMismatch` raise/recovery on this path: a verify failure / unavailable slice raises the retryable
      // `FollowSliceVerificationError`, which the batch loop logs + skips WITHOUT `shouldRedownload` (no recovery storm) —
      // gl1 simply retries next 10s tick.
      def applyGlobalSnapshotFn(
        lastGlobalState: GlobalSnapshotInfo,
        lastGlobalSnapshot: Signed[GlobalIncrementalSnapshot],
        globalSnapshot: Signed[GlobalIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[GlobalSnapshotInfo] = {
        val verifier = GlobalFollowMirrorVerifier.make[F]

        // The signed per-field roots for the snapshot at the slice's own ordinal `sliceOrdinal`, resolved from gl1's OWN
        // LOCAL state (no peer re-pull — see the CATCH-UP FIX note above):
        //   - if the slice is at the ordinal currently being processed (N == M), use the in-hand snapshot's `stateProof`
        //     directly (it is not yet in the lastN window — `setLastNSnapshots` runs only after this apply);
        //   - otherwise look up gl1's local finalized snapshot at `sliceOrdinal` in `lastNGlobalSnapshotStorage` and take
        //     THAT snapshot's `stateProof`. This is the same signed snapshot gl0 projected the slice from, so the field-root
        //     match is byte-exact. `None` (M not yet followed) defers this tick and resolves once the follow reaches M.
        def signedRootsAt(sliceOrdinal: SnapshotOrdinal): F[Option[GlobalSnapshotStateProof]] =
          if (sliceOrdinal === globalSnapshot.value.ordinal)
            globalSnapshot.value.stateProof.some.pure[F]
          else
            lastNGlobalSnapshotStorage.getByOrdinal(sliceOrdinal).map(_.map(_.signed.value.stateProof))

        // #287 "send diffs": fetch the INCREMENTAL slice since the held tip when the mirror is populated, else the FULL
        // latest slice. The mirror holds `(lastVerifiedTip, state)`; on a populated mirror we ask gl0 for the change-set
        // since `lastVerifiedTip` and apply it on `state` — but ONLY if the response's `baseOrdinal` matches that tip
        // (otherwise gl0 fell back to a full from-empty slice, or the bases diverged, and we apply from empty). This is
        // a pure optimization: field-root equality below rejects any wrong/stale base as `FieldRootMismatch`, which
        // resets the mirror and forces a full re-fetch next tick — so the diff path can never advance bad state.
        followMirrorRef.get.flatMap { mirror =>
          val fetch: F[Option[GlobalFollowSliceResponse]] = mirror match {
            case Some((heldTip, _)) => l0Service.getFollowSliceSince(heldTip)
            case None               => l0Service.getLatestFollowSlice
          }
          fetch.flatMap {
            case None =>
              // No slice yet (no GlobalFollowClient wired, peer down, or gl0 has no finalized ordinal). Don't advance.
              FollowSliceVerificationError(
                s"no follow slice available while processing ordinal=${globalSnapshot.value.ordinal.show}"
              ).raiseError[F, GlobalSnapshotInfo]
            case Some(GlobalFollowSliceResponse(sliceOrdinal, slice, baseOrdinal)) =>
              // Apply the delta on the held state iff the producer diffed against EXACTLY the tip we hold; any other case
              // (full slice `baseOrdinal=None`, a base we don't hold, or an empty mirror) applies from empty.
              val prior = mirror match {
                case Some((heldTip, st)) if baseOrdinal.contains(heldTip) => st
                case _                                                    => ConsumedFieldState.empty
              }
              signedRootsAt(sliceOrdinal).flatMap {
                case None =>
                  // gl1 has not yet followed (and locally retained) the finalized snapshot at the slice's ordinal M. Defer
                  // this tick WITHOUT a peer re-pull AND WITHOUT clearing the mirror; it resolves as soon as gl1's
                  // finalized-follow reaches M.
                  FollowSliceVerificationError(
                    s"local finalized snapshot at slice ordinal=${sliceOrdinal.show} not yet followed " +
                      s"(processing ordinal=${globalSnapshot.value.ordinal.show}) — deferring slice verify"
                  ).raiseError[F, GlobalSnapshotInfo]
                case Some(stateProof) =>
                  verifier
                    .verifyByFieldRoot(prior, sliceOrdinal, slice, signedFieldRoots(stateProof))
                    .flatMap {
                      case Right(verified) =>
                        // Advance the mirror to the verified tip + post-state so the NEXT tick can request a diff since M.
                        followMirrorRef.set((sliceOrdinal, verified.value).some) >>
                          consumedFieldsToGlobalSnapshotInfo(verified.value).pure[F]
                      case Left(err) =>
                        // Verify failed against the trusted signed roots — RESET the mirror so the next tick re-fetches a
                        // FULL from-empty slice (the guaranteed fallback), log + don't advance. NO recovery storm.
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
    }

  /** Map the snapshot's `stateProof` per-field roots onto the five [[GlobalStateFieldId]]s gl1 consumes, in the shape
    * [[GlobalFollowMirrorVerifier.verifyByFieldRoot]] expects. `Balances` / `LastTxRefs` are always-present `Hash`es; `LastAllowSpendRefs`
    * / `LastTokenLockRefs` / `ActiveTokenLocks` are `Option[Hash]` on the proof (the GSI fields are `Option`-typed) — `None` is OMITTED
    * from the map, and the verifier treats an absent field as [[Hash.empty]] (matching gl0's `getOrElse(_, Hash.empty)`), so an empty
    * consumed field recompute-matches.
    */
  private def signedFieldRoots(stateProof: GlobalSnapshotStateProof): SortedMap[GlobalStateFieldId, Hash] =
    FollowVerifyCore.signedFieldRoots(stateProof)

  /** Build a `GlobalSnapshotInfo` populated with ONLY the five Address-keyed consumed fields from a verified [[ConsumedFieldState]]; every
    * other field is left at `GlobalSnapshotInfo.empty`'s value. The three `Option`-typed consumed fields are lifted into `Some(...)`
    * (matching gl0's GSI, where these are always `Some` post-V2). This partial GSI is what gl1 stores + tx-validates against;
    * `mptStore.syncFromGlobalSnapshotInfo` tolerates the empty fields (no-op inserts). `activeTokenLocks` is THE bug-fix: it is read by
    * gl1's token-lock-replacement validator (via `TokenLockService.getActiveTokenLocks`); without it the mirror stays empty and every
    * replacement fails `NothingToReplace`.
    */
  private def consumedFieldsToGlobalSnapshotInfo(state: ConsumedFieldState): GlobalSnapshotInfo =
    FollowVerifyCore.toGlobalSnapshotInfo(state)

  private def describe(err: FollowVerificationError): String = err match {
    case FollowVerificationError.CommittedRootMismatch(expected, got) =>
      s"CommittedRootMismatch(expected=${expected.show.take(12)}, got=${got.show.take(12)})"
    case FollowVerificationError.MissingFieldProof(field)    => s"MissingFieldProof($field)"
    case FollowVerificationError.MissingFieldValues(field)   => s"MissingFieldValues($field)"
    case FollowVerificationError.MissingFieldValue(field, _) => s"MissingFieldValue($field)"
    case FollowVerificationError.RangeBoundsMismatch(field, _, _, _, _) =>
      s"RangeBoundsMismatch($field)"
    case FollowVerificationError.UnprovenEmptyField(field)    => s"UnprovenEmptyField($field)"
    case FollowVerificationError.RangeProofInvalid(field, _)  => s"RangeProofInvalid($field)"
    case FollowVerificationError.ValueBindingFailed(field, _) => s"ValueBindingFailed($field)"
    case FollowVerificationError.FieldRootMismatch(field, expected, got) =>
      s"FieldRootMismatch($field, expected=${expected.show.take(12)}, got=${got.show.take(12)})"
  }

  /** Raised by [[applyGlobalSnapshotFn]] when the follow slice is unavailable or fails to verify against the trusted signed roots. Distinct
    * from `GlobalSnapshotContextFunctions.StateProofMismatch` — it carries NO recovery semantics: the batch loop logs it and idles (no
    * `shouldRedownload`, no `replaceByRefs`), so gl1 retries cleanly on the next tick.
    */
  final case class FollowSliceVerificationError(message: String) extends RuntimeException(message) with scala.util.control.NoStackTrace
}
