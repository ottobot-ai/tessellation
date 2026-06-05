package io.constellationnetwork.currency.l1.domain.snapshot.programs

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.Async
import cats.effect.kernel.Ref
import cats.effect.std.Random
import cats.syntax.all._
import cats.{Applicative, Parallel}

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
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage._
import io.constellationnetwork.node.shared.domain.snapshot.{SnapshotContextFunctions, Validator}
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
    currencySnapshotContextFns: SnapshotContextFunctions[F, CurrencyIncrementalSnapshot, CurrencySnapshotContext],
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
                // Populate MPT with the bootstrap GSI BEFORE processCurrencySnapshots runs
                // — processAlignment reads the MPT during checkAlignment, so the seed must
                // happen first. Without it, accept() on the next incremental snapshot reads
                // from an empty MPT and produces a state proof that diverges from the
                // leader's claimed root (StateProofMismatch at ordinal 2). Uses scodec-typed
                // syncFromGlobalSnapshotInfo to match the bytes syncFromStateChanges writes
                // during accept(). Same pattern as dag-l0/Main.scala bootstrap paths.
                val setGlobalSnapshot = lastGlobalSnapshotStorage
                  .setInitial(globalSnapshot, globalState)
                  .as[SnapshotProcessingResult](DownloadPerformed(globalSnapshotReference, Set.empty, Set.empty))
                val setNGlobalSnapshots = lastNGlobalSnapshotStorage
                  .setInitialFetchingGL0(globalSnapshot, globalState, l0Service.asLeft.some, none)
                  .as[SnapshotProcessingResult](DownloadPerformed(globalSnapshotReference, Set.empty, Set.empty))

                mptStore.syncFromGlobalSnapshotInfo(globalState, globalSnapshot.ordinal) >>
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
                    // Parent-hash mismatch between our stored last snapshot and the incoming one.
                    // Under finality-gating (#122) followers consume only depth-k-finalized gl0
                    // snapshots, so this branch should be unreachable on the happy path — a
                    // mismatch at the finalized horizon implies either a genuine chain-fork bug
                    // or local state divergence (e.g. stale storage). Defensive recovery kept:
                    // clear our last snapshot state so the next pull falls into the bootstrap
                    // (Left) branch, rebootstrapping from the canonical head. Without this, cl1
                    // would SnapshotIgnored every subsequent snapshot forever (observed: cl1
                    // stranded at ord 56 while gl0 reached ord 161+). Also sets the redownload
                    // flag for observability / TooFarEpochProgress path consistency.
                    val reason =
                      s"Parent-hash mismatch at incoming ord=${globalSnapshotReference.ordinal.show}: stored last ord=${lastGlobalSnapshot.ordinal.show} hash=${lastGlobalSnapshot.hash.value
                          .take(12)} but incoming.lastSnapshotHash=${globalSnapshot.signed.value.lastSnapshotHash.value.take(12)}. Clearing state + forcing redownload."
                    Slf4jLogger
                      .getLogger[F]
                      .warn(
                        s"cl1 NotNext on finalized snapshot (#122 anomaly) at ord=${globalSnapshotReference.ordinal.show} — destructive clear+redownload preserved as defense; investigate"
                      ) >>
                      globalL0AlignmentStorage.updateShouldRedownload(value = true, reasons = List(reason)) >>
                      lastGlobalSnapshotStorage.clear >>
                      lastNGlobalSnapshotStorage.clear
                        .as[SnapshotProcessingResult](SnapshotIgnored(globalSnapshotReference))
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
      // activeTokenLocks) PLUS `lastCurrencySnapshots` (the metagraph's own currency-genesis bootstrap reads
      // `globalState.lastCurrencySnapshots.get(identifier)` in `processCurrencySnapshots`). All other GSI fields stay empty.
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

      def applySnapshotFn(
        lastState: CurrencySnapshotInfo,
        lastSnapshot: Signed[CurrencyIncrementalSnapshot],
        snapshot: Signed[CurrencyIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[CurrencySnapshotInfo] =
        currencySnapshotContextFns
          .createContext(
            CurrencySnapshotContext(identifier, lastState),
            lastSnapshot,
            snapshot,
            getGlobalSnapshotByOrdinal
          )
          .map(_.snapshotInfo)

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
        fetchCurrencySnapshots(globalSnapshot).flatMap {
          case Some(Validated.Valid(hashedSnapshots)) =>
            prepareIntermediateStorages(
              addressStorage,
              blockStorage,
              lastCurrencySnapshotStorage,
              transactionStorage,
              allowSpendStorage,
              tokenLockStorage
            ).flatMap {
              case (as, bs, lcss, ts, als, tls) =>
                type Success = NonEmptyList[Alignment]
                type Agg = (NonEmptyList[Hashed[CurrencyIncrementalSnapshot]], List[Alignment])
                type Result = Option[Success]

                lastCurrencySnapshotStorage.getCombined.flatMap {
                  case None =>
                    val snapshotToDownload = hashedSnapshots.last

                    // Bootstrap (cl1 has no local currency snapshot yet): download the metagraph's state from gl0's view of
                    // this metagraph. gl0's view can legitimately be at one of three states, all of which must be handled
                    // WITHOUT crashing the alignment stream (a `raiseError` here propagates up to `globalSnapshotProcessing`'s
                    // `handleErrorWith` → "Global snapshot processing stream failed, restarting" → restart-loop; cl1 then never
                    // establishes a currency snapshot and the first metagraph tx send dies).
                    def bootstrapFrom(stateToDownload: CurrencySnapshotInfo): F[Option[Success]] = {
                      val toPass = (snapshotToDownload, stateToDownload).asLeft[Hashed[CurrencyIncrementalSnapshot]]

                      checkAlignment(
                        toPass,
                        bs,
                        lcss,
                        txHasher,
                        getGlobalSnapshotByOrdinal,
                        globalL0AlignmentStorage
                      ).map { alignment =>
                        NonEmptyList.one(alignment).some
                      }
                    }

                    globalState.lastCurrencySnapshots.get(identifier) match {
                      // gl0 has adopted a non-genesis incremental for this metagraph — bootstrap from the carried info.
                      case Some(Right((_, stateToDownload))) => bootstrapFrom(stateToDownload)

                      // gl0's view is still the metagraph's genesis FULL snapshot (gl0 lagging the metagraph). The full
                      // snapshot carries the complete `CurrencySnapshotInfo` (as the V1 `info`), so cl1 CAN bootstrap from it
                      // and then catches up by following ml0's incrementals forward.
                      case Some(Left(genesisFullSnapshot)) => bootstrapFrom(genesisFullSnapshot.value.info.toCurrencySnapshotInfo)

                      // The metagraph isn't in gl0's `lastCurrencySnapshots` at this gl0 ordinal yet — NOT an error. Return
                      // `none` (no alignment this round) so the stream retries on the next gl0 snapshot, rather than crashing.
                      case None => none[Success].pure[F]
                    }

                  case Some((_, _)) =>
                    (hashedSnapshots, List.empty[Alignment]).tailRecM {
                      case (NonEmptyList(snapshot, nextSnapshots), agg) =>
                        val toPass = snapshot.asRight[(Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]

                        checkAlignment(
                          toPass,
                          bs,
                          lcss,
                          txHasher,
                          getGlobalSnapshotByOrdinal,
                          globalL0AlignmentStorage
                        ).flatMap {
                          case _: Ignore =>
                            Applicative[F].pure(none[Success].asRight[Agg])
                          case alignment =>
                            processAlignment(alignment, bs, ts, als, tls, lcss, as, mptStore).as {
                              val updatedAgg = agg :+ alignment

                              NonEmptyList.fromList(nextSnapshots) match {
                                case Some(next) =>
                                  (next, updatedAgg).asLeft[Result]
                                case None =>
                                  NonEmptyList
                                    .fromList(updatedAgg)
                                    .asRight[Agg]
                              }
                            }
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
              case None => Applicative[F].pure(SnapshotIgnored(globalSnapshotReference))
            }

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
    * `DAGSnapshotProcessor.consumedFieldsToGlobalSnapshotInfo`; the cl1/dl1 ADDITION is `lastCurrencySnapshots`, which
    * `processCurrencySnapshots` reads (`globalState.lastCurrencySnapshots.get(identifier)`) for the metagraph's own currency-genesis
    * bootstrap. `lastCurrencySnapshotsProofs` is left empty — the MPT-format proof carries no such field root and cl1/dl1 never read the
    * proofs map (only the snapshot+info value via `.get(identifier)`).
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
