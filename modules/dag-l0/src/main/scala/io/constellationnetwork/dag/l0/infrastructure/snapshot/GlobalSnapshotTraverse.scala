package io.constellationnetwork.dag.l0.infrastructure.snapshot

import cats.Parallel
import cats.data.NonEmptyChain
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.util.control.NoStackTrace

import io.constellationnetwork.dag.l0.StoragesInitializer.initializeStorages
import io.constellationnetwork.ext.cats.syntax.partialPrevious.catsSyntaxPartialPrevious
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.snapshot.SnapshotContextFunctions
import io.constellationnetwork.node.shared.domain.snapshot.programs.Download
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.CompatCodecs._
import io.constellationnetwork.validator.{GlobalSnapshotActiveEraValidator, StateProofValidator}

import io.circe.Json
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait GlobalSnapshotTraverse[F[_]] {
  def loadChain(): F[(GlobalSnapshotInfo, Signed[GlobalIncrementalSnapshot])]
}

object GlobalSnapshotTraverse {

  def make[F[_]: Async: Parallel: HasherSelector: JsonSerializer](
    loadInc: Hash => F[Option[Signed[GlobalIncrementalSnapshot]]],
    loadFull: Hash => F[Option[Signed[GlobalSnapshot]]],
    loadInfo: SnapshotOrdinal => F[Option[GlobalSnapshotInfo]],
    contextFns: SnapshotContextFunctions[F, GlobalSnapshotArtifact, GlobalSnapshotContext],
    rollbackHash: Hash,
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    globalSnapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    download: Download[F, GlobalIncrementalSnapshot],
    mptStore: MptStore[F, GlobalStateKey]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): GlobalSnapshotTraverse[F] =
    new GlobalSnapshotTraverse[F] {
      implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)
      private val builder = GlobalSnapshotInfo.stateProofBuilder(Some(mptStore.underlying))

      def loadChain(): F[(GlobalSnapshotInfo, Signed[GlobalIncrementalSnapshot])] = {
        def loadIncOrErr(h: Hash) =
          loadInc(h).flatMap(_.liftTo[F](new Exception(s"Incremental snapshot not found during rollback, hash=${h.show}")))

        def loadInfoOrErr(o: SnapshotOrdinal) =
          loadInfo(o).flatMap(_.liftTo[F](new Exception(s"Expected SnapshotInfo not found during rollback, ordinal=${o.show}")))

        def loadFullOrIncOrErr(h: Hash) =
          loadFull(h)
            .map(_.map(_.asRight[Signed[GlobalIncrementalSnapshot]]))
            .flatMap {
              _.fold(loadInc(h).map(_.map(_.asLeft[Signed[GlobalSnapshot]])))(_.some.pure[F])
            }
            .flatMap(_.liftTo[F](new Exception(s"Found neither global snapshot nor global incremental snapshot for hash=${h.show}")))

        def discoverHashesChain(rollbackHash: Hash): F[(Hash, NonEmptyChain[Hash])] =
          (NonEmptyChain.one(rollbackHash), none[SnapshotOrdinal]).tailRecM {
            case (hashes, lastOrdinal) =>
              val lastHash = hashes.head

              loadInc(lastHash)
                .onError(_ => logger.error(s"Error during hash chain discovery at ${lastOrdinal.show} with hash ${lastHash.show}"))
                .flatMap {
                  case Some(inc) =>
                    loadInfo(inc.ordinal).map {
                      case Some(_) =>
                        (hashes, lastOrdinal).asRight[(NonEmptyChain[Hash], Option[SnapshotOrdinal])]
                      case None =>
                        (hashes.prepend(inc.lastSnapshotHash), inc.ordinal.partialPrevious)
                          .asLeft[(NonEmptyChain[Hash], Option[SnapshotOrdinal])]
                    }

                  case None =>
                    (hashes, lastOrdinal).asRight[(NonEmptyChain[Hash], Option[SnapshotOrdinal])].pure[F]
                }
          }.flatMap {
            case (hashes, lastOrdinal) =>
              val hashCandidate = hashes.head

              loadInc(hashCandidate)
                .map(_.fold(hashes.tail)(_ => hashes.toChain))
                .flatMap(c =>
                  NonEmptyChain.fromChain(c) match {
                    case Some(incHashes) =>
                      logger
                        .info(s"Finished rollback chain discovery with hash candidate $hashCandidate at $lastOrdinal")
                        .as((hashCandidate, incHashes))
                    case None => RollbackSnapshotNotFound(rollbackHash).raiseError[F, (Hash, NonEmptyChain[Hash])]
                  }
                )
          }

        for {
          (hashCandidate, incHashesNec) <- discoverHashesChain(rollbackHash)
          _ <- logger.info(s"Rollback hash candidate: ${hashCandidate.show}")
          firstInc <- loadIncOrErr(incHashesNec.head)
          _ <- GlobalSnapshotActiveEraValidator.requireValid[F](firstInc.value)

          firstInfo <- loadFullOrIncOrErr(hashCandidate).flatMap {
            case Left(globalIncrementalSnapshot) => loadInfoOrErr(globalIncrementalSnapshot.ordinal)
            case Right(globalSnapshot)           => globalSnapshot.info.toGlobalSnapshotInfo.pure[F]
          }

          firstInfoCalculatedProof <- HasherSelector[F].withCurrent { implicit hasher =>
            hasher.getLogic(firstInc.ordinal) match {
              case KryoHash =>
                GlobalSnapshotInfoV2.fromGlobalSnapshotInfo(firstInfo).stateProof(firstInc.ordinal)
              case JsonHash =>
                // Typed-scodec sync — writes per-field `ImmutableCodec[V]` bytes that match
                // `mptStateProof` (`buildMptFromBytes`) and the typed MPT reads. No JSON blob
                // intermediate. Idempotent per-ordinal.
                //
                // FINDING-S01 fail-closed rollback seed: the persisted GSI has NO field for the MPT-native consensus
                // partitions (`ConsumedAllowSpends` 33 / `Slashings` 34), so a plain from-GSI rebuild of the empty boot-time
                // store would resurface WITHOUT the cross-shard spent-set and with a root diverging from the snapshot's
                // SIGNED `stateProof.mptRoot`. Seed order: (1) byte-faithful reload of the node's OWN persisted MPT at the
                // rollback ordinal (carries 33/34 verbatim — reproduces the signed root by construction; no needless
                // re-bootstrap); (2) root-verified GSI rebuild (always passes at `numShards = 1` / empty spent-set);
                // (3) FAIL CLOSED — neither source reproduces the signed root, so raise BEFORE any write rather than
                // replaying atop a divergent base (pre-fix this was write-then-detect: the mismatch was only caught after
                // the store was clobbered, by the proof validation below — which stays as the outer full-proof gate).
                {
                  firstInc.value.stateProof.mptRoot match {
                    case None =>
                      // Pre-MPT legacy snapshot: no signed mptRoot to verify against — legacy plain rebuild, unchanged.
                      mptStore.syncFromGlobalSnapshotInfo(firstInfo, firstInc.ordinal)
                    case signedRoot @ Some(_) =>
                      mptStore.syncFromPersistedMptVerified(firstInc.ordinal, signedRoot).flatMap {
                        case true =>
                          logger.info(
                            s"Rollback MPT seed: adopted OWN persisted MPT bytes at ordinal=${firstInc.ordinal.show} " +
                              s"(root == signed stateProof.mptRoot; ConsumedAllowSpends/Slashings preserved verbatim)"
                          )
                        case false =>
                          mptStore.syncFromGlobalSnapshotInfoVerified(firstInfo, firstInc.ordinal, signedRoot).flatMap {
                            case true => ().pure[F]
                            case false =>
                              (new Exception(
                                s"Rollback at ordinal=${firstInc.ordinal.show}: neither the persisted MPT nor the persisted " +
                                  s"GlobalSnapshotInfo reproduces the snapshot's SIGNED stateProof.mptRoot (MPT-native " +
                                  s"ConsumedAllowSpends/Slashings are not carried by the GSI). FAILING CLOSED rather than " +
                                  s"rolling back onto a wiped cross-shard spent-set."
                              )).raiseError[F, Unit]
                          }
                      }
                  }
                } >>
                  builder.buildProof(firstInfo, firstInc.ordinal)
            }
          }

          hashedFirstInc <- HasherSelector[F].withCurrent(implicit hasher => firstInc.toHashed)
          stateProofInvalid <- StateProofValidator.validateProof(hashedFirstInc, firstInfoCalculatedProof).map(_.isInvalid)

          _ <- (new Exception(s"Snapshot info does not match the snapshot at ordinal=${firstInc.ordinal.show}"))
            .raiseError[F, Unit]
            .whenA(stateProofInvalid)

          _ <- HasherSelector[F].withCurrent(implicit hasher =>
            initializeStorages[F](
              globalSnapshotStorage,
              lastNGlobalSnapshotStorage,
              lastGlobalSnapshotStorage,
              download,
              hashedFirstInc,
              firstInfo
            )
          )
          (info, lastInc) <- incHashesNec.tail.foldLeftM((firstInfo, firstInc)) {
            case ((lastCtx, lastInc), hash) =>
              for {
                inc <- loadIncOrErr(hash)
                _ <- GlobalSnapshotActiveEraValidator.requireValid[F](inc.value)

                (hashedInc, (updatedState, _)) <- HasherSelector[F].withCurrent { implicit hasher =>
                  for {
                    hashed <- inc.toHashed
                    context <- contextFns
                      .createContext(lastCtx, lastInc, inc, getGlobalSnapshotByOrdinal)
                      .map(_ -> inc)
                  } yield (hashed, context)
                }
                _ <-
                  if (hashedInc.ordinal > hashedFirstInc.ordinal) {
                    lastNGlobalSnapshotStorage.set(hashedInc, updatedState)
                  } else ().pure
                _ <-
                  if (hashedInc.ordinal > hashedFirstInc.ordinal) {
                    lastGlobalSnapshotStorage.set(hashedInc, updatedState)
                  } else ().pure
                _ <-
                  if (hashedInc.ordinal > hashedFirstInc.ordinal) {
                    HasherSelector[F].withCurrent(implicit hasher => globalSnapshotStorage.prepend(inc, updatedState))
                  } else ().pure
              } yield (updatedState, inc)
          }
        } yield (info, lastInc)
      }
    }

  case class RollbackSnapshotNotFound(h: Hash) extends NoStackTrace {
    override def getMessage: String = s"Rollback snapshot with hash=${h.show} not found!"
  }
}
