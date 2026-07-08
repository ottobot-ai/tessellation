package io.constellationnetwork.dag.l0.infrastructure.snapshot

import java.nio.file.NoSuchFileException

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.cutoff.{LogarithmicOrdinalCutoff, OrdinalCutoff}
import io.constellationnetwork.dag.l0.domain.snapshot.storages.SnapshotDownloadStorage
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{
  CombinedSnapshotCheckpointFileSystemStorage,
  SnapshotInfoLocalFileSystemStorage,
  SnapshotLocalFileSystemStorage
}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.CompatCodecs._
import io.constellationnetwork.validator.StateProofValidator

import io.circe.Json
import org.typelevel.log4cats.slf4j.Slf4jLogger

object SnapshotDownloadStorage {
  def make[F[_]: Async: Parallel: HasherSelector: KryoSerializer: JsonSerializer](
    tmpStorage: SnapshotLocalFileSystemStorage[F, GlobalIncrementalSnapshot],
    persistedStorage: SnapshotLocalFileSystemStorage[F, GlobalIncrementalSnapshot],
    fullGlobalSnapshotStorage: SnapshotLocalFileSystemStorage[F, GlobalSnapshot],
    snapshotInfoStorage: SnapshotInfoLocalFileSystemStorage[F, GlobalSnapshotStateProof, GlobalSnapshotInfo],
    snapshotInfoKryoStorage: SnapshotInfoLocalFileSystemStorage[F, GlobalSnapshotStateProof, GlobalSnapshotInfoV2],
    combinedSnapshotCheckpointFileSystemStorage: CombinedSnapshotCheckpointFileSystemStorage[
      F,
      GlobalIncrementalSnapshot,
      GlobalSnapshotInfo
    ],
    hashSelect: HashSelect,
    mptStore: MptStore[F, GlobalStateKey]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): SnapshotDownloadStorage[F] =
    new SnapshotDownloadStorage[F] {

      val logger = Slf4jLogger.getLogger[F]

      val cutoffLogic: OrdinalCutoff = LogarithmicOrdinalCutoff.make

      private val validator = StateProofValidator.forGlobal(Some(mptStore.underlying))
      private val builder = GlobalSnapshotInfo.stateProofBuilder(Some(mptStore.underlying))

      def readPersisted(ordinal: SnapshotOrdinal): F[Option[Signed[GlobalIncrementalSnapshot]]] = persistedStorage.read(ordinal)

      def readTmp(ordinal: SnapshotOrdinal): F[Option[Signed[GlobalIncrementalSnapshot]]] = tmpStorage.read(ordinal)

      def writeTmp(snapshot: Signed[GlobalIncrementalSnapshot]): F[Unit] =
        tmpStorage.exists(snapshot.ordinal).flatMap(tmpStorage.delete(snapshot.ordinal).whenA) >>
          tmpStorage.writeUnderOrdinal(snapshot)

      def writePersisted(snapshot: Signed[GlobalIncrementalSnapshot]): F[Unit] = HasherSelector[F].withCurrent { implicit hasher =>
        persistedStorage.write(snapshot)
      }

      def deletePersisted(ordinal: SnapshotOrdinal): F[Unit] = persistedStorage.delete(ordinal)

      def isPersisted(hash: Hash): F[Boolean] = persistedStorage.exists(hash)

      def hasCorrectSnapshotInfo(
        ordinal: SnapshotOrdinal,
        proof: GlobalSnapshotStateProof
      )(implicit hasher: Hasher[F]): F[Boolean] =
        (hashSelect.select(ordinal) match {
          case JsonHash => snapshotInfoStorage.read(ordinal).flatMap(_.traverse(builder.buildProof(_, ordinal)))
          case KryoHash =>
            snapshotInfoKryoStorage.read(ordinal).flatMap(_.traverse(i => builder.buildProof(i.toGlobalSnapshotInfo, ordinal)))
        }).map {
          case Some(calculatedProof) => calculatedProof === proof
          case _                     => false
        }

      def getHighestSnapshotInfoOrdinal(lte: SnapshotOrdinal): F[Option[SnapshotOrdinal]] =
        snapshotInfoStorage.listStoredOrdinals
          .flatMap(_.filter(_ <= lte).compile.toList)
          .map(_.maximumOption)

      def readCombined(
        ordinal: SnapshotOrdinal
      )(
        implicit hasher: Hasher[F],
        stateProofSelector: StateProofSelector
      ): F[Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] = {
        val maybeInfo = hashSelect.select(ordinal) match {
          case JsonHash => snapshotInfoStorage.read(ordinal).map(_.map(_.asRight[GlobalSnapshotInfoV2]))
          case KryoHash => snapshotInfoKryoStorage.read(ordinal).map(_.map(_.asLeft[GlobalSnapshotInfo]))
        }

        (readPersisted(ordinal).flatMap(_.traverse(_.toHashed)), maybeInfo).tupled.map(_.tupled).flatMap {
          case Some((snapshot, info)) =>
            for {
              // Typed-scodec sync — writes per-field `ImmutableCodec[V]` bytes that match
              // `mptStateProof` and typed MPT reads. No JSON blob intermediate.
              //
              // FINDING-S01 fail-closed download-replay seed: the persisted GSI has NO field for the MPT-native consensus
              // partitions (`ConsumedAllowSpends` 33 / `Slashings` 34), so a plain from-GSI rebuild would silently seed the
              // replay base WITHOUT the cross-shard spent-set and with a root diverging from the snapshot's SIGNED
              // `stateProof.mptRoot`. `syncFromGlobalSnapshotInfoVerified` is check-then-write: it rebuilds ONLY when a
              // {GSI ∪ preserved 33/34, GSI alone} candidate reproduces the signed root (always at `numShards = 1` / empty
              // spent-set) and otherwise raises BEFORE any store write — previously the same mismatch was caught only AFTER
              // the store was clobbered, by the proof validation below (which stays as the outer full-proof gate).
              // A legacy pre-MPT snapshot (`mptRoot = None`) keeps the plain rebuild, unchanged.
              _ <- info match {
                case Left(value) => ().pure[F]
                case Right(value) =>
                  snapshot.signed.value.stateProof.mptRoot match {
                    case None => mptStore.syncFromGlobalSnapshotInfo(value, ordinal)
                    case signedRoot @ Some(_) =>
                      mptStore.syncFromGlobalSnapshotInfoVerified(value, ordinal, signedRoot).flatMap {
                        case true => ().pure[F]
                        case false =>
                          new Exception(
                            s"Persisted snapshot info at ordinal=${ordinal.show} cannot reproduce the snapshot's SIGNED " +
                              s"stateProof.mptRoot (MPT-native ConsumedAllowSpends/Slashings are not carried by the GSI). " +
                              s"FAILING CLOSED rather than replaying atop a wiped cross-shard spent-set."
                          ).raiseError[F, Unit]
                      }
                  }
              }
              result <- (info match {
                case Left(infoV2) =>
                  infoV2.stateProof(ordinal).flatMap(proof => StateProofValidator.validateProof(snapshot, proof).map(_.isValid))
                case Right(gsi) =>
                  validator.validate(snapshot, gsi).map(_.isValid)
              }).ifM(
                (snapshot.signed, info.leftMap(_.toGlobalSnapshotInfo).fold(identity, identity)).some.pure[F],
                new Exception("Persisted snapshot info does not match the persisted snapshot")
                  .raiseError[F, Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]]
              )
            } yield result
          case _ => none[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)].pure[F]
        }
      }

      def persistSnapshotInfoWithCutoff(ordinal: SnapshotOrdinal, info: GlobalSnapshotInfo): F[Unit] =
        snapshotInfoStorage.write(ordinal, info) >> {
          val toKeep = cutoffLogic.cutoff(SnapshotOrdinal.MinValue, ordinal)

          snapshotInfoStorage.listStoredOrdinals.flatMap {
            _.compile.toList
              .map(_.toSet.diff(toKeep).toList)
              .flatMap(_.traverse_(snapshotInfoStorage.delete))
          }
        }

      def movePersistedToTmp(hash: Hash, ordinal: SnapshotOrdinal): F[Unit] =
        tmpStorage.getPath(hash).flatMap(persistedStorage.move(hash, _) >> persistedStorage.delete(ordinal))

      def moveTmpToPersisted(snapshot: Signed[GlobalIncrementalSnapshot]): F[Unit] =
        HasherSelector[F].withCurrent { implicit hasher =>
          persistedStorage.getPath(snapshot).flatMap(tmpStorage.moveByOrdinal(snapshot, _) >> persistedStorage.link(snapshot))
        }

      def readGenesis(ordinal: SnapshotOrdinal): F[Option[Signed[GlobalSnapshot]]] = fullGlobalSnapshotStorage.read(ordinal)

      def writeGenesis(genesis: Signed[GlobalSnapshot]): F[Unit] = HasherSelector[F].withCurrent { implicit hasher =>
        fullGlobalSnapshotStorage.write(genesis)
      }

      def cleanupAbove(ordinal: SnapshotOrdinal): F[Unit] = {
        val deleteSnapshotInfo = for {
          _ <- logger.info(s"Starting cleanup above ordinal ${ordinal.show}")
          _ <- snapshotInfoStorage
            .deleteAbove(ordinal)
            .handleErrorWith {
              case _: java.nio.file.NoSuchFileException =>
                // Files already deleted - not an error
                logger.debug(s"Snapshot_info files above ${ordinal.show} already deleted or do not exist")
              case err =>
                logger.error(err)(s"Error while deleting snapshot_info files above ${ordinal.show}") >>
                  Async[F].raiseError(err)
            }
          _ <- logger.debug(s"Completed snapshot_info cleanup above ordinal ${ordinal.show}")
        } yield ()

        val cleanupAboveOrdinal = persistedStorage.cleanupAboveOrdinal(ordinal, movePersistedToTmp)

        val verify = for {
          remainingFiles <- persistedStorage
            .findAbove(ordinal)
            .compile
            .count

          _ <-
            if (remainingFiles > 0) {
              throw new RuntimeException(s"Cleanup incomplete: $remainingFiles files still remain above ordinal ${ordinal.show}")
            } else {
              logger.info(s"Cleanup successful: No files remain above ordinal ${ordinal.show}")
            }
        } yield ()

        deleteSnapshotInfo >>
          cleanupAboveOrdinal >>
          verify >>
          combinedSnapshotCheckpointFileSystemStorage.deleteAbove(ordinal) >>
          mptStore.deleteAbove(ordinal)
      }
    }
}
