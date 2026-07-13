package io.constellationnetwork.node.shared.infrastructure.snapshot.storage

import cats.Order._
import cats.effect.std.{Queue, Supervisor}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import cats.{Applicative, MonadThrow}

import io.constellationnetwork.cutoff.{LogarithmicOrdinalCutoff, OrdinalCutoff}
import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.ext.cats.syntax.partialPrevious._
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher, HasherSelector}

import eu.timepit.refined.types.numeric.NonNegLong
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.mapref.MapRef
import io.circe.Encoder
import org.typelevel.log4cats.slf4j.Slf4jLogger

object SnapshotStorage {

  /** Tentative snapshot entry — held in memory until confirmed or pruned. */
  private case class TentativeEntry[S, C](snapshot: Signed[S], state: C)

  private def makeResources[F[_]: Async, S <: Snapshot, C <: SnapshotInfo[_]]() = {
    def mkHeadRef = SignallingRef.of[F, Option[(Signed[S], Hasher[F], C)]](none)
    def mkOrdinalCache = MapRef.ofSingleImmutableMap[F, SnapshotOrdinal, Hash](Map.empty)
    def mkHashCache = MapRef.ofSingleImmutableMap[F, Hash, Signed[S]](Map.empty)
    def mkNotPersistedCache = Ref.of(Set.empty[SnapshotOrdinal])
    def mkOffloadQueue = Queue.unbounded[F, SnapshotOrdinal]
    def mkCutoffQueue = Queue.unbounded[F, SnapshotOrdinal]
    def mkTentativeRef = Ref.of[F, Map[SnapshotOrdinal, Map[Hash, TentativeEntry[S, C]]]](Map.empty)

    def mkLogger = Slf4jLogger.create[F]

    (mkHeadRef, mkOrdinalCache, mkHashCache, mkNotPersistedCache, mkOffloadQueue, mkCutoffQueue, mkTentativeRef, mkLogger).mapN {
      (_, _, _, _, _, _, _, _)
    }
  }

  def make[F[_]: Async, S <: Snapshot: Encoder, C <: SnapshotInfo[_]](
    snapshotLocalFileSystemStorage: SnapshotLocalFileSystemStorage[F, S],
    snapshotInfoLocalFileSystemStorage: SnapshotInfoLocalFileSystemStorage[F, _, C],
    inMemoryCapacity: NonNegLong,
    snapshotInfoCutoffOrdinal: SnapshotOrdinal,
    hasherSelector: HasherSelector[F],
    combinedSnapshotCheckpointFileSystemStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, C],
    // Optional finalized-ordinal source. When provided, `setHeadForRecovery` refuses
    // different-hash overwrites at-or-below finalized — a raw-layer safety net so that
    // any direct caller (bypassing NakamotoChainStore.store's guard) still cannot
    // silently rewrite finalized content. BFT consumers leave this None (their BFT
    // recovery path legitimately rewrites content as part of download catch-up).
    // Nakamoto GL0 constructs with Some(nakamotoFinalizedOrdinalRef).
    nakamotoFinalizedOrdinalRef: Option[Ref[F, SnapshotOrdinal]] = None
  )(
    implicit supervisor: Supervisor[F]
  ): F[SnapshotStorage[F, S, C] with LatestBalances[F]] =
    makeResources[F, S, C]().flatMap {
      case (headRef, ordinalCache, hashCache, notPersistedCache, offloadQueue, cutoffQueue, tentativeRef, _) =>
        make(
          headRef,
          ordinalCache,
          hashCache,
          notPersistedCache,
          offloadQueue,
          cutoffQueue,
          tentativeRef,
          snapshotLocalFileSystemStorage,
          snapshotInfoLocalFileSystemStorage,
          inMemoryCapacity,
          snapshotInfoCutoffOrdinal,
          hasherSelector,
          combinedSnapshotCheckpointFileSystemStorage,
          nakamotoFinalizedOrdinalRef
        )
    }

  def make[F[_]: Async, S <: Snapshot: Encoder, C <: SnapshotInfo[_]](
    headRef: SignallingRef[F, Option[(Signed[S], Hasher[F], C)]],
    ordinalCache: MapRef[F, SnapshotOrdinal, Option[Hash]],
    hashCache: MapRef[F, Hash, Option[Signed[S]]],
    notPersistedCache: Ref[F, Set[SnapshotOrdinal]],
    offloadQueue: Queue[F, SnapshotOrdinal],
    snapshotInfoCutoffQueue: Queue[F, SnapshotOrdinal],
    tentativeRef: Ref[F, Map[SnapshotOrdinal, Map[Hash, TentativeEntry[S, C]]]],
    snapshotLocalFileSystemStorage: SnapshotLocalFileSystemStorage[F, S],
    snapshotInfoLocalFileSystemStorage: SnapshotInfoLocalFileSystemStorage[F, _, C],
    inMemoryCapacity: NonNegLong,
    snapshotInfoCutoffOrdinal: SnapshotOrdinal,
    hasherSelector: HasherSelector[F],
    combinedSnapshotCheckpointFileSystemStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, C],
    nakamotoFinalizedOrdinalRef: Option[Ref[F, SnapshotOrdinal]]
  )(implicit supervisor: Supervisor[F]): F[SnapshotStorage[F, S, C] with LatestBalances[F]] = {

    def logger = Slf4jLogger.getLogger[F]

    def cutoffLogic: OrdinalCutoff = LogarithmicOrdinalCutoff.make

    def offloadProcess: Stream[F, Unit] =
      Stream
        .fromQueueUnterminated(offloadQueue)
        .evalMap { cutOffOrdinal =>
          ordinalCache.keys
            .map(_.filter(_ <= cutOffOrdinal))
            .flatMap { toOffload =>
              notPersistedCache.get.map { toPersist =>
                val allOrdinals = toOffload.toSet ++ toPersist

                allOrdinals.map(o => (o, toPersist.contains(o), toOffload.contains(o))).toList.sorted
              }
            }
            .flatMap {
              _.traverse {
                case (ordinal, shouldPersist, shouldOffload) =>
                  def offload: F[Unit] =
                    ordinalCache(ordinal).get.flatMap {
                      case Some(hash) =>
                        hashCache(hash).get.flatMap {
                          case Some(snapshot) =>
                            Applicative[F].whenA(shouldPersist) {
                              hasherSelector.withCurrent { implicit hasher =>
                                snapshotLocalFileSystemStorage.write(snapshot)
                              } >>
                                notPersistedCache.update(current => current - ordinal)
                            } >>
                              Applicative[F].whenA(shouldOffload) {
                                ordinalCache(ordinal).set(none) >>
                                  hashCache(hash).set(none)
                              }
                          case None =>
                            MonadThrow[F].raiseError[Unit](
                              new Throwable("Unexpected state: ordinal and hash found but snapshot not found")
                            )
                        }
                      case None =>
                        MonadThrow[F].raiseError[Unit](
                          new Throwable("Unexpected state: hash not found but ordinal exists")
                        )
                    }

                  offload.handleErrorWith { e =>
                    logger.error(e)(s"Failed offloading global snapshot! Snapshot ordinal=${ordinal.show}")
                  }
              }
            }
        }
        .void

    def snapshotInfoCutoffProcess: Stream[F, Unit] =
      Stream
        .fromQueueUnterminated(snapshotInfoCutoffQueue)
        .evalMap { ordinal =>
          val toKeep = cutoffLogic.cutoff(snapshotInfoCutoffOrdinal, ordinal)

          snapshotInfoLocalFileSystemStorage.listStoredOrdinals.flatMap {
            _.compile.toList
              .map(_.toSet.diff(toKeep).toList)
              .flatMap(_.traverse(snapshotInfoLocalFileSystemStorage.delete))
          }
        }
        .void

    def enqueue(snapshot: Signed[S], snapshotInfo: C)(implicit hasher: Hasher[F]) =
      snapshot.value.hash.flatMap { hash =>
        hashCache(hash).set(snapshot.some) >>
          ordinalCache(snapshot.ordinal).set(hash.some) >>
          snapshotLocalFileSystemStorage.write(snapshot).handleErrorWith { e =>
            snapshotExists(snapshot).ifM(
              logger.info(s"Snapshot is already saved on disk. hash=$hash ordinal=${snapshot.ordinal}"),
              logger.error(e)(s"Failed writing snapshot to disk! hash=$hash ordinal=${snapshot.ordinal}") >>
                notPersistedCache.update(current => current + snapshot.ordinal)
            )
          } >>
          snapshotInfoLocalFileSystemStorage
            .write(snapshot.ordinal, snapshotInfo)
            .attempt
            .flatMap {
              case Right(_) =>
                snapshotInfoCutoffQueue.offer(snapshot.ordinal) >>
                  snapshot.ordinal
                    .partialPreviousN(inMemoryCapacity)
                    .fold(Applicative[F].unit)(offloadQueue.offer) >>
                  combinedSnapshotCheckpointFileSystemStorage.tryWrite(snapshot.ordinal, snapshot, snapshotInfo, hash)
              case Left(e) =>
                logger.error(e)(s"Failed writing snapshot info to disk! ordinal=${snapshot.ordinal}. Skipping cutoff and checkpoint.")
            }
      }

    def snapshotExists(snapshot: Signed[S])(implicit hasher: Hasher[F]): F[Boolean] =
      snapshot.toHashed
        .flatMap(hashed =>
          List(snapshotLocalFileSystemStorage.read(hashed.hash), snapshotLocalFileSystemStorage.read(snapshot.value.ordinal))
            .traverse(_.flatMap(_.traverse(_.toHashed).map(_.fold(false)(_.hash === hashed.hash))))
        )
        .map(_.reduce(_ && _))

    supervisor.supervise(offloadProcess.merge(snapshotInfoCutoffProcess).compile.drain).map { _ =>
      new SnapshotStorage[F, S, C] with LatestBalances[F] {
        def prepend(snapshot: Signed[S], state: C)(implicit hasher: Hasher[F]): F[Boolean] = {

          def offer = enqueue(snapshot, state).as(true)

          def loop(implicit hasher: Hasher[F]): F[Boolean] =
            headRef.access.flatMap {
              case (v, setter) =>
                v match {
                  case None =>
                    setter((snapshot, hasher, state).some).ifM(offer, loop)
                  case Some((current, currentHasher, _)) =>
                    isNextSnapshot(current, currentHasher, snapshot).flatMap { isNext =>
                      if (isNext) setter((snapshot, hasher, state).some).ifM(offer, loop)
                      else
                        logger
                          .debug(s"Trying to prepend ${snapshot.ordinal.show} but the current snapshot is: ${current.ordinal.show}")
                          .as(false)
                    }
                }
            }

          loop
        }

        def head: F[Option[(Signed[S], C)]] = headRef.get.map(_.map { case (snapshot, _, info) => (snapshot, info) })
        def headSnapshot: F[Option[Signed[S]]] = headRef.get.map(_.map(_._1))

        def get(ordinal: SnapshotOrdinal): F[Option[Signed[S]]] =
          ordinalCache(ordinal).get.flatMap {
            case Some(hash) =>
              get(hash).flatMap {
                case some @ Some(_) => (some: Option[Signed[S]]).pure[F]
                // Stale cache entry (hash from a reorged snapshot whose file was deleted).
                // Evict and fall through to disk which has the current ordinal file.
                case None => ordinalCache(ordinal).set(None) >> snapshotLocalFileSystemStorage.read(ordinal)
              }
            case None => snapshotLocalFileSystemStorage.read(ordinal)
          }

        def getHashed(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[F]): F[Option[Hashed[S]]] =
          get(ordinal).flatMap(_.traverse(_.toHashed))

        def get(hash: Hash): F[Option[Signed[S]]] =
          hashCache(hash).get.flatMap {
            case Some(s) => s.some.pure[F]
            case None    => snapshotLocalFileSystemStorage.read(hash)
          }

        def getHash(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[F]): F[Option[Hash]] =
          get(ordinal).flatMap {
            _.traverse(_.toHashed.map(_.hash))
          }

        def setHeadForRecovery(snapshot: Signed[S], state: C)(implicit hasher: Hasher[F]): F[Unit] = {
          // Raw-layer finality-safety net. NakamotoChainStore.store is the primary enforcement
          // point, but any direct caller of setHeadForRecovery (e.g. NakamotoSyncDaemon's gossip
          // accept path, SnapshotLeaderLoop's post-store write) would bypass it. This mirror
          // guard at the storage boundary ensures that a different-hash rewrite at-or-below
          // finalized is refused regardless of caller. BFT constructs without the ref; Nakamoto
          // constructs with Some(nakamotoFinalizedOrdinalRef) and is subject to the check.
          def finalitySafe(hashedNew: Hashed[S]): F[Boolean] =
            nakamotoFinalizedOrdinalRef match {
              case None => true.pure[F]
              case Some(ref) =>
                ref.get.flatMap { finalized =>
                  if (snapshot.ordinal > finalized) true.pure[F]
                  else
                    getHash(snapshot.ordinal).map {
                      case Some(existingHash) => existingHash === hashedNew.hash
                      case None               => true
                    }.flatTap { safe =>
                      Applicative[F].whenA(!safe) {
                        logger.warn(
                          s"[SnapshotStorage] REFUSED setHeadForRecovery: finality-safety violation at ordinal=${snapshot.ordinal.show} " +
                            s"(finalized=${finalized.show}, new=${hashedNew.hash.show.take(12)}). Refusing to rewrite finalized content."
                        )
                      }
                    }
                }
            }

          logger.info(s"[SnapshotStorage] Recovery: setting head to ordinal=${snapshot.ordinal.show}") >>
            snapshot.toHashed.flatMap { hashed =>
              finalitySafe(hashed).ifM(
                // Only delete existing file if a DIFFERENT snapshot exists at this ordinal
                // (Nakamoto reorgs). Don't delete when the same snapshot is re-stored (genesis seeding).
                getHash(snapshot.ordinal).flatMap {
                  case Some(existingHash) if existingHash =!= hashed.hash =>
                    logger.info(s"[SnapshotStorage] Replacing snapshot at ordinal=${snapshot.ordinal.show} (old=${existingHash.show
                        .take(12)}, new=${hashed.hash.show.take(12)})") >>
                      snapshotLocalFileSystemStorage.delete(snapshot.ordinal).attempt.void
                  case _ =>
                    Async[F].unit
                } >>
                  enqueue(snapshot, state) >>
                  // Ensure the ordinal file exists on disk after enqueue. enqueue → write can fail
                  // due to race conditions during concurrent reorgs (UnableToPersistSnapshot when
                  // another thread recreated the ordinal file first, or hash file missing). Write
                  // directly to the ordinal path as a guaranteed fallback — no hash file or link needed.
                  snapshotLocalFileSystemStorage.exists(snapshot.ordinal).flatMap { exists =>
                    if (!exists) snapshotLocalFileSystemStorage.writeUnderOrdinal(snapshot).attempt.void
                    else Async[F].unit
                  } >>
                  headRef.set((snapshot, hasher, state).some).void,
                // Refused — leave storage untouched so the canonical finalized content remains.
                Async[F].unit
              )
            }
        }

        def setTentativeHead(snapshot: Signed[S], state: C)(implicit hasher: Hasher[F]): F[Unit] =
          for {
            hash <- snapshot.toHashed.map(_.hash)
            _ <- logger.info(
              s"[SnapshotStorage] Tentative head: ordinal=${snapshot.ordinal.show} hash=${hash.show.take(16)}"
            )
            _ <- tentativeRef.update { m =>
              val atOrdinal = m.getOrElse(snapshot.ordinal, Map.empty)
              m.updated(snapshot.ordinal, atOrdinal.updated(hash, TentativeEntry(snapshot, state)))
            }
            _ <- headRef.set((snapshot, hasher, state).some)
            // Update caches so get(ordinal) and get(hash) work for tentative snapshots
            _ <- ordinalCache(snapshot.ordinal).set(hash.some)
            _ <- hashCache(hash).set(snapshot.some)
          } yield ()

        def confirmHead(hash: Hash): F[Unit] =
          tentativeRef.get.flatMap { tentatives =>
            tentatives.collectFirst {
              case (ordinal, entries) if entries.contains(hash) => (ordinal, entries(hash))
            } match {
              case Some((ordinal, entry)) =>
                hasherSelector.withCurrent { implicit hasher =>
                  logger.info(s"[SnapshotStorage] Confirming tentative: ordinal=${ordinal.show} hash=${hash.show.take(16)}") >>
                    snapshotLocalFileSystemStorage.delete(ordinal).attempt.void >>
                    enqueue(entry.snapshot, entry.state)
                } >> tentativeRef.update(_.updated(ordinal, Map.empty).filter(_._2.nonEmpty))
              case None =>
                logger
                  .debug(s"[SnapshotStorage] confirmHead: hash=${hash.show.take(16)} not in tentative store (already confirmed or pruned)")
            }
          }

        def pruneTentative(finalizedOrdinal: SnapshotOrdinal): F[Unit] =
          tentativeRef.modify { m =>
            val (stale, remaining) = m.partition(_._1 <= finalizedOrdinal)
            val staleCount = stale.values.map(_.size).sum
            (remaining, staleCount)
          }.flatMap { staleCount =>
            logger
              .info(s"[SnapshotStorage] Pruned $staleCount tentative entries at ordinals <= ${finalizedOrdinal.show}")
              .whenA(staleCount > 0)
          }

        def getLatestBalances: F[Option[Map[Address, Balance]]] =
          headRef.get.map(_.map(_._3.balances))

        def getLatestBalancesStream: Stream[F, Map[Address, Balance]] =
          headRef.discrete
            .map(_.map(_._3))
            .flatMap(_.fold[Stream[F, C]](Stream.empty)(Stream(_)))
            .map(_.balances)

        private def isNextSnapshot(
          current: Signed[S],
          currentHasher: Hasher[F],
          snapshot: Signed[S]
        ): F[Boolean] =
          currentHasher.hash(current.value).map { hash =>
            hash === snapshot.value.lastSnapshotHash && current.value.ordinal.next === snapshot.value.ordinal
          }
      }
    }
  }

}
