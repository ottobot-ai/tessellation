package io.constellationnetwork.node.shared.http.routes

import cats.effect._
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.ext.http4s.headers.negotiation.resolveEncoder
import io.constellationnetwork.ext.http4s.{BlockingEntityEncoder, HashVar}
import io.constellationnetwork.json.StreamingCollectionEncoder
import io.constellationnetwork.node.shared.config.types.{RouteRateLimiterConfig, SnapshotTimeoutsConfig}
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.ext.http4s.SnapshotOrdinalVar
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{
  CombinedSnapshotCheckpointFileSystemStorage,
  SnapshotLocalFileSystemStorage
}
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, SnapshotMetadata}
import io.constellationnetwork.schema.{GlobalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.signature.Signed

import io.circe.shapes._
import io.circe.{Encoder, Printer}
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.server.middleware.Timeout
import shapeless.HNil
import shapeless.syntax.singleton._

final case class SnapshotRoutes[F[_]: Async, S <: Snapshot: Encoder, SI <: SnapshotInfo[_]: Encoder](
  snapshotStorage: SnapshotStorage[F, S, SI],
  fullGlobalSnapshotStorage: Option[SnapshotLocalFileSystemStorage[F, GlobalSnapshot]],
  prefixPath: InternalUrlPrefix,
  nodeStorage: NodeStorage[F],
  hasherSelector: HasherSelector[F],
  snapshotTimeoutsConfig: SnapshotTimeoutsConfig,
  cachedCombinedResponse: CachedCombinedResponse[F, S, SI],
  combinedSnapshotCheckpointFileSystemStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, SI],
  // Optional override for the finalized-ordinal route. In BFT GL0 mode every snapshot is
  // immediately final, so the default below returns the head ordinal — semantically equivalent
  // to the legacy behavior for any client. In Nakamoto GL0 mode, dag-l0 wires this to the
  // chain store's lastFinalizedOrdinal so CL0 can gate state-channel-binary pruning on
  // actual finality (not just first sight). See task #6 in NAKAMOTO-PLAN.md.
  getFinalizedOrdinal: Option[F[Option[SnapshotOrdinal]]] = None
) extends Http4sDsl[F]
    with PublicRoutes[F]
    with P2PRoutes[F] {

  object FullSnapshotQueryParam extends FlagQueryParamMatcher("full")

  // Use blocking encoder to prevent CPU starvation when serializing large snapshots
  implicit def jsonEncoders[A <: AnyRef: Encoder]: List[EntityEncoder[F, A]] =
    List(BlockingEntityEncoder.blockingJsonEncoder[F, A])

  private val serviceUnavailableNodeNotReady: F[Response[F]] =
    ServiceUnavailable(("message" ->> "Node is not ready yet") :: HNil)

  private def validStateForSnapshotReturn(state: NodeState): Boolean = state === NodeState.Ready

  private def whenNodeReady(action: F[Response[F]]): F[Response[F]] =
    nodeStorage.getNodeState
      .map(validStateForSnapshotReturn)
      .ifM(action, serviceUnavailableNodeNotReady)

  /** The effective latest ordinal for external consumers. When finality gating is active (Nakamoto GL0), this returns the finalized ordinal
    * — strictly <= chain head. All non-GL0 consumers (metagraphs, DAG-L1, dashboards) see only finalized snapshots, preventing
    * fork-confused reads during temporary Nakamoto chain splits. When not active (BFT mode, or non-GL0 layers), returns the head ordinal.
    */
  private def effectiveLatestOrdinal: F[Option[SnapshotOrdinal]] =
    getFinalizedOrdinal match {
      case Some(getFinalized) => getFinalized
      case None               => snapshotStorage.headSnapshot.map(_.map(_.ordinal))
    }

  /** Check if an ordinal is within the finalized range. When finality gating is active, returns true only if ordinal <= finalized. When not
    * active, always returns true.
    */
  private def isOrdinalServable(ordinal: SnapshotOrdinal): F[Boolean] =
    getFinalizedOrdinal match {
      case Some(getFinalized) =>
        getFinalized.map(_.exists(fin => ordinal.value.value <= fin.value.value))
      case None => Async[F].pure(true)
    }

  protected val httpRoutes: HttpRoutes[F] =
    Timeout(snapshotTimeoutsConfig.routes)(
      HttpRoutes.of[F] {
        case GET -> Root / "latest" / "ordinal" =>
          whenNodeReady {
            effectiveLatestOrdinal.flatMap {
              case Some(ordinal) => Ok(("value" ->> ordinal.value.value) :: HNil)
              case None          => NotFound()
            }
          }

        case GET -> Root / "latest" / "finalized-ordinal" =>
          whenNodeReady {
            val finalizedOrdinalF: F[Option[SnapshotOrdinal]] =
              getFinalizedOrdinal.getOrElse(snapshotStorage.headSnapshot.map(_.map(_.ordinal)))
            finalizedOrdinalF.flatMap {
              case Some(ordinal) => Ok(("value" ->> ordinal.value.value) :: HNil)
              case None          => NotFound()
            }
          }

        case GET -> Root / "latest" / "metadata" =>
          whenNodeReady {
            effectiveLatestOrdinal.flatMap {
              case Some(effOrdinal) =>
                hasherSelector.withCurrent { implicit hasher =>
                  snapshotStorage.getHashed(effOrdinal)
                }.flatMap {
                  case Some(snapshot) =>
                    Ok(SnapshotMetadata(snapshot.ordinal, snapshot.hash, snapshot.lastSnapshotHash))
                  case None => NotFound()
                }
              case None => NotFound()
            }
          }

        case req @ GET -> Root / "latest" =>
          whenNodeReady {
            effectiveLatestOrdinal.flatMap {
              case Some(effOrdinal) =>
                resolveEncoder[F, Signed[S]](req) { implicit enc =>
                  snapshotStorage.get(effOrdinal).flatMap {
                    case Some(snapshot) => Ok(snapshot)
                    case _              => NotFound()
                  }
                }
              case None => NotFound()
            }
          }

        case GET -> Root / "latest" / "info" =>
          // Gated on finality: in Nakamoto mode returns info at finalized ordinal,
          // in BFT mode returns head (equivalent since every snapshot is final).
          whenNodeReady {
            effectiveLatestOrdinal.flatMap {
              case Some(ordinal) =>
                snapshotStorage.get(ordinal).flatMap {
                  case Some(snapshot) if snapshot.ordinal === ordinal =>
                    // Found the snapshot — get its context from snapshotStorage head
                    // (context is only available for the current head)
                    snapshotStorage.head.flatMap {
                      case Some((_, info)) => Ok(info)
                      case _               => NotFound()
                    }
                  case _ => NotFound()
                }
              case None => NotFound()
            }
          }

        case GET -> Root / "latest" / "combined" =>
          // Gated on finality: serves the combined snapshot at the finalized ordinal.
          whenNodeReady {
            effectiveLatestOrdinal.flatMap {
              case Some(ordinal) =>
                combinedSnapshotCheckpointFileSystemStorage
                  .getAsHttpResponse(ordinal)
                  .flatMap {
                    case Some(resp) => resp.pure[F]
                    case None =>
                      combinedSnapshotCheckpointFileSystemStorage.getLatestAsHttpResponse.flatMap {
                        case Some(resp) => resp.pure[F]
                        case None       => NotFound()
                      }
                  }
              case None => NotFound()
            }
          }

        case GET -> Root / "latest" / "combined" / "stream" =>
          // Finality-gated: GL0 only tells consumers about finalized snapshots.
          // The alignment loop must handle the case where finalized ordinal
          // hasn't advanced (same ordinal returned on consecutive pulls).
          whenNodeReady {
            effectiveLatestOrdinal.flatMap {
              case Some(ordinal) =>
                combinedSnapshotCheckpointFileSystemStorage
                  .getAsHttpResponse(ordinal)
                  .flatMap {
                    case Some(resp) => resp.pure[F]
                    // Checkpoint file doesn't exist at the exact head ordinal —
                    // checkpoints are written at epoch intervals (every 5 epochs),
                    // so the head typically runs ahead of the latest checkpoint.
                    // Fall back to the most recent checkpoint on disk.
                    case None =>
                      combinedSnapshotCheckpointFileSystemStorage.getLatestAsHttpResponse.flatMap {
                        case Some(resp) => resp.pure[F]
                        case None       => NotFound()
                      }
                  }
              case None => NotFound()
            }
          }

        case GET -> Root / "latest" / "combined" / "checkpoint" / "info" =>
          whenNodeReady {
            combinedSnapshotCheckpointFileSystemStorage.getLatestCheckpointInfo.flatMap(latestCheckpointInfo => Ok(latestCheckpointInfo))
          }

        case GET -> Root / "latest" / "combined" / "checkpoint" / SnapshotOrdinalVar(ordinal) =>
          whenNodeReady {
            isOrdinalServable(ordinal).flatMap {
              case false => NotFound()
              case true =>
                combinedSnapshotCheckpointFileSystemStorage
                  .getAsStream(ordinal)
                  .flatMap {
                    case Some(byteStream) =>
                      Ok(byteStream, org.http4s.headers.`Content-Type`(org.http4s.MediaType.application.json))
                    case None => NotFound()
                  }
            }
          }

        case req @ GET -> Root / SnapshotOrdinalVar(ordinal) :? FullSnapshotQueryParam(fullSnapshot) =>
          whenNodeReady {
            isOrdinalServable(ordinal).flatMap {
              case false => NotFound()
              case true =>
                if (!fullSnapshot)
                  resolveEncoder[F, Signed[S]](req) { implicit enc =>
                    snapshotStorage.get(ordinal).flatMap {
                      case Some(snapshot) => Ok(snapshot)
                      case _              => NotFound()
                    }
                  }
                else
                  fullGlobalSnapshotStorage.map { storage =>
                    resolveEncoder[F, Signed[GlobalSnapshot]](req) { implicit enc =>
                      storage.read(ordinal).flatMap {
                        case Some(snapshot) => Ok(snapshot)
                        case _              => NotFound()
                      }
                    }
                  }.getOrElse(NotFound())
            }
          }

        case GET -> Root / SnapshotOrdinalVar(ordinal) / "hash" =>
          whenNodeReady {
            isOrdinalServable(ordinal).flatMap {
              case false => NotFound()
              case true =>
                hasherSelector.withCurrent { implicit hasher =>
                  snapshotStorage.getHash(ordinal)
                }.flatMap {
                  case None           => NotFound()
                  case Some(snapshot) => Ok(snapshot)
                }
            }
          }

        case req @ GET -> Root / HashVar(hash) =>
          // Hash-based lookup: can't easily gate on finality without resolving
          // the hash to an ordinal first. For now, serve if found — the caller
          // is typically requesting a specific known hash (e.g., rollback anchor).
          whenNodeReady {
            resolveEncoder[F, Signed[S]](req) { implicit enc =>
              snapshotStorage.get(hash).flatMap {
                case Some(snapshot) => Ok(snapshot)
                case _              => NotFound()
              }
            }
          }
      }
    )

  protected val public: HttpRoutes[F] = httpRoutes
  protected val p2p: HttpRoutes[F] = httpRoutes
}

object SnapshotRoutes {
  def make[F[_]: Async, S <: Snapshot: Encoder, SI <: SnapshotInfo[_]: Encoder](
    snapshotStorage: SnapshotStorage[F, S, SI],
    fullGlobalSnapshotStorage: Option[SnapshotLocalFileSystemStorage[F, GlobalSnapshot]],
    prefixPath: InternalUrlPrefix,
    nodeStorage: NodeStorage[F],
    hasherSelector: HasherSelector[F],
    snapshotTimeoutsConfig: SnapshotTimeoutsConfig,
    combinedSnapshotCheckpointFileSystemStorage: CombinedSnapshotCheckpointFileSystemStorage[F, S, SI],
    getFinalizedOrdinal: Option[F[Option[SnapshotOrdinal]]] = None
  ): F[SnapshotRoutes[F, S, SI]] =
    for {
      cachedCombined <- CachedCombinedResponse.make[F, S, SI]
    } yield
      new SnapshotRoutes[F, S, SI](
        snapshotStorage,
        fullGlobalSnapshotStorage,
        prefixPath,
        nodeStorage,
        hasherSelector,
        snapshotTimeoutsConfig,
        cachedCombined,
        combinedSnapshotCheckpointFileSystemStorage,
        getFinalizedOrdinal
      )
}

trait CachedCombinedResponse[F[_], S <: Snapshot, SI <: SnapshotInfo[_]] {
  def get(currentOrdinal: SnapshotOrdinal, snapshot: Signed[S], state: SI): F[Array[Byte]]
}

object CachedCombinedResponse {
  private val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

  def make[F[_]: Async, S <: Snapshot: Encoder, SI <: SnapshotInfo[_]: Encoder]: F[CachedCombinedResponse[F, S, SI]] =
    Ref[F].of(Option.empty[(SnapshotOrdinal, Deferred[F, Either[Throwable, Array[Byte]]])]).map { ref =>
      new CachedCombinedResponse[F, S, SI] {
        private def serialize(snapshot: Signed[S], state: SI): F[Array[Byte]] =
          Async[F].blocking {
            val baos = new java.io.ByteArrayOutputStream()
            val writer = new java.io.OutputStreamWriter(baos, "UTF-8")

            writer.append('[')
            Encoder[Signed[S]] match {
              case sce: StreamingCollectionEncoder[Signed[S]] =>
                sce.streamEncode(snapshot, printer, writer)
              case enc =>
                printer.unsafePrintToAppendable(enc(snapshot), writer)
            }
            writer.append(',')
            Encoder[SI] match {
              case sce: StreamingCollectionEncoder[SI] =>
                sce.streamEncode(state, printer, writer)
              case enc =>
                printer.unsafePrintToAppendable(enc(state), writer)
            }
            writer.append(']')
            writer.flush()
            baos.toByteArray
          }

        def get(currentOrdinal: SnapshotOrdinal, snapshot: Signed[S], state: SI): F[Array[Byte]] =
          ref.get.flatMap {
            case Some((ord, existing)) if ord === currentOrdinal =>
              existing.get.flatMap(Async[F].fromEither)
            case _ =>
              Deferred[F, Either[Throwable, Array[Byte]]].flatMap { newDef =>
                ref.modify {
                  case Some((ord, existing)) if ord === currentOrdinal =>
                    (Some((ord, existing)), existing.get.flatMap(Async[F].fromEither))
                  case _ =>
                    (
                      Some((currentOrdinal, newDef)),
                      serialize(snapshot, state).attempt.flatTap(newDef.complete).flatMap(Async[F].fromEither)
                    )
                }.flatten
              }
          }
      }
    }
}
