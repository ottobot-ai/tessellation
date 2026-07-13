package io.constellationnetwork.node.shared.http.routes

import cats.effect._
import cats.syntax.all._

import io.constellationnetwork.ext.http4s.headers.negotiation.resolveEncoder
import io.constellationnetwork.ext.http4s.{BlockingEntityEncoder, HashVar}
import io.constellationnetwork.node.shared.config.types.SnapshotTimeoutsConfig
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.finality.{FinalityGate, FinalizedSnapshotReader}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.ext.http4s.SnapshotOrdinalVar
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.SnapshotLocalFileSystemStorage
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, SnapshotMetadata}
import io.constellationnetwork.schema.{GlobalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.signature.Signed

import io.circe.Encoder
import io.circe.shapes._
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.server.middleware.Timeout
import shapeless.HNil
import shapeless.syntax.singleton._

final case class SnapshotRoutes[F[_]: Async: FinalityGate, S <: Snapshot: Encoder, SI <: SnapshotInfo[_]: Encoder](
  snapshotStorage: SnapshotStorage[F, S, SI],
  fullGlobalSnapshotStorage: Option[SnapshotLocalFileSystemStorage[F, GlobalSnapshot]],
  prefixPath: InternalUrlPrefix,
  nodeStorage: NodeStorage[F],
  hasherSelector: HasherSelector[F],
  snapshotTimeoutsConfig: SnapshotTimeoutsConfig,
  finalizedReader: FinalizedSnapshotReader[F, S, SI]
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

  // Gating delegates to FinalityGate[F]. ML0 may use its BFT pass-through instance. GL0 currently
  // reads an ordinal-only transitional watermark; target serving is exact-hash Phase 2 and reorg-aware.
  private def effectiveLatestOrdinal: F[Option[SnapshotOrdinal]] =
    FinalityGate[F].finalizedOrdinal

  private def isOrdinalServable(ordinal: SnapshotOrdinal): F[Boolean] =
    FinalityGate[F].isServable(ordinal)

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
            FinalityGate[F].finalizedOrdinal.flatMap {
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
          // BFT mode: head == finalized, so `latestCombinedResponse` (which reads from head) always carries the latest info.
          // Nakamoto mode: returns info from the finalized checkpoint on disk — never pre-finality state.
          // `FinalizedSnapshotReader` encapsulates both; we extract just the info portion from the combined response.
          whenNodeReady {
            snapshotStorage.head.flatMap {
              case Some((snapshot, info)) =>
                effectiveLatestOrdinal.flatMap {
                  case Some(finalized) if snapshot.ordinal === finalized =>
                    // Fast path (both modes agree): head equals finalized.
                    Ok(info)
                  case Some(_) =>
                    // Nakamoto head-ahead-of-finalized path — can't expose head.info (tentative). The reader serves the finalized
                    // checkpoint but currently only exposes combined, not info-only; return 503 so callers retry once finality advances.
                    ServiceUnavailable(("message" ->> "Finalized snapshot info not yet available") :: HNil)
                  case None => NotFound()
                }
              case None => NotFound()
            }
          }

        case GET -> Root / "latest" / "combined" =>
          // Dispatched through `FinalizedSnapshotReader`: BFT reads straight from in-memory head (no disk indirection, no staleness),
          // Nakamoto reads from the on-disk checkpoint at-or-below finalized (tentative state never leaves). See the reader's docs
          // for why the modes diverge.
          whenNodeReady {
            finalizedReader.latestCombinedResponse.flatMap {
              case Some(resp) => resp.pure[F]
              case None       => NotFound()
            }
          }

        case GET -> Root / "latest" / "combined" / "stream" =>
          // Same dispatch as /latest/combined. The /stream variant historically differed only to let clients long-poll for an advancing
          // finalized ordinal — the payload itself is identical, so it shares the reader path.
          whenNodeReady {
            finalizedReader.latestCombinedResponse.flatMap {
              case Some(resp) => resp.pure[F]
              case None       => NotFound()
            }
          }

        case GET -> Root / "latest" / "combined" / "mpt-entries" =>
          // 3c-A serve side (`docs/serde/FINISH-3C-EXECUTION-PLAN.md` §3c-A). Sibling of `/latest/combined`: serves the JSON triple
          // `[ Signed[S], SI, Map[Hex, Array[Byte]] ]` — the finalized snapshot, its GSI, and gl0's SIGNED MPT byte map at that SAME
          // finalized ordinal, read VERBATIM (no re-encode). Finality-gated identically to `/latest/combined` via the reader; `None`
          // (→ NotFound) when no servable snapshot exists, the signed byte file for that ordinal is absent, or on non-global layers
          // (the reader returns `None` when it has no MPT byte store). A follower loads the third element through `MptStore.loadBytes`,
          // so its `sidecarFreeMptRoot(entries) === signed mptRoot` verify gate passes by construction.
          whenNodeReady {
            finalizedReader.latestMptEntriesResponse.flatMap {
              case Some(resp) => resp.pure[F]
              case None       => NotFound()
            }
          }

        case GET -> Root / "latest" / "combined" / "checkpoint" / "info" =>
          // Checkpoint metadata is always disk-backed (even in BFT it points at the preserved on-disk checkpoint consumers pin to).
          // Nakamoto gates the response through finality; BFT returns unconditionally via the reader.
          whenNodeReady {
            finalizedReader.latestCheckpointInfo.flatMap {
              case Some(info) => Ok(info)
              case None       => NotFound()
            }
          }

        case GET -> Root / "latest" / "combined" / "checkpoint" / SnapshotOrdinalVar(ordinal) =>
          whenNodeReady {
            finalizedReader.combinedCheckpointAt(ordinal).flatMap {
              case Some(resp) => resp.pure[F]
              case None       => NotFound()
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

        case GET -> Root / SnapshotOrdinalVar(ordinal) / "mpt-entries" =>
          // Signed-byte-store backfill serve side (2026-07-09) — the by-ordinal sibling of `/latest/combined/mpt-entries`, serving ONLY
          // the signed MPT byte map at `ordinal` (the puller root-verifies against its OWN committed `stateProof.mptRoot` there, so no
          // snapshot/GSI ride along). Finality-gated twice (route `isOrdinalServable` + the reader's own gate); 404 on a hole / pruned
          // ordinal / non-global layer. Consumed by `PinnedCurrencyInfoReader.PinnedByteBackfill` to heal signed-store holes at stamped
          // shard-checkpoint `executionBaseOrdinal`s (the gap>1 / adopt-race residual of the 2026-07-09 token-lock mirror freeze).
          whenNodeReady {
            isOrdinalServable(ordinal).flatMap {
              case false => NotFound()
              case true =>
                finalizedReader.mptEntriesAt(ordinal).flatMap {
                  case Some(resp) => resp.pure[F]
                  case None       => NotFound()
                }
            }
          }

        case req @ GET -> Root / HashVar(hash) =>
          // Hash-based lookup, gated on finality: fetch the snapshot, then refuse
          // to serve if its ordinal is past the finalized head. Closes a leak
          // where a hostile caller could learn a tentative-snapshot hash via the
          // GossipSub stream and pull full content via HTTP, bypassing the
          // "only finalized data leaves GL0" invariant. Sidecar gossip remains
          // the only path for pending (pre-finality) snapshots.
          whenNodeReady {
            resolveEncoder[F, Signed[S]](req) { implicit enc =>
              snapshotStorage.get(hash).flatMap {
                case Some(snapshot) =>
                  isOrdinalServable(snapshot.ordinal).ifM(Ok(snapshot), NotFound())
                case _ => NotFound()
              }
            }
          }
      }
    )

  protected val public: HttpRoutes[F] = httpRoutes
  protected val p2p: HttpRoutes[F] = httpRoutes
}

object SnapshotRoutes {
  def make[F[_]: Async: FinalityGate, S <: Snapshot: Encoder, SI <: SnapshotInfo[_]: Encoder](
    snapshotStorage: SnapshotStorage[F, S, SI],
    fullGlobalSnapshotStorage: Option[SnapshotLocalFileSystemStorage[F, GlobalSnapshot]],
    prefixPath: InternalUrlPrefix,
    nodeStorage: NodeStorage[F],
    hasherSelector: HasherSelector[F],
    snapshotTimeoutsConfig: SnapshotTimeoutsConfig,
    finalizedReader: FinalizedSnapshotReader[F, S, SI]
  ): F[SnapshotRoutes[F, S, SI]] =
    Async[F].pure(
      new SnapshotRoutes[F, S, SI](
        snapshotStorage,
        fullGlobalSnapshotStorage,
        prefixPath,
        nodeStorage,
        hasherSelector,
        snapshotTimeoutsConfig,
        finalizedReader
      )
    )
}
