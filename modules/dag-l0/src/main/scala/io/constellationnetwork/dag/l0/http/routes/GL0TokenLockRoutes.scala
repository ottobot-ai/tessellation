package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.ext.http4s.AddressVar
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.auto._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl

final case class GL0TokenLockRoutes[F[_]: Async: HasherSelector](
  snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  reader: GlobalStateReader[F]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  protected val prefixPath: InternalUrlPrefix = "/"

  override protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "token-locks" / AddressVar(address) =>
      // Read the chain's pending-tip view via `pendingReader`; under `OverlayMode.MultiBranch`
      // this picks up writes that haven't yet been folded into the base, matching what other
      // gl0 read sites return (#117/#118 Phase 2). Snapshot-storage availability is still
      // required as the head sentinel so we don't serve stale state before genesis converges.
      snapshotStorage.head.flatMap {
        case Some(_) =>
          HasherSelector[F].withCurrent { implicit hasher =>
            reader
              .getActiveTokenLocks(address)
              .map(_.getOrElse(SortedSet.empty[Signed[TokenLock]]).toList.map(_.value))
              .flatMap(Ok(_))
          }
        case None => ServiceUnavailable()
      }

  }
}
