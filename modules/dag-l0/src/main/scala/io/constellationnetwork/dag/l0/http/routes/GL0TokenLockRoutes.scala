package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.ext.http4s.AddressVar
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.auto._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl

final case class GL0TokenLockRoutes[F[_]: Async](
  snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  mptStore: MptStore[F, GlobalStateKey]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  protected val prefixPath: InternalUrlPrefix = "/"

  override protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "token-locks" / AddressVar(address) =>
      // Read directly from MPT — the latest committed snapshot's active set lives there as the source
      // of truth (see #11). The GSI `activeTokenLocks` field is on its way out; we no longer rely on
      // it for HTTP queries. Snapshot-storage availability is still required as the head sentinel so
      // we don't serve stale state before genesis converges.
      snapshotStorage.head.flatMap {
        case Some(_) =>
          mptStore
            .getActiveTokenLocks(address)
            .map(_.getOrElse(SortedSet.empty[Signed[TokenLock]]).toList.map(_.value))
            .flatMap(Ok(_))
        case None => ServiceUnavailable()
      }

  }
}
