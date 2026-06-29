package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.ext.http4s.AddressVar
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.security.HasherSelector

import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl

/** Exposes a metagraph's (CL1) currency-token balance for an address as KNOWN + queryable by global peers — read straight from gl0's MPT
  * (`MgBalances`, reconstructed via
  * [[io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps.getCurrencySnapshotInfo]]), the per-metagraph state
  * gl0 mirrors and commits to via the shard checkpoint `perMetagraphMptRoot`.
  *
  * '''Why this route exists.''' Under roots-only sharding the per-MG currency state lives in the MPT, NOT in the
  * `GlobalSnapshotInfo.lastCurrencySnapshots` blob (which is being eliminated — `info` is no longer the source of truth and is empty in the
  * combined view). So a metagraph-token balance — e.g. a data-application fee that lands in the metagraph token — is held by gl0 but had no
  * read surface. This is that surface: "whatever touches CL1 is tessellation-level, so the global layer knows it." This serves the value
  * (the "known" half); the VERIFIABLE half — an MPT inclusion proof of the `MgBalances` key against the shard checkpoint root — is what
  * [[ShardProofRoutes]] (`POST /shard/{shardId}/proof`) is built for. That route + its `ShardSubtreeProofService` are now wired into the
  * gl0 server on the sharding-active path (`numShards > 1`); at `numShards = 1` the proof route serves 503 (no shard committees to prove
  * against).
  *
  * '''Endpoint''': `GET /currency/{metagraphId}/balance/{address}` → `{ "metagraphId", "address", "balance": <long> }`. Balance is `0` when
  * the metagraph or address is absent (no proof-of-absence in v1 — `0` is the read default). `503` before the snapshot head exists
  * (pre-genesis), mirroring [[GL0TokenLockRoutes]]'s head sentinel.
  *
  * '''Greenfield''' (per `feedback-greenfield-no-wire-compat`): fresh read route, no compat ceremony.
  */
final case class GL0CurrencyBalanceRoutes[F[_]: Async: HasherSelector](
  snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  reader: GlobalStateReader[F]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  protected val prefixPath: InternalUrlPrefix = "/"

  override protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "currency" / AddressVar(metagraphId) / "balance" / AddressVar(address) =>
      // Read the chain's pending-tip view via `pendingReader` (matches GL0TokenLockRoutes); the snapshot-storage head is the sentinel so we
      // never serve state before genesis converges. `getCurrencySnapshotInfo` reconstructs the metagraph's `CurrencySnapshotInfo` from the
      // unrolled `Mg*` MPT partitions, so the balance reflects exactly what gl0 committed to in `perMetagraphMptRoot`.
      snapshotStorage.head.flatMap {
        case Some(_) =>
          HasherSelector[F].withCurrent { implicit hasher =>
            reader.getCurrencySnapshotInfo(metagraphId)
          }
            .map(_.flatMap(_.balances.get(address)).getOrElse(Balance.empty))
            .flatMap { balance =>
              Ok(
                Json.obj(
                  "metagraphId" -> metagraphId.asJson,
                  "address" -> address.asJson,
                  "balance" -> Json.fromLong(balance.value.value)
                )
              )
            }
        case None => ServiceUnavailable()
      }
  }
}
