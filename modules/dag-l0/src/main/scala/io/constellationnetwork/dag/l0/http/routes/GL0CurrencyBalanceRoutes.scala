package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.http4s.AddressVar
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ConsumedAllowSpendStateManager
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
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
  * '''Why this route exists.''' GL0 recreates every CL1 transition and commits the resulting per-MG currency state in the MPT; it does not
  * adopt producer-carried cumulative state. A metagraph-token balance — e.g. a data-application fee that lands in the metagraph token — is
  * therefore held by GL0 but needs a read surface. This serves the value (the "known" half); the VERIFIABLE half — an MPT inclusion proof
  * of the `MgBalances` key against the shard checkpoint root — is what [[ShardProofRoutes]] (`POST /shard/{shardId}/proof`) is built for.
  * That route + its `ShardSubtreeProofService` are now wired into the gl0 server on the sharding-active path (`numShards > 1`); at
  * `numShards = 1` the proof route serves 503.
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

  private val consumedAllowSpendStateManager: ConsumedAllowSpendStateManager[F] =
    ConsumedAllowSpendStateManager.make[F](reader)

  override protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "currency" / AddressVar(metagraphId) / "balance" / AddressVar(address) =>
      // Read the chain's pending-tip view via `pendingReader` (matches GL0TokenLockRoutes); the snapshot-storage head is the sentinel so we
      // never serve state before genesis converges. `getCurrencySnapshotInfo` reconstructs the metagraph's `CurrencySnapshotInfo` from the
      // unrolled `Mg*` MPT partitions, so the balance reflects exactly what gl0 committed to in `perMetagraphMptRoot`.
      //
      // ECONOMIC-TRUTH overlay (cross-shard I-ONCE, read-side W3e): the ATTESTED `MgBalances` value reflects what the metagraph M pushed —
      // which, AFTER a cross-shard allow-spend it never witnessed expires, REFUNDS the source a phantom +amount. So this serving route
      // applies the same effective-balance overlay the SpendActionValidator uses (derived from the already-committed `ConsumedAllowSpends`
      // spent-set), so the served balance is the economically-truthful one (source still debited, destination credited). `epochFor(M)` uses
      // gl0's head `epochProgress` (the route has no per-MG pinned map) — monotonic-safe: it may show the source's debit slightly earlier
      // than the consensus pinned epoch, NEVER later, so it never serves a phantom-refunded (over-stated) balance. Empty spent-set (always
      // at numShards=1) ⇒ effective == attested ⇒ byte-identical to the pre-change route.
      snapshotStorage.head.flatMap {
        case Some((headSigned, _)) =>
          val liveEpoch: EpochProgress = headSigned.value.epochProgress
          HasherSelector[F].withCurrent { implicit hasher =>
            (
              reader.getCurrencySnapshotInfo(metagraphId),
              reader.getMetagraphSyncData(metagraphId),
              consumedAllowSpendStateManager.materializeConsumedAllowSpendsFromMpt
            ).tupled
          }.map {
            case (maybeInfo, metagraphSyncData, spentSet) =>
              val attested: SortedMap[Address, Balance] = maybeInfo.map(_.balances).getOrElse(SortedMap.empty[Address, Balance])
              val pendingGlobalChangeOrdinals = metagraphSyncData.fold(Map.empty[Address, SortedSet[SnapshotOrdinal]]) { syncData =>
                Map(metagraphId -> syncData.unappliedGlobalChangeOrdinals)
              }
              val effective = consumedAllowSpendStateManager.effectiveCurrencyBalances(
                attested,
                metagraphId.some,
                spentSet,
                pendingGlobalChangeOrdinals,
                Map.empty[Address, EpochProgress],
                liveEpoch
              )
              effective.getOrElse(address, Balance.empty)
          }.flatMap { balance =>
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
