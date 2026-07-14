package io.constellationnetwork.node.shared.http.p2p.clients

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.cluster.services.Session
import io.constellationnetwork.node.shared.http.p2p.PeerResponse
import io.constellationnetwork.node.shared.http.p2p.PeerResponse.PeerResponse
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, SnapshotMetadata}
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.signature.Signed

import fs2.text
import io.circe.magnolia.derivation.decoder.semiauto._
import io.circe.refined._
import io.circe.{Decoder, Json, parser}
import org.http4s.Method.GET
import org.http4s.client.Client

abstract class SnapshotClient[
  F[_]: Async: SecurityProvider,
  S <: Snapshot: Decoder,
  SI <: SnapshotInfo[_]: Decoder
] {
  def client: Client[F]
  def optionalSession: Option[Session[F]]
  def urlPrefix: String

  def getLatestOrdinal: PeerResponse[F, SnapshotOrdinal] = {
    import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

    implicit val decoder: Decoder[SnapshotOrdinal] = deriveMagnoliaDecoder[SnapshotOrdinal]

    PeerResponse[F, SnapshotOrdinal](s"$urlPrefix/latest/ordinal")(client, optionalSession)
  }

  /** Highest snapshot ordinal that has reached finality on the remote node.
    *
    * In BFT mode every snapshot is immediately final, so this returns the same value as [[getLatestOrdinal]]. In Nakamoto mode the chain
    * store tracks finality explicitly via attestation-2/3 OR depth-k confirmation, and this returns the actual finalized ordinal, which
    * lags the head ordinal.
    *
    * Used by CL0's StateChannelBinarySender to gate state-channel-binary pruning on actual finality so reorgs cannot silently drop
    * binaries.
    */
  def getLatestFinalizedOrdinal: PeerResponse[F, SnapshotOrdinal] = {
    import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

    implicit val decoder: Decoder[SnapshotOrdinal] = deriveMagnoliaDecoder[SnapshotOrdinal]

    PeerResponse[F, SnapshotOrdinal](s"$urlPrefix/latest/finalized-ordinal")(client, optionalSession)
  }

  def getLatestMetadata: PeerResponse[F, SnapshotMetadata] = {
    import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

    PeerResponse[F, SnapshotMetadata](s"$urlPrefix/latest/metadata")(client, optionalSession)
  }

  def getLatest: PeerResponse[F, (Signed[S], SI)] =
    PeerResponse.stream[F, (Signed[S], SI)](uri => uri.addPath(s"$urlPrefix/latest/combined/stream"))(client, optionalSession) { body =>
      body
        .through(text.utf8.decode)
        .compile
        .string
        .flatMap { json =>
          for {
            arr <- Async[F].fromEither(parser.decode[List[Json]](json))
            tuple <- arr match {
              case List(snapshotJson, stateJson) =>
                for {
                  snapshot <- Async[F].fromEither(snapshotJson.as[Signed[S]])
                  state <- Async[F].fromEither(stateJson.as[SI])
                } yield (snapshot, state)
              case other =>
                Async[F].raiseError[(Signed[S], SI)](
                  new RuntimeException(s"Unexpected combined snapshot JSON structure: $other")
                )
            }
          } yield tuple
        }
    }

  /** 3c-A — fetch gl0's SIGNED MPT byte map at its latest finalized ordinal alongside the snapshot + its GSI
    * (`docs/serde/FINISH-3C-EXECUTION-PLAN.md` §3c-A). The route returns the JSON triple `[ Signed[snapshot], snapshotInfo (GSI), Map[Hex,
    * Array[Byte]] ]`. A follower loads the third element VERBATIM via `MptStore.loadBytes` (no re-encode), so its
    * `consensusMptRoot(entries) === signed mptRoot` verify gate passes BY CONSTRUCTION — eliminating the `recomputed ≠ signed` drift the
    * `syncFromGlobalSnapshotInfo` re-encode path exhibits. The GSI is carried through ONLY for `setForRecovery` (do NOT re-derive the root
    * from it). Mirrors [[getLatest]]'s stream decode with a third element decoded by `MptStateStorage.mptEntriesDecoder` (the shared
    * byte-map codec anchor).
    */
  def getLatestMptEntries: PeerResponse[F, (Signed[S], SI, Map[Hex, Array[Byte]])] = {
    implicit val entriesDecoder: Decoder[Map[Hex, Array[Byte]]] = MptStateStorage.mptEntriesDecoder

    PeerResponse.stream[F, (Signed[S], SI, Map[Hex, Array[Byte]])](uri => uri.addPath(s"$urlPrefix/latest/combined/mpt-entries"))(
      client,
      optionalSession
    ) { body =>
      body
        .through(text.utf8.decode)
        .compile
        .string
        .flatMap { json =>
          for {
            arr <- Async[F].fromEither(parser.decode[List[Json]](json))
            tuple <- arr match {
              case List(snapshotJson, stateJson, entriesJson) =>
                for {
                  snapshot <- Async[F].fromEither(snapshotJson.as[Signed[S]])
                  state <- Async[F].fromEither(stateJson.as[SI])
                  entries <- Async[F].fromEither(entriesJson.as[Map[Hex, Array[Byte]]])
                } yield (snapshot, state, entries)
              case other =>
                Async[F].raiseError[(Signed[S], SI, Map[Hex, Array[Byte]])](
                  new RuntimeException(s"Unexpected combined mpt-entries JSON structure: $other")
                )
            }
          } yield tuple
        }
    }
  }

  def get(ordinal: SnapshotOrdinal): PeerResponse[F, Signed[S]] = {
    import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

    PeerResponse[F, Signed[S]](s"$urlPrefix/${ordinal.value.value}")(client, optionalSession)
  }

  /** Signed-byte-store backfill transport (2026-07-09) — fetch the peer's SIGNED MPT byte map at an EXACT finalized `ordinal` (the
    * by-ordinal sibling of [[getLatestMptEntries]], serving ONLY the entries map: the caller verifies against its OWN locally-committed
    * `stateProof.mptRoot` at that ordinal, so the peer's snapshot/GSI are not needed). 404 when the peer's signed store has no bytes at
    * `ordinal` (hole / outside its contiguous window / non-global layer) — the caller tries the next peer or fails closed. Decodes with the
    * shared `MptStateStorage.mptEntriesDecoder` wire-codec anchor, identical to the latest-entries route.
    */
  def getMptEntriesAt(ordinal: SnapshotOrdinal): PeerResponse[F, Map[Hex, Array[Byte]]] = {
    import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

    implicit val entriesDecoder: Decoder[Map[Hex, Array[Byte]]] = MptStateStorage.mptEntriesDecoder

    PeerResponse[F, Map[Hex, Array[Byte]]](s"$urlPrefix/${ordinal.value.value}/mpt-entries")(client, optionalSession)
  }

  def get(hash: Hash): PeerResponse[F, Signed[S]] = {
    import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

    PeerResponse[F, Signed[S]](s"$urlPrefix/$hash")(client, optionalSession)
  }

  def getHash(ordinal: SnapshotOrdinal): PeerResponse[F, Option[Hash]] = {
    import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

    PeerResponse(s"$urlPrefix/${ordinal.value.value}/hash", GET)(client, optionalSession) { (req, client) =>
      client.expectOption[Hash](req)
    }
  }
}
