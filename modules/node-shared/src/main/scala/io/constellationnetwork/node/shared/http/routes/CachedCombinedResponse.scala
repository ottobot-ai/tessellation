package io.constellationnetwork.node.shared.http.routes

import cats.effect._
import cats.syntax.all._

import io.constellationnetwork.json.StreamingCollectionEncoder
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo}
import io.constellationnetwork.security.signature.Signed

import io.circe.{Encoder, Printer}

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
