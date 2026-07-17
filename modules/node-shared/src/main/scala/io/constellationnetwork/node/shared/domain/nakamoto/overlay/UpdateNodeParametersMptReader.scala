package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, StrictMptRead}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.unpRecordImmutableCodec

/** Strict point reader for field 12. The physical key is the hash of the sole proof id, so callers must never infer identity from
  * `proofs.head` without first proving that the stored record has exactly one proof and reproduces the requested key.
  */
object UpdateNodeParametersMptReader {

  type Record = (Signed[UpdateNodeParameters], SnapshotOrdinal)

  private val context = "read UpdateNodeParameters"

  private def malformed[F[_]: Async, A](physicalKey: Hex, reason: String): F[A] =
    Async[F].raiseError(StrictMptRead.MalformedConsensusMptValue(context, physicalKey, reason))

  private def inconsistent[F[_]: Async, A](physicalKey: Hex, reason: String): F[A] =
    Async[F].raiseError(StrictMptRead.InconsistentConsensusMptIndex(context, physicalKey, reason))

  def read[F[_]: Async: Hasher](reader: GlobalStateReader[F], requestedId: Id): F[Option[Record]] =
    for {
      key <- GlobalStateKey.updateNodeParametersKey[F](requestedId)
      physicalKey <- GlobalStateKey.toHex[F](key)
      read <- reader.getStrict[Record](key)
      result <- read match {
        case StrictMptRead.Absent               => none[Record].pure[F]
        case StrictMptRead.Malformed(reason, _) => malformed[F, Option[Record]](physicalKey, reason)
        case StrictMptRead.Present(record @ (signed, _), rawBytes) =>
          if (rawBytes != unpRecordImmutableCodec.immutableBytes(record))
            malformed[F, Option[Record]](physicalKey, "non-canonical value encoding")
          else if (signed.proofs.size != 1)
            inconsistent[F, Option[Record]](physicalKey, s"expected exactly one proof, found=${signed.proofs.size}")
          else {
            val proofId = signed.proofs.head.id

            if (proofId != requestedId)
              inconsistent[F, Option[Record]](
                physicalKey,
                s"proof id does not match requested id: requested=$requestedId actual=$proofId"
              )
            else
              GlobalStateKey.updateNodeParametersKey[F](proofId).flatMap(key => GlobalStateKey.toHex[F](key)).flatMap { derivedKey =>
                if (derivedKey != physicalKey)
                  inconsistent[F, Option[Record]](
                    physicalKey,
                    s"proof id does not reproduce physical key: expected=${physicalKey.value} actual=${derivedKey.value}"
                  )
                else record.some.pure[F]
              }
          }
      }
    } yield result
}
