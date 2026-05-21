package io.constellationnetwork.node.shared.http.routes

import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.kes.{KesRegistrationCertValidator, MutableKesRegistry}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.auto._
import io.circe.shapes._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.slf4j.Slf4jLogger
import shapeless._
import shapeless.syntax.singleton._

/** Slice 10 (#179): HTTP intake for runtime KES master-VK registration certs.
  *
  * '''POST `/kes-registration`''' — Submit a `Signed[KesRegistrationCert]`. The route runs the standard
  * [[KesRegistrationCertValidator]] checks against the current epoch (from the head snapshot) and the operator's `lastRef`
  * (looked up in [[MutableKesRegistry]]). On success the cert is handed to the supplied `onAccepted` sink — typically a queue feeding
  * the GSAM event pipeline, mirroring `NodeCollateralRoutes`' `mkCell` callback.
  *
  * '''GET `/kes-registration/{peerId}/last-reference`''' — Returns the operator's most-recent accepted `KesRegistrationReference`, or
  * `KesRegistrationReference.empty` if none. Clients use this to populate the next cert's `parent` field.
  *
  * '''GET `/kes-registration/{peerId}/info`''' — Diagnostic: returns the chain of accepted certs for this operator with their accepted
  * snapshot ordinals, plus an "active VK" hint based on the head snapshot's epoch.
  */
final case class KesRegistrationCertRoutes[F[_]: Async: Hasher](
  onAccepted: Signed[KesRegistrationCert] => F[Unit],
  validator: KesRegistrationCertValidator[F],
  snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  registry: MutableKesRegistry[F]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  private val logger = Slf4jLogger.getLoggerFromName[F]("KesRegistrationLogger")

  protected val prefixPath: InternalUrlPrefix = "/kes-registration"

  /** Look up the operator's latest chain-head reference for the chain-link parent field. */
  private def lastReferenceFor(peerId: PeerId): F[KesRegistrationReference] =
    registry.runtimeCertsFor(peerId).flatMap { records =>
      records.headOption match {
        case None       => KesRegistrationReference.empty.pure[F]
        case Some(head) => head.event.toHashed.map(KesRegistrationReference.of)
      }
    }

  /** Per-operator info: list of accepted records + indication of which one is active at the current epoch. */
  private def infoFor(peerId: PeerId, currentEpoch: EpochProgress): F[KesRegistrationInfo] =
    for {
      records <- registry.runtimeCertsFor(peerId)
      activeRecord = records.find(_.event.value.effectiveFromEpoch <= currentEpoch)
    } yield
      KesRegistrationInfo(
        peerId = peerId,
        records = records,
        activeRecord = activeRecord
      )

  protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root =>
      snapshotStorage.head.flatMap {
        case None => ServiceUnavailable()
        case Some((signedSnapshot, _)) =>
          val currentEpoch = signedSnapshot.value.epochProgress
          for {
            signed <- req.as[Signed[KesRegistrationCert]]
            lastRef <- lastReferenceFor(signed.value.operatorPeerId)
            result <- validator.validate(signed, lastRef, currentEpoch)
            response <- result match {
              case Valid(validSigned) =>
                logger.info(
                  s"Accepted KES registration cert from operator=${signed.value.operatorPeerId.show} " +
                    s"ordinal=${signed.value.ordinal.show} effectiveFromEpoch=${signed.value.effectiveFromEpoch.show}"
                ) >>
                  onAccepted(validSigned) >>
                  validSigned.toHashed.flatMap(hashed => Ok(("hash" ->> hashed.hash) :: HNil))

              case Invalid(errors) =>
                logger.warn(s"Invalid KES registration cert: $errors") >>
                  BadRequest(errors.mkString_("\n"))
            }
          } yield response
      }

    case GET -> Root / peerIdStr / "last-reference" =>
      val peerId = PeerId(io.constellationnetwork.security.hex.Hex(peerIdStr))
      snapshotStorage.head.flatMap {
        case None    => ServiceUnavailable()
        case Some(_) => lastReferenceFor(peerId).flatMap(Ok(_))
      }

    case GET -> Root / peerIdStr / "info" =>
      val peerId = PeerId(io.constellationnetwork.security.hex.Hex(peerIdStr))
      snapshotStorage.head.flatMap {
        case None => ServiceUnavailable()
        case Some((signedSnapshot, _)) =>
          infoFor(peerId, signedSnapshot.value.epochProgress).flatMap(Ok(_))
      }
  }
}

/** Response body for `GET /kes-registration/{peerId}/info`. */
final case class KesRegistrationInfo(
  peerId: PeerId,
  records: List[KesRegistrationRecord],
  activeRecord: Option[KesRegistrationRecord]
)

object KesRegistrationInfo {
  import io.circe.generic.semiauto._
  import io.circe.{Decoder, Encoder}
  implicit val encoder: Encoder[KesRegistrationInfo] = deriveEncoder
  implicit val decoder: Decoder[KesRegistrationInfo] = deriveDecoder
}
