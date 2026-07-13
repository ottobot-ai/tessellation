package io.constellationnetwork.node.shared.http.routes

import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator.RegistrationEvaluationContext
import io.constellationnetwork.node.shared.domain.nakamoto.kes.{KesRegistrationCertValidator, MutableKesRegistry}
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
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

/** Preliminary HTTP intake for unified KES+VRF operator-key registration candidates.
  *
  * '''POST `/kes-registration`''' — Submit a `Signed[KesRegistrationCert]`. The route runs the standard [[KesRegistrationCertValidator]]
  * checks against an injected exact candidate-parent context and the operator's `lastRef` (looked up in [[MutableKesRegistry]]). On success
  * the cert is handed to the supplied `onAccepted` sink. The GSAM inclusion path must revalidate against its actual proposal parent and
  * period; route acceptance alone is never canonical registration.
  *
  * '''GET `/kes-registration/{peerId}/last-reference`''' — Returns the operator's most-recent accepted `KesRegistrationReference`, or
  * `KesRegistrationReference.empty` if none. Clients use this to populate the next cert's `parent` field.
  *
  * '''GET `/kes-registration/{peerId}/info`''' — Diagnostic: returns the chain of accepted certs for this operator with their accepted
  * snapshot ordinals, plus an "active VK" hint based on the injected canonical eta period.
  */
final case class KesRegistrationCertRoutes[F[_]: Async: Hasher](
  onAccepted: Signed[KesRegistrationCert] => F[Unit],
  validator: KesRegistrationCertValidator[F],
  registrationContext: F[Option[RegistrationEvaluationContext]],
  registry: MutableKesRegistry[F]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  private val logger = Slf4jLogger.getLoggerFromName[F]("KesRegistrationLogger")

  protected val prefixPath: InternalUrlPrefix = "/kes-registration"

  /** Resolve the exact pointer-selected prior cert once so its chain reference and activation period cannot come from different MPT reads.
    * Returns the empty baseline when no prior runtime pointer exists.
    */
  private def lastRegistrationStateFor(peerId: PeerId): F[(KesRegistrationReference, EtaPeriod)] =
    registry.latestRuntimeCertFor(peerId).flatMap {
      case None => (KesRegistrationReference.empty, EtaPeriod.Zero).pure[F]
      case Some(record) =>
        KesRegistrationReference.of[F](record.event).map(_ -> record.event.value.effectiveFromPeriod)
    }

  /** Per-operator info: list of accepted records + indication of which one is active at the current eta period. */
  private def infoFor(peerId: PeerId, currentPeriod: EtaPeriod): F[KesRegistrationInfo] =
    for {
      records <- registry.runtimeCertsFor(peerId)
      activeRecord = records.find(_.event.value.effectiveFromPeriod <= currentPeriod)
    } yield
      KesRegistrationInfo(
        peerId = peerId,
        records = records,
        activeRecord = activeRecord
      )

  protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root =>
      registrationContext.flatMap {
        case None => ServiceUnavailable()
        case Some(context) =>
          for {
            signed <- req.as[Signed[KesRegistrationCert]]
            (lastRef, lastEffective) <- lastRegistrationStateFor(signed.value.operatorPeerId)
            result <- validator.validate(signed, lastRef, lastEffective, context)
            response <- result match {
              case Valid(validSigned) =>
                logger.info(
                  s"Accepted operator-key registration candidate from operator=${signed.value.operatorPeerId.show} " +
                    s"ordinal=${signed.value.ordinal.show} effectiveFromPeriod=${signed.value.effectiveFromPeriod.show}"
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
      lastRegistrationStateFor(peerId).flatMap { case (lastRef, _) => Ok(lastRef) }

    case GET -> Root / peerIdStr / "info" =>
      val peerId = PeerId(io.constellationnetwork.security.hex.Hex(peerIdStr))
      registrationContext.flatMap {
        case None          => ServiceUnavailable()
        case Some(context) => infoFor(peerId, context.inclusionPeriod).flatMap(Ok(_))
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
