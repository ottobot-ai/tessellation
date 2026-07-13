package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot}
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSyncView
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed

import org.typelevel.log4cats.Logger

/** Resolves the two independent references carried by a framework-currency state-channel binary.
  *
  * `metagraphParentOrdinal` is ML0 chain-continuity data only. Eta, active operator keys, KES period, and global reads instead derive from
  * the exact signed GL0 `globalSyncView`. The caller must prove that `(ordinal, hash)` is the current canonical Phase-2 reference before
  * using it. A receiver-local head, an ordinal-only watermark, and the metagraph cadence are never substitutes for that proof.
  *
  * The metagraph parent identity guard remains separate: the binary's `lastSnapshotHash` must equal the recorded or explicitly cached ML0
  * parent. Currency partitions may legitimately omit an otherwise known metagraph, so the ML0 ordinal is decoded from the signed binary;
  * this does not confer any global authority on that self-reported ordinal.
  */
object MetagraphParentOrdinalResolver {

  /** Currency-framework context committed by the signed state-channel content. `metagraphParentOrdinal` is used only for ML0 chain
    * continuity. `gl0Anchor` is the exact global reference that controls eta, active operator keys, KES period, and global reads.
    */
  final case class CurrencyBinaryContext(metagraphParentOrdinal: Long, gl0Anchor: GlobalSyncView)

  /** Capability proving that [[CurrencyBinaryContext.gl0Anchor]] was checked as the exact current Phase-2 GL0 `(ordinal, hash)`. The
    * constructor is private so admission code cannot silently promote an unverified or ordinal-only reference.
    */
  final case class Phase2CurrencyBinaryContext private (
    metagraphParentOrdinal: Long,
    gl0AnchorOrdinal: SnapshotOrdinal,
    gl0AnchorHash: Hash
  )

  object Phase2CurrencyBinaryContext {
    def verify[F[_]: Async](
      context: CurrencyBinaryContext
    )(isExactPhase2: (SnapshotOrdinal, Hash) => F[Boolean]): F[Option[Phase2CurrencyBinaryContext]] =
      isExactPhase2(context.gl0Anchor.ordinal, context.gl0Anchor.hash).map {
        case true =>
          Some(
            Phase2CurrencyBinaryContext(
              context.metagraphParentOrdinal,
              context.gl0Anchor.ordinal,
              context.gl0Anchor.hash
            )
          )
        case false => None
      }
  }

  /** Decode the framework-currency content and require its signed exact GL0 reference. An opaque/custom payload, malformed currency
    * payload, or currency snapshot without `globalSyncView` returns `None`; there is no receiver-head or metagraph-cadence fallback.
    */
  def currencyContextFromContent[F[_]: Async: JsonSerializer](content: Array[Byte]): F[Option[CurrencyBinaryContext]] = {
    def decodeOpt[A: io.circe.Decoder]: F[Option[A]] =
      JsonSerializer[F]
        .deserialize[A](content)
        .map(_.toOption)
        .handleError(_ => Option.empty[A])

    decodeOpt[Signed[CurrencyIncrementalSnapshot]].flatMap {
      case Some(signed) =>
        Async[F].pure(
          signed.value.globalSyncView.map { anchor =>
            CurrencyBinaryContext(math.max(0L, signed.value.ordinal.value.value - 1L), anchor)
          }
        )
      case None =>
        decodeOpt[Signed[CurrencySnapshot]].map(
          _.flatMap { signed =>
            signed.value.globalSyncView.map { anchor =>
              CurrencyBinaryContext(math.max(0L, signed.value.ordinal.value.value - 1L), anchor)
            }
          }
        )
    }
  }

  /** Apply the existing metagraph-parent identity guard, then decode both the ML0 continuity ordinal and exact GL0 anchor from the signed
    * currency content. This method does not assert Phase 2; callers must mint [[Phase2CurrencyBinaryContext]] immediately before use.
    */
  def resolveCurrencyContextFromBinary[F[_]: Async: JsonSerializer: Logger](
    reader: GlobalStateReader[F],
    metagraphAddress: Address,
    parentHash: Hash,
    binaryContent: Array[Byte]
  ): F[Option[CurrencyBinaryContext]] =
    reader.getLastStateChannelSnapshotHash(metagraphAddress).flatMap {
      case Some(storedHash) if storedHash === parentHash =>
        currencyContextFromContent[F](binaryContent).flatTap {
          case None =>
            Logger[F].warn(
              s"Currency binary for mg=$metagraphAddress parent=${parentHash.value.take(12)}... has no decodable signed exact GL0 anchor; deferring"
            )
          case Some(_) => Async[F].unit
        }
      case Some(storedHash) =>
        Logger[F]
          .warn(
            s"Currency binary parent mismatch for mg=$metagraphAddress parent=${parentHash.value.take(12)}... " +
              s"gl0-tip=${storedHash.value.take(12)}...; deferring"
          )
          .as(Option.empty[CurrencyBinaryContext])
      case None => Async[F].pure(Option.empty[CurrencyBinaryContext])
    }

}
