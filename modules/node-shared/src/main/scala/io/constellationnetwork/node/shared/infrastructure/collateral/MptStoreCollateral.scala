package io.constellationnetwork.node.shared.infrastructure.collateral

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import io.constellationnetwork.node.shared.config.types.CollateralConfig
import io.constellationnetwork.node.shared.domain.collateral.Collateral
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.key.ops.PublicKeyOps

object MptStoreCollateral {

  def make[F[_]: Async: SecurityProvider](
    config: CollateralConfig,
    reader: GlobalStateReader[F]
  ): Collateral[F] =
    new Collateral[F] {

      // Note: `.forall` on `Option[Balance]` returns `true` when `None` (address not found via
      // the reader). This is intentional and matches the previous behavior where an unknown address
      // was considered to have collateral, since the balance map only tracked known addresses.
      def hasCollateral(peerId: PeerId): F[Boolean] =
        peerId.value.toPublicKey
          .map(_.toAddress)
          .flatMap(reader.getBalance)
          .map(_.forall(_.satisfiesCollateral(config.amount)))
    }
}
