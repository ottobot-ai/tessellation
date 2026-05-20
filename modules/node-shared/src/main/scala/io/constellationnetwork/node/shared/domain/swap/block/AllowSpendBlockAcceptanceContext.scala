package io.constellationnetwork.node.shared.domain.swap.block

import cats.Applicative
import cats.syntax.applicative._

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.swap.AllowSpendReference
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendReferenceImmutableCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

trait AllowSpendBlockAcceptanceContext[F[_]] {

  def getBalance(address: Address): F[Option[Balance]]

  def getLastTxRef(address: Address): F[Option[AllowSpendReference]]

  def getInitialTxRef: AllowSpendReference

  def getCollateral: Amount

}

object AllowSpendBlockAcceptanceContext {

  /** Static-map factory. Kept for tests and any non-production caller; the production path (`BlockAcceptanceCoordinatorManager.make`) uses
    * [[fromMpt]] instead. See §G4.
    */
  def fromStaticData[F[_]: Applicative](
    balances: Map[Address, Balance],
    lastTxRefs: Map[Address, AllowSpendReference],
    collateral: Amount,
    initialTxRef: AllowSpendReference
  ): AllowSpendBlockAcceptanceContext[F] =
    new AllowSpendBlockAcceptanceContext[F] {

      def getBalance(address: Address): F[Option[Balance]] =
        balances.get(address).pure[F]

      def getLastTxRef(address: Address): F[Option[AllowSpendReference]] =
        lastTxRefs.get(address).pure[F]

      def getInitialTxRef: AllowSpendReference =
        initialTxRef

      def getCollateral = collateral
    }

  /** §G4: MPT-backed allow-spend acceptance context. Mirrors [[BlockAcceptanceContext.fromMpt]] — `getBalance` and `getLastTxRef` are
    * per-address MPT point reads against the branch-aware reader. The caller MUST pass a reader scoped to the same branch view as the
    * consensus parent (see §G1 `pendingReader` wiring).
    */
  def fromMpt[F[_]](
    reader: GlobalStateReader[F],
    collateral: Amount,
    initialTxRef: AllowSpendReference
  ): AllowSpendBlockAcceptanceContext[F] =
    new AllowSpendBlockAcceptanceContext[F] {

      def getBalance(address: Address): F[Option[Balance]] =
        reader.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address))

      def getLastTxRef(address: Address): F[Option[AllowSpendReference]] =
        reader.get[AllowSpendReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, address))

      def getInitialTxRef: AllowSpendReference =
        initialTxRef

      def getCollateral = collateral
    }

}
