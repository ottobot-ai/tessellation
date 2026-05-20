package io.constellationnetwork.node.shared.domain.block.processing

import cats.Applicative
import cats.syntax.applicative._

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.BlockReference
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => transactionReferenceImmutableCodec}

import eu.timepit.refined.types.numeric.NonNegLong

trait BlockAcceptanceContext[F[_]] {

  def getBalance(address: Address): F[Option[Balance]]

  def getLastTxRef(address: Address): F[Option[TransactionReference]]

  def getInitialTxRef: TransactionReference

  def getParentUsage(blockReference: BlockReference): F[Option[NonNegLong]]

  def getCollateral: Amount

}

object BlockAcceptanceContext {

  /** Static-map factory. Kept for tests and any non-production caller that still passes pre-extracted GSI views; the production path
    * (`BlockAcceptanceCoordinatorManager.make`) uses [[fromMpt]] instead. See §G4 (GSI → MPT migration of the block-acceptance read path).
    */
  def fromStaticData[F[_]: Applicative](
    balances: Map[Address, Balance],
    lastTxRefs: Map[Address, TransactionReference],
    parentUsages: Map[BlockReference, NonNegLong],
    collateral: Amount,
    initialTxRef: TransactionReference
  ): BlockAcceptanceContext[F] =
    new BlockAcceptanceContext[F] {

      def getBalance(address: Address): F[Option[Balance]] =
        balances.get(address).pure[F]

      def getLastTxRef(address: Address): F[Option[TransactionReference]] =
        lastTxRefs.get(address).pure[F]

      def getInitialTxRef: TransactionReference =
        initialTxRef

      def getParentUsage(blockReference: BlockReference): F[Option[NonNegLong]] =
        parentUsages.get(blockReference).pure[F]

      def getCollateral = collateral
    }

  /** §G4: MPT-backed acceptance context. `getBalance` and `getLastTxRef` become per-address MPT point reads against the branch-aware reader
    * (under MultiBranch this is `GlobalStateReader.pending` resolved to chain bestTip — the same view `lastSnapshotContext` was derived
    * from in §G1). `parentUsages` and `getInitialTxRef` keep their static-map / static-value provenance because tip usages come from the
    * in-memory `TipUsageManager` (not the MPT) and the initial tx ref is a global constant.
    *
    * Reorg-safety: the caller MUST pass a reader scoped to the same branch view as the consensus parent (see §G1 for the `pendingReader`
    * wiring pattern). Reading any other branch would race against in-flight writes and break determinism across nodes.
    */
  def fromMpt[F[_]: Applicative](
    reader: GlobalStateReader[F],
    parentUsages: Map[BlockReference, NonNegLong],
    collateral: Amount,
    initialTxRef: TransactionReference
  ): BlockAcceptanceContext[F] =
    new BlockAcceptanceContext[F] {

      def getBalance(address: Address): F[Option[Balance]] =
        reader.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address))

      def getLastTxRef(address: Address): F[Option[TransactionReference]] =
        reader.get[TransactionReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, address))

      def getInitialTxRef: TransactionReference =
        initialTxRef

      def getParentUsage(blockReference: BlockReference): F[Option[NonNegLong]] =
        parentUsages.get(blockReference).pure[F]

      def getCollateral = collateral
    }

}
