package io.constellationnetwork.node.shared.domain.tokenlock.block

import cats.Applicative
import cats.syntax.applicative._

import io.constellationnetwork.node.shared.domain.economics.StakeBackingValidator.BackingReplacementRequirement
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockReferenceImmutableCodec}

trait TokenLockBlockAcceptanceContext[F[_]] {

  def getBalance(address: Address): F[Option[Balance]]

  def getLastTxRef(address: Address): F[Option[TokenLockReference]]

  def getInitialTxRef: TokenLockReference

  def getCollateral: Amount

  def getCurrentEpochProgress: EpochProgress

  def getToBeReplacedHashedTokenLocks: List[Hashed[TokenLock]]

  def getBackingReplacementRequirement(ref: Hash): F[Option[BackingReplacementRequirement]]
}

object TokenLockBlockAcceptanceContext {

  /** Static-map factory. Kept for tests and any non-production caller; the production path (`BlockAcceptanceCoordinatorManager.make`) uses
    * [[fromMpt]] instead. See §G4.
    */
  def fromStaticData[F[_]: Applicative](
    balances: Map[Address, Balance],
    lastTxRefs: Map[Address, TokenLockReference],
    collateral: Amount,
    initialTxRef: TokenLockReference,
    toBeReplacedHashedTokenLocks: List[Hashed[TokenLock]],
    currentEpochProgress: EpochProgress,
    backingReplacementRequirements: Map[Hash, BackingReplacementRequirement] = Map.empty
  ): TokenLockBlockAcceptanceContext[F] =
    new TokenLockBlockAcceptanceContext[F] {

      def getBalance(address: Address): F[Option[Balance]] =
        balances.get(address).pure[F]

      def getLastTxRef(address: Address): F[Option[TokenLockReference]] =
        lastTxRefs.get(address).pure[F]

      def getInitialTxRef: TokenLockReference =
        initialTxRef

      def getCollateral: Amount = collateral

      def getCurrentEpochProgress: EpochProgress = currentEpochProgress

      def getToBeReplacedHashedTokenLocks: List[Hashed[TokenLock]] = toBeReplacedHashedTokenLocks

      def getBackingReplacementRequirement(ref: Hash): F[Option[BackingReplacementRequirement]] =
        backingReplacementRequirements.get(ref).pure[F]
    }

  /** §G4: MPT-backed token-lock acceptance context. Mirrors [[BlockAcceptanceContext.fromMpt]] — `getBalance` and `getLastTxRef` are
    * per-address MPT point reads against the branch-aware reader. `getToBeReplacedHashedTokenLocks` keeps its static-list provenance
    * because the caller pre-resolves the replacement-target hashes from the MPT before constructing the context (see
    * `BlockAcceptanceCoordinatorManager.acceptTokenLockBlocks` — pre-existing MPT read at `ActiveTokenLocks`).
    */
  def fromMpt[F[_]](
    reader: GlobalStateReader[F],
    collateral: Amount,
    initialTxRef: TokenLockReference,
    toBeReplacedHashedTokenLocks: List[Hashed[TokenLock]],
    currentEpochProgress: EpochProgress,
    backingReplacementRequirements: Map[Hash, BackingReplacementRequirement] = Map.empty
  )(implicit F: Applicative[F]): TokenLockBlockAcceptanceContext[F] =
    fromMptWithBackingLookup(
      reader,
      collateral,
      initialTxRef,
      toBeReplacedHashedTokenLocks,
      currentEpochProgress,
      ref => backingReplacementRequirements.get(ref).pure[F]
    )

  def fromMptWithBackingLookup[F[_]](
    reader: GlobalStateReader[F],
    collateral: Amount,
    initialTxRef: TokenLockReference,
    toBeReplacedHashedTokenLocks: List[Hashed[TokenLock]],
    currentEpochProgress: EpochProgress,
    backingRequirementLookup: Hash => F[Option[BackingReplacementRequirement]]
  ): TokenLockBlockAcceptanceContext[F] =
    new TokenLockBlockAcceptanceContext[F] {

      def getBalance(address: Address): F[Option[Balance]] =
        reader.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address))

      def getLastTxRef(address: Address): F[Option[TokenLockReference]] =
        reader.get[TokenLockReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, address))

      def getInitialTxRef: TokenLockReference =
        initialTxRef

      def getCollateral: Amount = collateral

      def getCurrentEpochProgress: EpochProgress = currentEpochProgress

      def getToBeReplacedHashedTokenLocks: List[Hashed[TokenLock]] = toBeReplacedHashedTokenLocks

      def getBackingReplacementRequirement(ref: Hash): F[Option[BackingReplacementRequirement]] =
        backingRequirementLookup(ref)
    }

}
