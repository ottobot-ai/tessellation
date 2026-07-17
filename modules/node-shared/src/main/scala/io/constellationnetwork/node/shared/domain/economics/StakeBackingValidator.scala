package io.constellationnetwork.node.shared.domain.economics

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.control.NoStackTrace

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{
  ActiveTokenLockMptReader,
  GlobalStateReader,
  StakeCollateralMptReader
}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, StrictMptRawEntry}
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}

import derevo.cats.{eqv, show}
import derevo.derive

/** Consensus gate for native token locks that back active weight and pending unbonding encumbrances.
  *
  * Only active records contribute stake or collateral weight. Active and pending records both remain economically encumbered until
  * maturity, so every record in either lifecycle must bind one unique, live, native, same-source, indefinite lock whose amount covers the
  * record's effective amount.
  */
object StakeBackingValidator {

  sealed abstract class StakeBackingViolation(message: String) extends RuntimeException(message) with NoStackTrace

  final case class BackingMapKeyMismatch(kind: String, mapKey: Address, source: Address, tokenLockRef: Hash)
      extends StakeBackingViolation(
        s"$kind backing map key differs from record source: key=${mapKey.value.value} source=${source.value.value} " +
          s"tokenLockRef=${tokenLockRef.value}"
      )

  final case class DuplicateActiveTokenLock(tokenLockRef: Hash)
      extends StakeBackingViolation(s"Native active-token-lock reference is not unique: tokenLockRef=${tokenLockRef.value}")

  final case class ActiveTokenLockMapKeyMismatch(mapKey: Address, source: Address)
      extends StakeBackingViolation(
        s"Native active-token-lock map key differs from lock source: key=${mapKey.value.value} source=${source.value.value}"
      )

  case object AtomicBackingStateCaptureUnavailable
      extends StakeBackingViolation("Global-state reader cannot atomically capture stake, collateral, and token-lock partitions")

  final case class MissingBackingTokenLock(kind: String, source: Address, tokenLockRef: Hash)
      extends StakeBackingViolation(
        s"$kind has no live native backing token lock: source=${source.value.value} tokenLockRef=${tokenLockRef.value}"
      )

  final case class BackingSourceMismatch(kind: String, source: Address, lockSource: Address, tokenLockRef: Hash)
      extends StakeBackingViolation(
        s"$kind backing token-lock source mismatch: recordSource=${source.value.value} lockSource=${lockSource.value.value} " +
          s"tokenLockRef=${tokenLockRef.value}"
      )

  final case class NonNativeBackingTokenLock(kind: String, source: Address, tokenLockRef: Hash)
      extends StakeBackingViolation(
        s"$kind backing token lock is not native: source=${source.value.value} tokenLockRef=${tokenLockRef.value}"
      )

  final case class FiniteBackingTokenLock(kind: String, source: Address, tokenLockRef: Hash)
      extends StakeBackingViolation(
        s"$kind backing token lock is finite: source=${source.value.value} tokenLockRef=${tokenLockRef.value}"
      )

  final case class BackingAmountShortfall(kind: String, source: Address, tokenLockRef: Hash, required: Long, actual: Long)
      extends StakeBackingViolation(
        s"$kind backing token-lock amount is insufficient: source=${source.value.value} tokenLockRef=${tokenLockRef.value} " +
          s"required=$required actual=$actual"
      )

  final case class DuplicateStakeBacking(
    tokenLockRef: Hash,
    firstKind: String,
    firstSource: Address,
    secondKind: String,
    secondSource: Address
  ) extends StakeBackingViolation(
        s"One native token lock backs multiple encumbered records: tokenLockRef=${tokenLockRef.value} " +
          s"first=$firstKind:${firstSource.value.value} second=$secondKind:${secondSource.value.value}"
      )

  final case class DuplicateBackingReplacement(tokenLockRef: Hash)
      extends StakeBackingViolation(
        s"Counted backing token lock has multiple accepted replacements: tokenLockRef=${tokenLockRef.value}"
      )

  final case class CyclicBackingReplacement(tokenLockRef: Hash)
      extends StakeBackingViolation(s"Counted backing token-lock replacement chain is cyclic: tokenLockRef=${tokenLockRef.value}")

  final case class CollateralBackingReplacementUnsupported(source: Address, tokenLockRef: Hash)
      extends StakeBackingViolation(
        s"Node-collateral backing replacement requires an atomic effective-reference schema: " +
          s"source=${source.value.value} tokenLockRef=${tokenLockRef.value}"
      )

  final case class PendingBackingReplacementUnsupported(kind: String, source: Address, tokenLockRef: Hash)
      extends StakeBackingViolation(
        s"Pending backing replacement is forbidden until maturity: " +
          s"kind=$kind source=${source.value.value} tokenLockRef=${tokenLockRef.value}"
      )

  final case class SlashedDelegatedBackingReplacement(
    source: Address,
    tokenLockRef: Hash,
    effectiveAmount: Long,
    backingAmount: Long
  ) extends StakeBackingViolation(
        s"Partially slashed delegated stake cannot replace its backing lock until slash conservation is specified: " +
          s"source=${source.value.value} tokenLockRef=${tokenLockRef.value} " +
          s"effectiveAmount=$effectiveAmount backingAmount=$backingAmount"
      )

  final case class InvalidDelegatedBackingReplacement(
    source: Address,
    tokenLockRef: Hash,
    replacementRef: Hash,
    reason: String
  ) extends StakeBackingViolation(
        s"Delegated-stake backing replacement is invalid: source=${source.value.value} tokenLockRef=${tokenLockRef.value} " +
          s"replacementRef=${replacementRef.value} reason=$reason"
      )

  final case class RewardChangedStakeBinding()
      extends StakeBackingViolation("Reward calculation changed delegated-stake authority, amount, or backing reference")

  final case class RewardChangedPendingStakeBinding()
      extends StakeBackingViolation("Reward calculation changed the canonical pending delegated-stake withdrawal state")

  sealed trait EncumbranceBinding {
    def kind: String
    def family: String
    def weightBearing: Boolean
    def mapKey: Address
    def source: Address
    def tokenLockRef: Hash
    def amount: Long
  }

  final case class DelegatedBinding(
    mapKey: Address,
    source: Address,
    tokenLockRef: Hash,
    amount: Long,
    weightBearing: Boolean
  ) extends EncumbranceBinding {
    val family: String = "delegated-stake"
    val kind: String = if (weightBearing) s"active-$family" else s"pending-$family"
  }

  final case class CollateralBinding(
    mapKey: Address,
    source: Address,
    tokenLockRef: Hash,
    amount: Long,
    weightBearing: Boolean
  ) extends EncumbranceBinding {
    val family: String = "node-collateral"
    val kind: String = if (weightBearing) s"active-$family" else s"pending-$family"
  }

  @derive(eqv, show)
  sealed trait BackingReplacementRequirement {
    def source: Address
    def effectiveAmount: Long
    def backingAmount: Long
  }

  @derive(eqv, show)
  final case class DelegatedBackingRequirement(source: Address, effectiveAmount: Long, backingAmount: Long)
      extends BackingReplacementRequirement

  @derive(eqv, show)
  final case class CollateralBackingRequirement(source: Address, effectiveAmount: Long, backingAmount: Long)
      extends BackingReplacementRequirement

  @derive(eqv, show)
  final case class PendingDelegatedBackingRequirement(source: Address, effectiveAmount: Long, backingAmount: Long)
      extends BackingReplacementRequirement

  @derive(eqv, show)
  final case class PendingCollateralBackingRequirement(source: Address, effectiveAmount: Long, backingAmount: Long)
      extends BackingReplacementRequirement

  final case class ValidatedBackingState private (
    activeLocksByRef: Map[Hash, Signed[TokenLock]],
    weightBearingBindings: List[EncumbranceBinding],
    encumbranceBindings: List[EncumbranceBinding]
  ) {
    val replacementRequirements: Map[Hash, BackingReplacementRequirement] =
      encumbranceBindings.map { binding =>
        val backingAmount = activeLocksByRef(binding.tokenLockRef).amount.value.value
        val requirement: BackingReplacementRequirement = binding match {
          case delegated: DelegatedBinding if delegated.weightBearing =>
            DelegatedBackingRequirement(binding.source, binding.amount, backingAmount)
          case _: DelegatedBinding =>
            PendingDelegatedBackingRequirement(binding.source, binding.amount, backingAmount)
          case collateral: CollateralBinding if collateral.weightBearing =>
            CollateralBackingRequirement(binding.source, binding.amount, backingAmount)
          case _: CollateralBinding =>
            PendingCollateralBackingRequirement(binding.source, binding.amount, backingAmount)
        }
        binding.tokenLockRef -> requirement
      }.toMap
  }

  private final case class ReplacementEdge(target: Hash, replacement: Hashed[TokenLock])

  private def encumbranceBindings(
    delegated: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    pendingDelegated: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
    collateral: SortedMap[Address, SortedSet[NodeCollateralRecord]],
    pendingCollateral: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
  ): (List[EncumbranceBinding], List[EncumbranceBinding]) = {
    val activeDelegatedBindings =
      delegated.toList.flatMap {
        case (mapKey, records) =>
          records.toList.map(record =>
            DelegatedBinding(mapKey, record.event.source, record.tokenLockRef, record.amount.value.value, weightBearing = true)
          )
      }
    val pendingDelegatedBindings =
      pendingDelegated.toList.flatMap {
        case (mapKey, records) =>
          records.toList.map(record =>
            DelegatedBinding(mapKey, record.event.source, record.tokenLockRef, record.amount.value.value, weightBearing = false)
          )
      }
    val activeCollateralBindings =
      collateral.toList.flatMap {
        case (mapKey, records) =>
          records.toList.map(record =>
            CollateralBinding(
              mapKey,
              record.event.source,
              record.event.tokenLockRef,
              record.event.amount.value.value,
              weightBearing = true
            )
          )
      }
    val pendingCollateralBindings =
      pendingCollateral.toList.flatMap {
        case (mapKey, records) =>
          records.toList.map(record =>
            CollateralBinding(
              mapKey,
              record.event.source,
              record.event.tokenLockRef,
              record.event.amount.value.value,
              weightBearing = false
            )
          )
      }
    val weightBearing = activeDelegatedBindings ++ activeCollateralBindings
    val encumbered = weightBearing ++ pendingDelegatedBindings ++ pendingCollateralBindings
    val canonicalSort: List[EncumbranceBinding] => List[EncumbranceBinding] =
      _.sortBy(binding => (binding.source.value.value, binding.tokenLockRef.value, binding.kind))

    (canonicalSort(weightBearing), canonicalSort(encumbered))
  }

  private def indexActiveLocks[F[_]: Async: Hasher](
    activeLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    knownRefsByValue: Map[TokenLock, Hash] = Map.empty
  ): F[Map[Hash, Signed[TokenLock]]] =
    activeLocks.toList
      .flatTraverse { case (mapKey, locks) =>
        locks.toList.traverse { lock =>
          if (mapKey =!= lock.source)
            Async[F].raiseError[(Hash, Signed[TokenLock])](ActiveTokenLockMapKeyMismatch(mapKey, lock.source))
          else
            knownRefsByValue
              .get(lock.value)
              .fold(TokenLockReference.of[F](lock).map(_.hash))(_.pure[F])
              .map(_ -> lock)
        }
      }
      .flatMap(
        _.foldLeftM(Map.empty[Hash, Signed[TokenLock]]) {
          case (indexed, (ref, _)) if indexed.contains(ref) =>
            Async[F].raiseError[Map[Hash, Signed[TokenLock]]](DuplicateActiveTokenLock(ref))
          case (indexed, (ref, lock)) =>
            indexed.updated(ref, lock).pure[F]
        }
      )

  private def validateIndexedState[F[_]: Async](
    locksByRef: Map[Hash, Signed[TokenLock]],
    delegated: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    pendingDelegated: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
    collateral: SortedMap[Address, SortedSet[NodeCollateralRecord]],
    pendingCollateral: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
  ): F[ValidatedBackingState] = {
    val (weightBearingBindings, bindings) =
      encumbranceBindings(delegated, pendingDelegated, collateral, pendingCollateral)

    for {
      _ <- bindings.foldLeftM(Map.empty[Hash, EncumbranceBinding]) { (claimed, binding) =>
        if (binding.mapKey =!= binding.source)
          Async[F].raiseError[Map[Hash, EncumbranceBinding]](
            BackingMapKeyMismatch(binding.kind, binding.mapKey, binding.source, binding.tokenLockRef)
          )
        else
          claimed.get(binding.tokenLockRef) match {
            case Some(first) =>
              Async[F].raiseError[Map[Hash, EncumbranceBinding]](
                DuplicateStakeBacking(binding.tokenLockRef, first.kind, first.source, binding.kind, binding.source)
              )
            case None =>
              locksByRef.get(binding.tokenLockRef) match {
                case None =>
                  Async[F].raiseError[Map[Hash, EncumbranceBinding]](
                    MissingBackingTokenLock(binding.kind, binding.source, binding.tokenLockRef)
                  )
                case Some(lock) if lock.source =!= binding.source =>
                  Async[F].raiseError[Map[Hash, EncumbranceBinding]](
                    BackingSourceMismatch(binding.kind, binding.source, lock.source, binding.tokenLockRef)
                  )
                case Some(lock) if lock.currencyId.nonEmpty =>
                  Async[F].raiseError[Map[Hash, EncumbranceBinding]](
                    NonNativeBackingTokenLock(binding.kind, binding.source, binding.tokenLockRef)
                  )
                case Some(lock) if lock.unlockEpoch.nonEmpty =>
                  Async[F].raiseError[Map[Hash, EncumbranceBinding]](
                    FiniteBackingTokenLock(binding.kind, binding.source, binding.tokenLockRef)
                  )
                case Some(lock) if lock.amount.value.value < binding.amount =>
                  Async[F].raiseError[Map[Hash, EncumbranceBinding]](
                    BackingAmountShortfall(
                      binding.kind,
                      binding.source,
                      binding.tokenLockRef,
                      binding.amount,
                      lock.amount.value.value
                    )
                  )
                case Some(_) =>
                  claimed.updated(binding.tokenLockRef, binding).pure[F]
              }
          }
      }
    } yield ValidatedBackingState(locksByRef, weightBearingBindings, bindings)
  }

  /** Validate active weight-bearing records only. Used by genesis, where pending partitions are necessarily empty. */
  def validateActiveState[F[_]: Async: Hasher](
    activeLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    delegated: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    collateral: SortedMap[Address, SortedSet[NodeCollateralRecord]]
  ): F[ValidatedBackingState] =
    validateBackingState(
      activeLocks,
      delegated,
      SortedMap.empty,
      collateral,
      SortedMap.empty
    )

  def validateBackingState[F[_]: Async: Hasher](
    activeLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    delegated: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    pendingDelegated: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
    collateral: SortedMap[Address, SortedSet[NodeCollateralRecord]],
    pendingCollateral: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
  ): F[ValidatedBackingState] =
    indexActiveLocks(activeLocks).flatMap(
      validateIndexedState(_, delegated, pendingDelegated, collateral, pendingCollateral)
    )

  /** Validate a post-transition state while reusing reference hashes already proved for the exact parent.
    *
    * Token-lock references hash the unsigned lock value. Unchanged parent values therefore retain their proven reference; only
    * newly-created or changed values invoke the active-era hasher.
    */
  def validateTransition[F[_]: Async: Hasher](
    parent: ValidatedBackingState,
    activeLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    delegated: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    pendingDelegated: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
    collateral: SortedMap[Address, SortedSet[NodeCollateralRecord]],
    pendingCollateral: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
  ): F[ValidatedBackingState] = {
    val knownRefsByValue = parent.activeLocksByRef.iterator.map { case (ref, lock) => lock.value -> ref }.toMap
    indexActiveLocks(activeLocks, knownRefsByValue).flatMap(
      validateIndexedState(_, delegated, pendingDelegated, collateral, pendingCollateral)
    )
  }

  private final case class DelegatedEconomicShape(
    mapKey: Address,
    event: Signed[io.constellationnetwork.schema.delegatedStake.UpdateDelegatedStake.Create],
    createdAt: io.constellationnetwork.schema.SnapshotOrdinal,
    currentTokenLockRef: Option[Hash],
    currentAmount: Option[io.constellationnetwork.schema.delegatedStake.DelegatedStakeAmount]
  )

  private def delegatedEconomicMultiset(
    state: SortedMap[Address, SortedSet[DelegatedStakeRecord]]
  ): Map[DelegatedEconomicShape, Int] =
    state.iterator
      .flatMap { case (mapKey, records) =>
        records.iterator.map(record =>
          DelegatedEconomicShape(
            mapKey,
            record.event,
            record.createdAt,
            record.currentTokenLockRef,
            record.currentAmount
          )
        )
      }
      .toList
      .groupMapReduce(identity)(_ => 1)(_ + _)

  /** Reward code may alter only the rewards accumulator, never stake authority or backing fields. */
  def requireRewardOnlyChange[F[_]: Async](
    before: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    after: SortedMap[Address, SortedSet[DelegatedStakeRecord]]
  ): F[Unit] =
    Async[F]
      .raiseError[Unit](RewardChangedStakeBinding())
      .unlessA(delegatedEconomicMultiset(before) === delegatedEconomicMultiset(after))

  /** Pending withdrawals do not accrue rewards. A reward callback must return the canonical exact-parent transition byte-for-byte. */
  def requirePendingStateUnchanged[F[_]: Async](
    expected: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]],
    actual: SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]]
  ): F[Unit] =
    Async[F]
      .raiseError[Unit](RewardChangedPendingStakeBinding())
      .unlessA(expected === actual)

  def validateParent[F[_]: Async: Hasher](reader: GlobalStateReader[F]): F[ValidatedBackingState] =
    for {
      tokenLockPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveTokenLocks)
      delegatedPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveDelegatedStakes)
      delegatedWithdrawalPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](
        GlobalStateFieldId.DelegatedStakesWithdrawals
      )
      collateralPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveNodeCollaterals)
      collateralWithdrawalPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](
        GlobalStateFieldId.NodeCollateralWithdrawals
      )
      prefixes =
        List(tokenLockPrefix, delegatedPrefix, delegatedWithdrawalPrefix, collateralPrefix, collateralWithdrawalPrefix)
      captured <- reader.captureRawPrefixesStrict(prefixes).liftTo[F](AtomicBackingStateCaptureUnavailable).flatten
      activeLocks <- ActiveTokenLockMptReader.materializeNativeFromRaw(
        captured.getOrElse(tokenLockPrefix, List.empty[StrictMptRawEntry])
      )
      delegated <- StakeCollateralMptReader.materializeActiveDelegatedStakesFromRaw(
        captured.getOrElse(delegatedPrefix, List.empty[StrictMptRawEntry])
      )
      pendingDelegated <- StakeCollateralMptReader.materializeDelegatedStakeWithdrawalsFromRaw(
        captured.getOrElse(delegatedWithdrawalPrefix, List.empty[StrictMptRawEntry])
      )
      collateral <- StakeCollateralMptReader.materializeActiveNodeCollateralsFromRaw(
        captured.getOrElse(collateralPrefix, List.empty[StrictMptRawEntry])
      )
      pendingCollateral <- StakeCollateralMptReader.materializeNodeCollateralWithdrawalsFromRaw(
        captured.getOrElse(collateralWithdrawalPrefix, List.empty[StrictMptRawEntry])
      )
      validated <- validateBackingState(activeLocks, delegated, pendingDelegated, collateral, pendingCollateral)
    } yield validated

  private def replacementEdges[F[_]: Async: Hasher](acceptedTokenLocks: List[Signed[TokenLock]]): F[List[ReplacementEdge]] =
    acceptedTokenLocks.flatTraverse { lock =>
      lock.replaceTokenLockRef match {
        case None         => List.empty[ReplacementEdge].pure[F]
        case Some(target) => lock.toHashed.map(hashed => List(ReplacementEdge(target, hashed)))
      }
    }

  private def edgesByTarget(edges: List[ReplacementEdge]): Map[Hash, List[ReplacementEdge]] =
    edges.foldLeft(Map.empty[Hash, List[ReplacementEdge]]) { (acc, edge) =>
      acc.updated(edge.target, acc.getOrElse(edge.target, List.empty) :+ edge)
    }

  private def resolveTerminal[F[_]: Async](
    start: Hash,
    byTarget: Map[Hash, List[ReplacementEdge]]
  ): F[Option[Hashed[TokenLock]]] = {
    def loop(current: Hash, visited: Set[Hash]): F[Option[Hashed[TokenLock]]] =
      byTarget.getOrElse(current, List.empty) match {
        case Nil => none[Hashed[TokenLock]].pure[F]
        case _ :: _ :: _ => Async[F].raiseError(DuplicateBackingReplacement(current))
        case edge :: Nil =>
          if (visited.contains(edge.replacement.hash))
            Async[F].raiseError(CyclicBackingReplacement(edge.replacement.hash))
          else
            loop(edge.replacement.hash, visited + edge.replacement.hash).map(_.orElse(edge.replacement.some))
      }

    loop(start, Set(start))
  }

  /** Validate only replacement chains that originate at a counted parent-state backing lock.
    *
    * Unbound replacements retain upstream-v4 behavior. Delegated replacements may move to the terminal lock atomically. Collateral
    * replacements fail until collateral records gain an effective lock reference/amount that can be updated in the same transition.
    */
  def validateAcceptedReplacements[F[_]: Async: Hasher](
    parent: ValidatedBackingState,
    acceptedTokenLocks: List[Signed[TokenLock]]
  ): F[Unit] =
    for {
      edges <- replacementEdges(acceptedTokenLocks)
      byTarget = edgesByTarget(edges)
      _ <- parent.encumbranceBindings.traverse_ { binding =>
        resolveTerminal[F](binding.tokenLockRef, byTarget).flatMap {
          case None => ().pure[F]
          case Some(_) if !binding.weightBearing =>
            Async[F].raiseError[Unit](PendingBackingReplacementUnsupported(binding.kind, binding.source, binding.tokenLockRef))
          case Some(_) if binding.isInstanceOf[CollateralBinding] =>
            Async[F].raiseError[Unit](CollateralBackingReplacementUnsupported(binding.source, binding.tokenLockRef))
          case Some(terminal) =>
            val parentLock = parent.activeLocksByRef(binding.tokenLockRef)
            if (binding.amount < parentLock.amount.value.value)
              Async[F].raiseError[Unit](
                SlashedDelegatedBackingReplacement(
                  binding.source,
                  binding.tokenLockRef,
                  binding.amount,
                  parentLock.amount.value.value
                )
              )
            else {
              val invalidReason =
                if (terminal.currencyId.nonEmpty) "replacement is not native".some
                else if (terminal.source =!= binding.source) "replacement source differs from record source".some
                else if (terminal.unlockEpoch.nonEmpty) "replacement is finite".some
                else if (terminal.amount.value.value < binding.amount) "replacement amount does not cover effective stake".some
                else none[String]

              invalidReason.traverse_(reason =>
                Async[F].raiseError[Unit](
                  InvalidDelegatedBackingReplacement(binding.source, binding.tokenLockRef, terminal.hash, reason)
                )
              )
            }
        }
      }
    } yield ()

  /** Resolve every requested reference through the complete accepted same-ordinal replacement chain.
    *
    * Used by delegated-stake materialization so the effective record never stops at an intermediate lock that is itself replaced in the same
    * snapshot.
    */
  def terminalReplacements[F[_]: Async: Hasher](
    acceptedTokenLocks: List[Signed[TokenLock]],
    startingRefs: List[Hash]
  ): F[Map[Hash, Hashed[TokenLock]]] =
    for {
      edges <- replacementEdges(acceptedTokenLocks)
      byTarget = edgesByTarget(edges)
      replacements <- startingRefs.distinct.sortBy(_.value).flatTraverse { start =>
        resolveTerminal[F](start, byTarget).map(_.toList.map(start -> _))
      }
    } yield replacements.toMap
}
