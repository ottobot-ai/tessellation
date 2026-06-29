package io.constellationnetwork.node.shared.domain.swap

import cats.Applicative
import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNec
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofClient
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator.{SpendActionValidationError, SpendActionValidationErrorOr}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.swap.AllowSpend
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed

import derevo.cats.{eqv, show}
import derevo.derive
import io.circe.parser

trait SpendActionValidator[F[_]] {
  def validate(
    spendAction: SpendAction,
    activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    allBalances: Map[Option[Address], SortedMap[Address, Balance]],
    currencyId: Address
  ): F[SpendActionValidationErrorOr[SpendAction]]

  def validateReturningAcceptedAndRejected(
    spendActions: Map[Address, List[SpendAction]],
    activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
    allBalances: Map[Option[Address], SortedMap[Address, Balance]]
  ): F[(Map[Address, List[SpendAction]], Map[Address, List[(SpendAction, List[SpendActionValidationError])]])]
}

object SpendActionValidator {

  /** W3c read-side EFFECTIVE-balance overlay seam for the CROSS-SHARD balance path. A `(attestedScopeBalances, ownerScope) =>
    * effectiveScopeBalances` function that the validator applies to the per-MG balance it PROVES from another shard
    * ([[ShardSubtreeProofClient.fetchAndVerify]]) BEFORE the balance check, so a cross-shard no-`allowSpendRef` self-spend of a PHANTOM
    * expiry-refund (the owner metagraph autonomously refunds a consumed-and-expired allow-spend's source) is rejected on the cross-shard
    * path just like the same-shard path.
    *
    * '''Why a function and not the manager.''' The arithmetic is REUSED verbatim — the wiring site closes over the gl0-finalized
    * `ConsumedAllowSpends` spent-set + the consensus-pinned epochs and calls `ConsumedAllowSpendStateManager.effectiveCurrencyBalances`.
    * The validator (a `domain.swap` type) is handed only the bound `(balances, scope)` function so it does NOT depend on the
    * `infrastructure.snapshot` manager, and the spent-set / epoch SOURCING (which must be consensus-deterministic) stays the wiring site's
    * responsibility. `ownerScope` is the proven balance's OWNER metagraph (`Some(M)` for the cross-shard balance path); the function
    * filters the spent-set to that scope, so passing the proven `(M′ -> balance)` singleton under `Some(M)` re-subtracts the marker whose
    * `source == M′` once expired.
    *
    * '''Determinism.''' The spent-set is gl0-finalized (cluster-uniform) and `effectiveCurrencyBalances` is deterministic + SATURATING
    * (never raises), so every honest validating node computes the same effective balance — provided the wiring supplies a consensus epoch
    * (gl0 `epochProgress` is always ≥ M's pinned `globalSyncView.epochProgress`, so firing the re-debit on it is conservative: it cancels
    * the phantom at-or-before it appears, never after).
    *
    * '''`numShards = 1`.''' Unreachable — the cross-shard balance path is gated behind `Cross(...)` classification, which never fires at
    * `numShards = 1` (every MG hashes to shard 0). The default [[noEffectiveBalanceOverlay]] is the IDENTITY, so even if it were reached
    * the behaviour is byte-identical (the proven balance is used as-is).
    */
  type CrossShardEffectiveBalanceOverlay =
    (SortedMap[Address, Balance], Option[Address]) => SortedMap[Address, Balance]

  /** Identity overlay — the proven cross-shard balance is checked as-is. The default for every same-shard / unsharded / pre-W3c-wiring call
    * site; keeps `numShards = 1` and the test/legacy paths byte-identical.
    */
  val noEffectiveBalanceOverlay: CrossShardEffectiveBalanceOverlay =
    (balances, _) => balances

  /** Same-shard / unsharded default constructor — preserved for backwards-compat with existing call sites (gl0 / dag-l1 / sdk wiring at
    * `SharedValidators.scala:100` and tests) that don't have a [[ShardSubtreeProofClient]] / [[ShardAssignment]] in scope. Equivalent to
    * `make(ShardSubtreeProofClient.noop, ShardAssignment.make(numShards = 1))` — i.e. the cross-shard branch is unreachable because every
    * MG hashes to shard 0 and the same-shard fast path covers every read.
    *
    * Production sharding wiring uses the [[ShardSubtreeProofClient]]-aware overload below.
    */
  def make[F[_]: Async: Hasher]: SpendActionValidator[F] =
    make(ShardSubtreeProofClient.noop[F], ShardAssignment.make[F](numShards = 1), noEffectiveBalanceOverlay)

  /** Sharding-aware constructor — Slice 11 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8.2.
    *
    * '''Cross-shard read path''': when the validator encounters a `SpendTransaction` whose `currencyId` is `Some(M_y)` where `M_y` belongs
    * to a different shard than the current MG (`shardAssignment.shardIdFor(M_y) =!= shardAssignment.shardIdFor(currencyId)`), the validator
    * routes through `proofClient.fetchAndVerify` to obtain a proven [[AllowSpend]] (for the `allowSpendRef` branch) or a proven [[Balance]]
    * (for the no-`allowSpendRef` branch) from shard Y's last committee-signed checkpoint.
    *
    * '''Same-shard fast path''': when `currencyId` is `None` (DAG hypergraph — always available to every shard) OR `currencyId ==
    * currentMg` (same MG) OR shard assignment matches (`shardIdFor(currencyId) == shardIdFor(currentMg)`), the validator reads from the
    * supplied in-process `activeAllowSpends` / `allBalances` maps without ever consulting the proof client. The cross-shard fetch is
    * reserved for the actual cross-shard case.
    *
    * '''Unavailable proof handling''': when `proofClient.fetchAndVerify` returns `None` (target shard offline, network failure, peer
    * doesn't own the requested MG, etc.) the validator rejects the cross-shard `SpendTransaction` with [[CrossShardProofUnavailable]] — the
    * safe default, as the design doc §8.2 notes: the SpendAction is retried on the next gl0 ord when the proof becomes available.
    *
    * '''Tampered value handling''': the proof's MPT inclusion verifier (run by the client implementation before surfacing the value to the
    * validator) catches structural tampering (witness-chain mismatch). Defence-in-depth at the validator: if the proven value bytes fail to
    * decode as the expected on-chain type ([[AllowSpend]] or [[Balance]]), the SpendTransaction is rejected with
    * [[CrossShardProofTampered]]. This makes a malicious peer that swaps a structurally-valid proof's value bytes (e.g. for a different
    * type) caught structurally at decode time rather than silently mis-validated.
    *
    * @param proofClient
    *   the cross-shard read client. Production wiring uses the HTTP implementation talking to peers' `POST /shard/{shardId}/proof` route
    *   (Slice 10). Single-shard / test clusters pass [[ShardSubtreeProofClient.noop]] — the same-shard fast path then covers every read.
    * @param shardAssignment
    *   the cluster-wide static metagraph→shard map (Slice 3). Used to classify each `currencyId` as same-shard or cross-shard.
    * @param crossShardEffectiveBalanceOverlay
    *   W3c read-side EFFECTIVE-balance overlay applied to the PROVEN cross-shard per-MG balance before the no-`allowSpendRef` balance check
    *   (see [[CrossShardEffectiveBalanceOverlay]]). Defaults to the IDENTITY ([[noEffectiveBalanceOverlay]]) — same-shard / `numShards = 1`
    *   / pre-W3c-wiring call sites are byte-identical. The sharded wiring supplies the spent-set-aware overlay.
    */
  def make[F[_]: Async: Hasher](
    proofClient: ShardSubtreeProofClient[F],
    shardAssignment: ShardAssignment[F],
    crossShardEffectiveBalanceOverlay: CrossShardEffectiveBalanceOverlay = noEffectiveBalanceOverlay
  ): SpendActionValidator[F] = new SpendActionValidator[F] {

    def validateReturningAcceptedAndRejected(
      spendActions: Map[Address, List[SpendAction]],
      activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      allBalances: Map[Option[Address], SortedMap[Address, Balance]]
    ): F[
      (
        Map[Address, List[SpendAction]],
        Map[Address, List[(SpendAction, List[SpendActionValidationError])]]
      )
    ] = {
      def processActionsForCurrency(
        currencyId: Address,
        currencySpendActions: List[SpendAction],
        currentAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      ): F[
        (
          SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
          (Address, (List[(SpendAction, List[SpendActionValidationError])], List[SpendAction]))
        )
      ] =
        currencySpendActions
          .foldLeftM(
            (currentAllowSpends, List.empty[(SpendAction, List[SpendActionValidationError])], List.empty[SpendAction])
          ) {
            case ((allowSpendsAcc, rejectedSpendActions, acceptedSpendActions), action) =>
              validate(action, allowSpendsAcc, allBalances, currencyId).flatMap {
                case Valid(validAction) =>
                  updateCurrentAllowSpendsForValidation(validAction, allowSpendsAcc).map { updated =>
                    (updated, rejectedSpendActions, validAction :: acceptedSpendActions)
                  }
                case Invalid(errors) =>
                  Async[F].pure((allowSpendsAcc, (action -> errors.toNonEmptyList.toList) :: rejectedSpendActions, acceptedSpendActions))
              }
          }
          .map {
            case (updatedAllowSpends, rejected, accepted) =>
              updatedAllowSpends -> (currencyId -> (rejected.reverse, accepted.reverse))
          }

      spendActions.toList
        .foldLeftM(
          (activeAllowSpends, List.empty[(Address, (List[(SpendAction, List[SpendActionValidationError])], List[SpendAction]))])
        ) {
          case ((allowSpendsAcc, results), (currencyId, currencySpendActions)) =>
            processActionsForCurrency(currencyId, currencySpendActions, allowSpendsAcc).map {
              case (updatedAllowSpends, result) =>
                (updatedAllowSpends, result :: results)
            }
        }
        .map {
          case (_, spendTransactionsValidations) =>
            val acceptedSpendActions = spendTransactionsValidations.map {
              case (address, (_, accepted)) => address -> accepted
            }.filter {
              case (_, spendAction) => spendAction.nonEmpty
            }.toMap

            // Preserve ALL rejections per address. The previous `.flatMap ... .toMap` form
            // silently dropped all but the last rejection per address — which meant, when
            // a single metagraph emitted multiple SpendActions and more than one was
            // rejected, only one surfaced in the log / state. The DoubleUseAllowSpend
            // scenario fed two SpendActions (reusing the same allow-spend) through the
            // same address and expected both rejections visible.
            val rejectedSpendActions: Map[Address, List[(SpendAction, List[SpendActionValidationError])]] =
              spendTransactionsValidations.map {
                case (address, (rejected, _)) => address -> rejected
              }.filter {
                case (_, rejected) => rejected.nonEmpty
              }.toMap

            (acceptedSpendActions, rejectedSpendActions)
        }
    }

    def validate(
      spendAction: SpendAction,
      activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      allBalances: Map[Option[Address], SortedMap[Address, Balance]],
      currencyId: Address
    ): F[SpendActionValidationErrorOr[SpendAction]] = {
      val hasDuplicatedAllowSpendReference = spendAction.spendTransactions
        .groupBy(_.allowSpendRef)
        .collect { case (Some(hash), value) => (hash, value) }
        .exists { case (_, value) => value.size > 1 }

      if (hasDuplicatedAllowSpendReference) {
        (DuplicatedAllowSpendReference(
          s"Duplicated allow spend reference in the same SpendAction"
        ): SpendActionValidationError).invalidNec[SpendAction].pure[F]
      } else {
        val validations = spendAction.spendTransactions.traverse { spendTransaction =>
          validateAllowSpendRef(spendTransaction, activeAllowSpends, allBalances, currencyId)
        }

        validations.map(_.sequence.as(spendAction))
      }
    }

    private def updateCurrentAllowSpendsForValidation(
      validAction: SpendAction,
      currentActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
    ): F[SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]] = {
      def removeAllowSpendRef(
        acc: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        currencyId: Option[Address],
        source: Address,
        ref: Hash
      ): F[SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]] = {
        val currencyActiveAllowSpends = acc.get(currencyId)
        currencyActiveAllowSpends.flatMap(_.get(source)) match {
          case Some(allowSpends) =>
            allowSpends.toList.filterA(_.toHashed.map(_.hash =!= ref)).map { filtered =>
              val updatedSet = SortedSet.from(filtered)

              val updatedCurrencyMap =
                if (updatedSet.nonEmpty)
                  acc(currencyId) + (source -> updatedSet)
                else
                  acc(currencyId) - source

              val updatedAllowSpends =
                if (updatedCurrencyMap.nonEmpty)
                  acc + (currencyId -> updatedCurrencyMap)
                else
                  acc - currencyId

              updatedAllowSpends
            }

          case None => acc.pure[F]
        }
      }

      val txnsToRemove = validAction.spendTransactions.collect {
        case txn if txn.allowSpendRef.isDefined =>
          (txn.currencyId.map(_.value), txn.source, txn.allowSpendRef.get)
      }

      txnsToRemove.foldM(currentActiveAllowSpends) {
        case (acc, (currencyId, source, ref)) =>
          removeAllowSpendRef(acc, currencyId, source, ref)
      }
    }

    /** Classify a `SpendTransaction.currencyId` against the validator's current MG (`currencyId`).
      *
      *   - `None` — DAG/hypergraph reference. Always [[Same]] (the `None`-slice of the input maps is the DAG slice and is universally
      *     available to every shard).
      *   - `Some(addr)` where `addr == currencyId` — same-MG reference, [[Same]].
      *   - `Some(addr)` where `shardIdFor(addr) == shardIdFor(currencyId)` — different MG but same shard, [[Same]] (the local maps already
      *     contain it).
      *   - `Some(addr)` otherwise — cross-shard, [[Cross]] (the local maps don't have it; need to fetch via proof client).
      */
    private def classifyReference(
      txCurrencyId: Option[Address],
      currentMg: Address
    ): F[Reference] =
      txCurrencyId match {
        case None                             => (Same: Reference).pure[F]
        case Some(addr) if addr === currentMg => (Same: Reference).pure[F]
        case Some(addr) =>
          for {
            currentShard <- shardAssignment.shardIdFor(currentMg)
            targetShard <- shardAssignment.shardIdFor(addr)
          } yield if (currentShard === targetShard) Same else Cross(addr, targetShard)
      }

    private def validateAllowSpendRef(
      spendTransaction: SpendTransaction,
      currentActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      allBalances: Map[Option[Address], SortedMap[Address, Balance]],
      currencyId: Address
    ): F[SpendActionValidationErrorOr[SpendTransaction]] =
      spendTransaction.allowSpendRef match {
        case Some(allowSpendRef) =>
          // ActiveAllowSpends partition: structured slot is `Some(M_y)` for currency-scoped
          // allow-spends; `None` for DAG-scoped. The cross-shard path applies only when M_y is in
          // a different shard than the validator's current MG.
          classifyReference(spendTransaction.currencyId.map(_.value), currencyId).flatMap {
            case Same =>
              validateAllowSpendRefSameShard(
                spendTransaction,
                allowSpendRef,
                currentActiveAllowSpends,
                currencyId
              )
            case Cross(targetMg, targetShard) =>
              validateAllowSpendRefCrossShard(
                spendTransaction,
                allowSpendRef,
                targetMg,
                targetShard,
                currencyId
              )
          }
        case None =>
          // No allowSpendRef: balance check against the currency-id's balance. Same classification
          // logic — DAG (`None`) and same-shard MGs use the local map; cross-shard fetches via
          // proof client.
          classifyReference(spendTransaction.currencyId.map(_.value), currencyId).flatMap {
            case Same =>
              validateBalanceSameShard(spendTransaction, allBalances, currencyId).pure[F]
            case Cross(targetMg, targetShard) =>
              validateBalanceCrossShard(spendTransaction, targetMg, targetShard, currencyId)
          }
      }

    // -----------------------------------------------------------------------------------------
    // Same-shard read paths (unchanged from the pre-Slice-11 behaviour — direct reads from the
    // in-process maps the caller passes in)
    // -----------------------------------------------------------------------------------------

    private def validateAllowSpendRefSameShard(
      spendTransaction: SpendTransaction,
      allowSpendRef: Hash,
      currentActiveAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
      currencyId: Address
    ): F[SpendActionValidationErrorOr[SpendTransaction]] =
      currentActiveAllowSpends
        .get(spendTransaction.currencyId.map(_.value))
        .map { activeAllowSpends =>
          activeAllowSpends.toList.traverse {
            case (_, hashedAllowSpends) =>
              hashedAllowSpends.toList.traverse(_.toHashed).map { hashedList =>
                hashedList.map(hashed => hashed.hash -> hashed.signed)
              }
          }
            .map(_.flatten.toMap)
            .map { allowSpendHashes =>
              allowSpendHashes.get(allowSpendRef) match {
                case None =>
                  AllowSpendNotFound(
                    s"Allow spend $allowSpendRef not found in currency active allow spends"
                  ).invalidNec[SpendTransaction]

                case Some(signedAllowSpend) =>
                  checkAllowSpendFields(signedAllowSpend.value, spendTransaction, currencyId)
              }
            }
        }
        .getOrElse(
          Applicative[F]
            .pure(
              NoActiveAllowSpends(s"Currency ${spendTransaction.currencyId} not found in active allow spends")
                .invalidNec[SpendTransaction]
            )
        )

    private def validateBalanceSameShard(
      spendTransaction: SpendTransaction,
      allBalances: Map[Option[Address], SortedMap[Address, Balance]],
      currencyId: Address
    ): SpendActionValidationErrorOr[SpendTransaction] = {
      val spendTransactionCurrencyAddress = spendTransaction.currencyId.map(_.value)
      val spendTransactionCurrencyBalances = allBalances.getOrElse(spendTransactionCurrencyAddress, SortedMap.empty[Address, Balance])
      val currencyIdBalance = spendTransactionCurrencyBalances.getOrElse(currencyId, Balance.empty)

      checkBalanceAndSource(spendTransaction, currencyIdBalance, currencyId)
    }

    // -----------------------------------------------------------------------------------------
    // Cross-shard read paths (Slice 11 — fetch via proofClient and use the proven state)
    // -----------------------------------------------------------------------------------------

    private def validateAllowSpendRefCrossShard(
      spendTransaction: SpendTransaction,
      allowSpendRef: Hash,
      targetMg: Address,
      targetShard: io.constellationnetwork.schema.sharding.ShardId,
      currencyId: Address
    ): F[SpendActionValidationErrorOr[SpendTransaction]] = {
      // Cross-shard ActiveAllowSpends lookup: ask shard Y to prove that `allowSpendRef` maps to
      // a signed AllowSpend whose source matches `spendTransaction.source`. v1 of Slice 11
      // queries the per-(MG, source) key in the ActiveAllowSpends partition; the value is the
      // serialized SortedSet[Signed[AllowSpend]] for that source — same shape as the in-process
      // map's leaf value. The validator then scans it for the matching `allowSpendRef` hash.
      val key = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, targetMg, spendTransaction.source)

      proofClient.fetchAndVerify(targetShard, targetMg, key).flatMap {
        case None =>
          // No proof obtained — target shard offline / network failure / etc. Reject for retry.
          (CrossShardProofUnavailable(
            s"could not fetch cross-shard proof for ($targetMg, $key) from shard $targetShard"
          ): SpendActionValidationError).invalidNec[SpendTransaction].pure[F]

        case Some((None, _)) =>
          // Proof of non-membership (or v2 absence-witness). Treat as "AllowSpend not present".
          (AllowSpendNotFound(
            s"Allow spend $allowSpendRef not found in cross-shard active allow spends (shard $targetShard, mg $targetMg)"
          ): SpendActionValidationError).invalidNec[SpendTransaction].pure[F]

        case Some((Some(valueBytes), _)) =>
          decodeCrossShardAllowSpends(valueBytes).flatMap {
            case Left(err) =>
              (err: SpendActionValidationError).invalidNec[SpendTransaction].pure[F]
            case Right(signedAllowSpends) =>
              signedAllowSpends.toList.traverse(_.toHashed).map { hashedList =>
                hashedList.find(_.hash === allowSpendRef) match {
                  case None =>
                    AllowSpendNotFound(
                      s"Allow spend $allowSpendRef not found in cross-shard active allow spends (shard $targetShard, mg $targetMg)"
                    ).invalidNec[SpendTransaction]
                  case Some(hashed) =>
                    checkAllowSpendFields(hashed.signed.value, spendTransaction, currencyId)
                }
              }
          }
      }
    }

    private def validateBalanceCrossShard(
      spendTransaction: SpendTransaction,
      targetMg: Address,
      targetShard: io.constellationnetwork.schema.sharding.ShardId,
      currencyId: Address
    ): F[SpendActionValidationErrorOr[SpendTransaction]] = {
      // Cross-shard Balances lookup: ask shard Y to prove the Balance for `currencyId` under the
      // Balances partition of metagraph M_y. (The `currencyId` here is the validator's current
      // MG — the SpendAction's emitter — which is the address whose balance we check against the
      // SpendAction's amount, per the no-allowSpendRef branch's semantics.)
      val key = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, targetMg, currencyId)

      // W3c proof-path inflation residual: the proven `balance` is the RAW committee-attested
      // `MgBalances[targetMg][currencyId]` value, which — once a cross-shard allow-spend targetMg never
      // witnessed expires — includes the PHANTOM refund targetMg autonomously credited its source. Apply
      // the gl0-finalized `ConsumedAllowSpends` spent-set overlay (the same `effectiveCurrencyBalances`
      // the same-shard path uses) BEFORE the balance check: scope the proven value as the singleton
      // `{ currencyId -> balance }` under the OWNER metagraph (`Some(targetMg)`) so a marker whose
      // `source == currencyId` (= this spender) re-subtracts the refund once expired. So a no-allowSpendRef
      // self-spend of the phantom-refunded amount is rejected on the cross-shard path too. The overlay is
      // the IDENTITY unless the sharded wiring supplies a non-empty spent-set ⇒ byte-identical otherwise.
      def effectiveBalanceFor(provenBalance: Balance): Balance =
        crossShardEffectiveBalanceOverlay(SortedMap(currencyId -> provenBalance), targetMg.some)
          .getOrElse(currencyId, provenBalance)

      proofClient.fetchAndVerify(targetShard, targetMg, key).flatMap {
        case None =>
          (CrossShardProofUnavailable(
            s"could not fetch cross-shard balance proof for ($targetMg, $currencyId) from shard $targetShard"
          ): SpendActionValidationError).invalidNec[SpendTransaction].pure[F]

        case Some((None, _)) =>
          // Proof of non-membership → balance is 0 (absent key in the Balances partition is
          // semantically the empty balance, matching the same-shard `getOrElse(_, Balance.empty)`).
          // The overlay still applies (a marker may CREDIT `currencyId` even with a 0 attested base).
          checkBalanceAndSource(spendTransaction, effectiveBalanceFor(Balance.empty), currencyId).pure[F]

        case Some((Some(valueBytes), _)) =>
          decodeCrossShardBalance(valueBytes) match {
            case Left(err) =>
              (err: SpendActionValidationError).invalidNec[SpendTransaction].pure[F]
            case Right(balance) =>
              checkBalanceAndSource(spendTransaction, effectiveBalanceFor(balance), currencyId).pure[F]
          }
      }
    }

    // -----------------------------------------------------------------------------------------
    // Shared structural checks (same logic on the same-shard and cross-shard paths)
    // -----------------------------------------------------------------------------------------

    private def checkAllowSpendFields(
      allowSpend: AllowSpend,
      spendTransaction: SpendTransaction,
      currencyId: Address
    ): SpendActionValidationErrorOr[SpendTransaction] =
      if (allowSpend.currencyId =!= spendTransaction.currencyId)
        InvalidCurrency(
          s"Currency mismatch: expected ${allowSpend.currencyId}, found ${spendTransaction.currencyId}"
        ).invalidNec[SpendTransaction]
      else if (allowSpend.destination =!= currencyId)
        InvalidCurrencyId(
          s"Currency mismatch: expected $currencyId, found ${allowSpend.currencyId}"
        ).invalidNec[SpendTransaction]
      else if (!allowSpend.approvers.contains(currencyId))
        InvalidCurrencyId(
          s"Currency mismatch: expected $currencyId, found ${allowSpend.currencyId}"
        ).invalidNec[SpendTransaction]
      else if (allowSpend.destination =!= spendTransaction.destination)
        InvalidDestinationAddress(
          s"Invalid destination address. Found: ${spendTransaction.destination}. Expected: ${allowSpend.destination}"
        ).invalidNec[SpendTransaction]
      else if (allowSpend.source =!= spendTransaction.source)
        InvalidSourceAddress(
          s"Invalid source address. Found: ${spendTransaction.source}. Expected: ${allowSpend.source}"
        ).invalidNec[SpendTransaction]
      else if (allowSpend.amount.value.value < spendTransaction.amount.value.value)
        SpendAmountGreaterThanAllowed(
          s"Spend amount: ${spendTransaction.amount} greater than allowed: ${allowSpend.amount}"
        ).invalidNec[SpendTransaction]
      else
        spendTransaction.validNec[SpendActionValidationError]

    private def checkBalanceAndSource(
      spendTransaction: SpendTransaction,
      currencyIdBalance: Balance,
      currencyId: Address
    ): SpendActionValidationErrorOr[SpendTransaction] =
      if (spendTransaction.amount.value.value > currencyIdBalance.value.value)
        NotEnoughCurrencyIdBalance(
          s"Spend amount: ${spendTransaction.amount} greater than currencyId balance: $currencyIdBalance"
        ).invalidNec[SpendTransaction]
      else if (spendTransaction.source =!= currencyId)
        InvalidSourceAddress(
          s"Invalid source address. Found: ${spendTransaction.source}. Expected: $currencyId"
        ).invalidNec[SpendTransaction]
      else
        spendTransaction.validNec[SpendActionValidationError]

    // -----------------------------------------------------------------------------------------
    // Cross-shard decode helpers (Circe-based — the proven value bytes carry the canonical wire
    // shape used by every other gl0 read path; we round-trip through the standard decoders so a
    // tampered or wrong-type value surfaces as `CrossShardProofTampered`)
    // -----------------------------------------------------------------------------------------

    /** Decode the proven value bytes as `SortedSet[Signed[AllowSpend]]` — same shape as the leaf value of the in-process ActiveAllowSpends
      * map. Returns `Left(CrossShardProofTampered)` on any decode failure: a malicious peer that returns structurally-valid proof bytes for
      * a different type (or fabricated bytes) is caught here rather than silently mis-validated.
      */
    private def decodeCrossShardAllowSpends(
      valueBytes: Array[Byte]
    ): F[Either[SpendActionValidationError, SortedSet[Signed[AllowSpend]]]] = {
      val asString = new String(valueBytes, java.nio.charset.StandardCharsets.UTF_8)
      val parsed = parser
        .decode[SortedSet[Signed[AllowSpend]]](asString)
        .left
        .map(e => CrossShardProofTampered(s"could not decode cross-shard AllowSpends value: ${e.getMessage}"))
      (parsed: Either[SpendActionValidationError, SortedSet[Signed[AllowSpend]]]).pure[F]
    }

    /** Decode the proven value bytes as a [[Balance]]. Same rejection semantics as [[decodeCrossShardAllowSpends]].
      */
    private def decodeCrossShardBalance(
      valueBytes: Array[Byte]
    ): Either[SpendActionValidationError, Balance] = {
      val asString = new String(valueBytes, java.nio.charset.StandardCharsets.UTF_8)
      parser
        .decode[Balance](asString)
        .left
        .map(e => CrossShardProofTampered(s"could not decode cross-shard Balance value: ${e.getMessage}"))
    }

    /** Reference classification of a `SpendTransaction.currencyId` against the validator's current MG. Same-shard reads use the in-process
      * map; cross-shard reads route through the proof client.
      */
    private sealed trait Reference
    private case object Same extends Reference
    private case class Cross(
      targetMg: Address,
      targetShard: io.constellationnetwork.schema.sharding.ShardId
    ) extends Reference
  }

  @derive(eqv, show)
  sealed trait SpendActionValidationError
  case class NoActiveAllowSpends(error: String) extends SpendActionValidationError
  case class InvalidDestinationAddress(error: String) extends SpendActionValidationError
  case class InvalidSourceAddress(error: String) extends SpendActionValidationError
  case class AllowSpendNotFound(error: String) extends SpendActionValidationError
  case class InvalidCurrency(error: String) extends SpendActionValidationError
  case class SpendAmountGreaterThanAllowed(error: String) extends SpendActionValidationError
  case class NotEnoughCurrencyIdBalance(error: String) extends SpendActionValidationError
  case class InvalidCurrencyId(error: String) extends SpendActionValidationError
  case class DuplicatedAllowSpendReference(error: String) extends SpendActionValidationError

  /** Slice 11 — cross-shard proof could not be fetched (target shard offline, network failure, peer doesn't own the requested MG, etc.).
    * The SpendAction is rejected this round; the gl0 leader retries on the next ord when the proof becomes available.
    */
  case class CrossShardProofUnavailable(error: String) extends SpendActionValidationError

  /** Slice 11 — cross-shard proof's value bytes failed to decode as the expected on-chain type (`SortedSet[Signed[AllowSpend]]` for
    * ActiveAllowSpends; `Balance` for Balances). Defence-in-depth against a peer that returns a structurally-valid proof for a different
    * type or fabricated bytes.
    */
  case class CrossShardProofTampered(error: String) extends SpendActionValidationError

  type SpendActionValidationErrorOr[A] = ValidatedNec[SpendActionValidationError, A]
}
