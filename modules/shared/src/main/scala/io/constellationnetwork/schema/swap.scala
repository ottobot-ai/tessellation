package io.constellationnetwork.schema

import cats.Order._
import cats.Show
import cats.data.NonEmptySet
import cats.effect.kernel.{Async, Sync}
import cats.syntax.functor._
import cats.syntax.semigroup._

import scala.util.Try

import io.constellationnetwork.ext.cats.data.OrderBasedOrdering
import io.constellationnetwork.ext.codecs._
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.ext.derevo.ordering
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}

import derevo.cats.{eqv, order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import enumeratum._
import eu.timepit.refined.auto.{autoRefineV, _}
import eu.timepit.refined.cats._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import io.circe.{Decoder, Encoder}
import io.estatico.newtype.macros.newtype

object swap {
  @derive(decoder, encoder, order, show)
  @newtype
  case class SwapAmount(value: PosLong)

  object SwapAmount {
    implicit def toAmount(amount: SwapAmount): Amount = Amount(amount.value)
  }

  @derive(decoder, encoder, order, show)
  @newtype
  case class AllowSpendFee(value: NonNegLong)

  object AllowSpendFee {
    implicit def toAmount(fee: AllowSpendFee): Amount = Amount(fee.value)
  }

  @derive(decoder, encoder, order, show, ordering)
  @newtype
  case class CurrencyId(value: Address)

  @derive(decoder, encoder, show, order, ordering)
  @newtype
  case class AllowSpendOrdinal(value: NonNegLong) {
    def next: AllowSpendOrdinal = AllowSpendOrdinal(value |+| 1L)
  }

  object AllowSpendOrdinal {
    val first: AllowSpendOrdinal = AllowSpendOrdinal(1L)
  }

  @derive(decoder, encoder, order, show)
  case class AllowSpendReference(ordinal: AllowSpendOrdinal, hash: Hash)

  object AllowSpendReference {
    def of(hashedTransaction: Hashed[AllowSpend]): AllowSpendReference =
      AllowSpendReference(hashedTransaction.ordinal, hashedTransaction.hash)

    def of[F[_]: Async: Hasher](signedTransaction: Signed[AllowSpend]): F[AllowSpendReference] =
      signedTransaction.value.hash.map(AllowSpendReference(signedTransaction.ordinal, _))

    val empty: AllowSpendReference = AllowSpendReference(AllowSpendOrdinal(0L), Hash.empty)

    def emptyCurrency[F[_]: Sync: Hasher](currencyAddress: Address): F[AllowSpendReference] =
      currencyAddress.value.value.hash.map(emptyCurrency(_))

    def emptyCurrency(currencyIdentifier: Hash): AllowSpendReference =
      AllowSpendReference(AllowSpendOrdinal(0L), currencyIdentifier)

  }

  @derive(decoder, encoder, order, show)
  case class AllowSpend(
    source: Address,
    destination: Address,
    currencyId: Option[CurrencyId],
    amount: SwapAmount,
    fee: AllowSpendFee,
    parent: AllowSpendReference,
    lastValidEpochProgress: EpochProgress,
    approvers: List[Address]
  ) {
    val ordinal: AllowSpendOrdinal = parent.ordinal.next
  }

  object AllowSpend {
    implicit object OrderingInstance extends OrderBasedOrdering[AllowSpend]
  }

  /** Value record stored in the cross-shard single-use SPENT-SET (`GlobalStateFieldId.ConsumedAllowSpends`, fieldId 33), keyed by the
    * allow-spend's content `Hash` (`GlobalStateKey.consumedAllowSpendKey`). Records that a specific allow-spend has ALREADY been consumed
    * by a CROSS-SHARD spend (a spend processed in metagraph M′ that consumed an allow-spend living on a different shard's metagraph M).
    *
    * '''Why the record is self-contained (extends the scaladoc's "minimal consuming reference").''' The gl0 reservation-adjustment fold
    * (`AllowSpendStateManager`/the cross-shard settlement) corrects the source's adopted balance from the spent-set ALONE — by the time the
    * fold runs, the allow-spend may already be gone from M's active set (M autonomously expired it and refunded the source). The fold must
    * therefore reconstruct everything it needs from this record without any other lookup:
    *   - `source` + `amount`: the reservation to re-subtract once M refunds it on expiry (the permanent-debit overlay).
    *   - `destination` + `amount`: the cross-shard credit re-applied every ordinal (neither M nor M′ re-pushes it — it is a pure gl0
    *     protocol-primitive overlay), so the destination is credited `+amount` for as long as the marker lives.
    *   - `currencyId`: the OWNER metagraph M whose pinned `globalSyncView.epochProgress` the expiry test reads (consistent with the R1
    *     fix); the fold subtracts `amount` from the source iff `currentEpoch(M) > lastValidEpochProgress`.
    *   - `lastValidEpochProgress`: the allow-spend's expiry — the boundary at which M refunds and the fold begins re-subtracting.
    *   - `consumedAtOrdinal` + `consumingSpendRef`: audit trail (which gl0 snapshot folded it, which spend consumed it).
    *
    * Append-only in spirit (a consumed hash never un-consumes). Membership alone is the single-use (I-ONCE) evidence checked by the
    * absence-gate; the carried fields drive the balance fold and auditability.
    */
  @derive(decoder, encoder, order, show)
  case class ConsumedAllowSpend(
    allowSpendHash: Hash,
    source: Address,
    destination: Address,
    currencyId: Option[CurrencyId],
    amount: SwapAmount,
    lastValidEpochProgress: EpochProgress,
    consumedAtOrdinal: SnapshotOrdinal,
    consumingSpendRef: Hash
  )

  object ConsumedAllowSpend {
    implicit object OrderingInstance extends OrderBasedOrdering[ConsumedAllowSpend]
  }

  @derive(encoder)
  case class AllowSpendView(
    transaction: AllowSpend,
    hash: Hash,
    status: AllowSpendStatus
  )

  @derive(eqv, show)
  sealed trait AllowSpendStatus extends EnumEntry

  object AllowSpendStatus extends Enum[AllowSpendStatus] with AllowSpendStatusCodecs {
    val values = findValues

    case object Waiting extends AllowSpendStatus
  }

  trait AllowSpendStatusCodecs {
    implicit val encode: Encoder[AllowSpendStatus] = Encoder.encodeString.contramap[AllowSpendStatus](_.entryName)
    implicit val decode: Decoder[AllowSpendStatus] =
      Decoder.decodeString.emapTry(s => Try(AllowSpendStatus.withName(s)))
  }

  @derive(decoder, encoder, show, order)
  case class AllowSpendBlock(
    roundId: RoundId,
    transactions: NonEmptySet[Signed[AllowSpend]]
  )

  object AllowSpendBlock {
    implicit val roundIdShow: Show[RoundId] = RoundId.shortShow
    implicit val transactionsDecoder: Decoder[NonEmptySet[Signed[AllowSpend]]] =
      NonEmptySetCodec.decoder[Signed[AllowSpend]]

    implicit object OrderingInstance extends OrderBasedOrdering[AllowSpendBlock]
  }
}
