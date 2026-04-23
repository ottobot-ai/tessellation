package io.constellationnetwork.schema.mpt

import cats.{Order, Show}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, order}
import derevo.derive

/** A reference to one `Signed[AllowSpend]` inside `activeAllowSpends`: `(metagraphId, source, hash)`. */
@derive(eqv, order)
case class AllowSpendExpiryKey(metagraphId: Option[Address], address: Address, hash: Hash)

object AllowSpendExpiryKey {
  implicit val show: Show[AllowSpendExpiryKey] =
    Show.show(k => s"(${k.metagraphId.map(_.value.value).getOrElse("-")}, ${k.address.value.value}, ${k.hash.value.take(12)})")
  implicit lazy val ordering: Ordering[AllowSpendExpiryKey] = Order[AllowSpendExpiryKey].toOrdering
}

/** A reference to one `Signed[TokenLock]` inside `activeTokenLocks`: `(source, hash)`. */
@derive(eqv, order)
case class TokenLockExpiryKey(address: Address, hash: Hash)

object TokenLockExpiryKey {
  implicit val show: Show[TokenLockExpiryKey] = Show.show(k => s"(${k.address.value.value}, ${k.hash.value.take(12)})")
  implicit lazy val ordering: Ordering[TokenLockExpiryKey] = Order[TokenLockExpiryKey].toOrdering
}

/** A reference to one `PendingNodeCollateralWithdrawal` inside `nodeCollateralWithdrawals`: `(source, hash)`. */
@derive(eqv, order)
case class NodeCollateralWithdrawalExpiryKey(address: Address, hash: Hash)

object NodeCollateralWithdrawalExpiryKey {
  implicit val show: Show[NodeCollateralWithdrawalExpiryKey] =
    Show.show(k => s"(${k.address.value.value}, ${k.hash.value.take(12)})")
  implicit lazy val ordering: Ordering[NodeCollateralWithdrawalExpiryKey] = Order[NodeCollateralWithdrawalExpiryKey].toOrdering
}
