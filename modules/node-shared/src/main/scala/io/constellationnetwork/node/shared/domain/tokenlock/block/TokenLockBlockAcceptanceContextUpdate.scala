package io.constellationnetwork.node.shared.domain.tokenlock.block

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.derive

@derive(eqv, show)
case class TokenLockBlockAcceptanceContextUpdate(
  balances: Map[Address, Balance],
  lastTokenLocksRefs: Map[Address, TokenLockReference],
  claimedReplacementRefs: Set[Hash],
  inRoundTokenLocksByHash: Map[Hash, Hashed[TokenLock]]
)

object TokenLockBlockAcceptanceContextUpdate {

  val empty: TokenLockBlockAcceptanceContextUpdate = TokenLockBlockAcceptanceContextUpdate(
    Map.empty,
    Map.empty,
    Set.empty,
    Map.empty
  )
}
