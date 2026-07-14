package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter
import io.constellationnetwork.schema.swap.AllowSpend
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed

private[global] object ActiveAllowSpendMptMaterializer {

  type ActiveAllowSpends = SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]

  def materialize[F[_]: Async: Hasher](entries: Map[Hex, SortedSet[Signed[AllowSpend]]]): F[ActiveAllowSpends] =
    GlobalStateConverter.materializeActiveAllowSpendEntries[F](entries)
}
