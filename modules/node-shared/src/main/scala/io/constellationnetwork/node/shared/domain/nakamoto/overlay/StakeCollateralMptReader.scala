package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._

import scodec.bits.ByteVector

/** Strict readers for the four global stake/collateral partitions (fields 13-16).
  *
  * Every value carries the source address needed to reproduce its lossy physical MPT key. Prefix scans retain that physical key and the
  * exact committed bytes, reject non-canonical encodings and structurally invalid sets, then require the reproduced global empty-contract
  * key to match exactly. Point reads enforce the same grammar and preserve malformed committed bytes as failures rather than absence.
  */
object StakeCollateralMptReader {

  type ActiveDelegatedStakes = SortedMap[Address, SortedSet[DelegatedStakeRecord]]
  type DelegatedStakeWithdrawals = SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]]
  type ActiveNodeCollaterals = SortedMap[Address, SortedSet[NodeCollateralRecord]]
  type NodeCollateralWithdrawals = SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]

  private def malformed[F[_]: Async, A](context: String, physicalKey: Hex, reason: String): F[A] =
    Async[F].raiseError(StrictMptRead.MalformedConsensusMptValue(context, physicalKey, reason))

  private def inconsistent[F[_]: Async, A](context: String, physicalKey: Hex, reason: String): F[A] =
    Async[F].raiseError(StrictMptRead.InconsistentConsensusMptIndex(context, physicalKey, reason))

  private def validateValue[F[_]: Async, A](
    context: String,
    physicalKey: Hex,
    records: SortedSet[A],
    rawBytes: ByteVector,
    sourceOf: A => Address,
    identityOf: A => Any
  )(implicit codec: ImmutableCodec[SortedSet[A]]): F[(Address, SortedSet[A])] = {
    val canonicalBytes = codec.immutableBytes(records)

    if (rawBytes != canonicalBytes)
      malformed(context, physicalKey, "non-canonical value encoding")
    else if (records.isEmpty)
      inconsistent(context, physicalKey, "empty record set")
    else {
      val sources = records.iterator.map(sourceOf).toSet

      if (sources.size =!= 1)
        inconsistent(context, physicalKey, "mixed embedded create sources")
      else {
        val identities = records.iterator.map(identityOf).toList
        if (identities.distinct.size =!= identities.size)
          inconsistent(context, physicalKey, "duplicate unsigned create identity")
        else (sources.head, records).pure[F]
      }
    }
  }

  private def requireCanonicalKey[F[_]: Async: Hasher](
    context: String,
    fieldId: GlobalStateFieldId,
    physicalKey: Hex,
    source: Address
  ): F[Unit] =
    GlobalStateKey.toHex[F](GlobalStateKey.hypergraph(fieldId, source)).flatMap { expectedKey =>
      if (physicalKey === expectedKey) ().pure[F]
      else inconsistent(context, physicalKey, s"key/value mismatch expected=${expectedKey.value} actual=${physicalKey.value}")
    }

  private def read[F[_]: Async: Hasher, A](
    reader: GlobalStateReader[F],
    fieldId: GlobalStateFieldId,
    context: String,
    address: Address,
    sourceOf: A => Address,
    identityOf: A => Any
  )(implicit codec: ImmutableCodec[SortedSet[A]]): F[Option[SortedSet[A]]] = {
    val key = GlobalStateKey.hypergraph(fieldId, address)

    for {
      physicalKey <- GlobalStateKey.toHex[F](key)
      strictRead <- reader.getStrict[SortedSet[A]](key)
      result <- strictRead match {
        case StrictMptRead.Absent               => none[SortedSet[A]].pure[F]
        case StrictMptRead.Malformed(reason, _) => malformed[F, Option[SortedSet[A]]](context, physicalKey, reason)
        case StrictMptRead.Present(records, rawBytes) =>
          validateValue(context, physicalKey, records, rawBytes, sourceOf, identityOf).flatMap {
            case (source, validated) =>
              requireCanonicalKey(context, fieldId, physicalKey, source) >>
                Async[F].raiseWhen(source =!= address)(
                  StrictMptRead.InconsistentConsensusMptIndex(
                    context,
                    physicalKey,
                    s"point-read source mismatch expected=$address actual=$source"
                  )
                ) >> validated.some.pure[F]
          }
      }
    } yield result
  }

  private def materializeEntries[F[_]: Async: Hasher, A](
    entries: List[StrictMptEntry[SortedSet[A]]],
    fieldId: GlobalStateFieldId,
    context: String,
    sourceOf: A => Address,
    identityOf: A => Any
  )(implicit codec: ImmutableCodec[SortedSet[A]]): F[SortedMap[Address, SortedSet[A]]] =
    entries
      .sortBy(_.physicalKey.value)
      .foldLeftM((SortedSet.empty[Address], List.empty[(Address, SortedSet[A])])) {
        case (_, StrictMptEntry(physicalKey, StrictMptRead.Absent)) =>
          Async[F].raiseError[(SortedSet[Address], List[(Address, SortedSet[A])])](
            StrictMptRead.MissingConsensusMptValue(context, physicalKey)
          )

        case (_, StrictMptEntry(physicalKey, StrictMptRead.Malformed(reason, _))) =>
          malformed[F, (SortedSet[Address], List[(Address, SortedSet[A])])](context, physicalKey, reason)

        case ((seen, acc), StrictMptEntry(physicalKey, StrictMptRead.Present(records, rawBytes))) =>
          validateValue(context, physicalKey, records, rawBytes, sourceOf, identityOf).flatMap {
            case (source, _) if seen.contains(source) =>
              inconsistent[F, (SortedSet[Address], List[(Address, SortedSet[A])])](
                context,
                physicalKey,
                s"duplicate logical source=$source"
              )
            case (source, validatedRecords) =>
              requireCanonicalKey(context, fieldId, physicalKey, source)
                .as((seen + source, (source -> validatedRecords) :: acc))
          }
      }
      .map(validated => SortedMap.from(validated._2.reverse))

  private def materialize[F[_]: Async: Hasher, A](
    reader: GlobalStateReader[F],
    fieldId: GlobalStateFieldId,
    context: String,
    sourceOf: A => Address,
    identityOf: A => Any
  )(implicit codec: ImmutableCodec[SortedSet[A]]): F[SortedMap[Address, SortedSet[A]]] =
    for {
      prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](fieldId)
      entries <- reader.getAllForPrefixStrict[SortedSet[A]](prefix)
      materialized <- materializeEntries(entries, fieldId, context, sourceOf, identityOf)
    } yield materialized

  private def materializeRawEntries[F[_]: Async: Hasher, A](
    entries: List[StrictMptRawEntry],
    fieldId: GlobalStateFieldId,
    context: String,
    sourceOf: A => Address,
    identityOf: A => Any
  )(implicit codec: ImmutableCodec[SortedSet[A]]): F[SortedMap[Address, SortedSet[A]]] = {
    val decodedEntries = entries.map {
      case StrictMptRawEntry(physicalKey, rawBytes) =>
        val storedBytes = rawBytes.fold[Array[Byte]](null)(_.toArray)
        StrictMptEntry(physicalKey, StrictMptRead.fromStoredBytes[SortedSet[A]](storedBytes))
    }

    materializeEntries(decodedEntries, fieldId, context, sourceOf, identityOf)
  }

  def readActiveDelegatedStakes[F[_]: Async: Hasher](
    reader: GlobalStateReader[F],
    address: Address
  ): F[Option[SortedSet[DelegatedStakeRecord]]] =
    read(
      reader,
      GlobalStateFieldId.ActiveDelegatedStakes,
      "read ActiveDelegatedStakes",
      address,
      _.event.value.source,
      _.event.value
    )

  def materializeActiveDelegatedStakes[F[_]: Async: Hasher](reader: GlobalStateReader[F]): F[ActiveDelegatedStakes] =
    materialize(
      reader,
      GlobalStateFieldId.ActiveDelegatedStakes,
      "materialize ActiveDelegatedStakes",
      _.event.value.source,
      _.event.value
    )

  def materializeActiveDelegatedStakesFromRaw[F[_]: Async: Hasher](
    entries: List[StrictMptRawEntry]
  ): F[ActiveDelegatedStakes] =
    materializeRawEntries(
      entries,
      GlobalStateFieldId.ActiveDelegatedStakes,
      "materialize ActiveDelegatedStakes",
      _.event.value.source,
      _.event.value
    )

  def readDelegatedStakeWithdrawals[F[_]: Async: Hasher](
    reader: GlobalStateReader[F],
    address: Address
  ): F[Option[SortedSet[PendingDelegatedStakeWithdrawal]]] =
    read(
      reader,
      GlobalStateFieldId.DelegatedStakesWithdrawals,
      "read DelegatedStakesWithdrawals",
      address,
      _.event.value.source,
      _.event.value
    )

  def materializeDelegatedStakeWithdrawals[F[_]: Async: Hasher](reader: GlobalStateReader[F]): F[DelegatedStakeWithdrawals] =
    materialize(
      reader,
      GlobalStateFieldId.DelegatedStakesWithdrawals,
      "materialize DelegatedStakesWithdrawals",
      _.event.value.source,
      _.event.value
    )

  def materializeDelegatedStakeWithdrawalsFromRaw[F[_]: Async: Hasher](
    entries: List[StrictMptRawEntry]
  ): F[DelegatedStakeWithdrawals] =
    materializeRawEntries(
      entries,
      GlobalStateFieldId.DelegatedStakesWithdrawals,
      "materialize DelegatedStakesWithdrawals",
      _.event.value.source,
      _.event.value
    )

  def readActiveNodeCollaterals[F[_]: Async: Hasher](
    reader: GlobalStateReader[F],
    address: Address
  ): F[Option[SortedSet[NodeCollateralRecord]]] =
    read(
      reader,
      GlobalStateFieldId.ActiveNodeCollaterals,
      "read ActiveNodeCollaterals",
      address,
      _.event.value.source,
      _.event.value
    )

  def materializeActiveNodeCollaterals[F[_]: Async: Hasher](reader: GlobalStateReader[F]): F[ActiveNodeCollaterals] =
    materialize(
      reader,
      GlobalStateFieldId.ActiveNodeCollaterals,
      "materialize ActiveNodeCollaterals",
      _.event.value.source,
      _.event.value
    )

  def materializeActiveNodeCollateralsFromRaw[F[_]: Async: Hasher](
    entries: List[StrictMptRawEntry]
  ): F[ActiveNodeCollaterals] =
    materializeRawEntries(
      entries,
      GlobalStateFieldId.ActiveNodeCollaterals,
      "materialize ActiveNodeCollaterals",
      _.event.value.source,
      _.event.value
    )

  def readNodeCollateralWithdrawals[F[_]: Async: Hasher](
    reader: GlobalStateReader[F],
    address: Address
  ): F[Option[SortedSet[PendingNodeCollateralWithdrawal]]] =
    read(
      reader,
      GlobalStateFieldId.NodeCollateralWithdrawals,
      "read NodeCollateralWithdrawals",
      address,
      _.event.value.source,
      _.event.value
    )

  def materializeNodeCollateralWithdrawals[F[_]: Async: Hasher](reader: GlobalStateReader[F]): F[NodeCollateralWithdrawals] =
    materialize(
      reader,
      GlobalStateFieldId.NodeCollateralWithdrawals,
      "materialize NodeCollateralWithdrawals",
      _.event.value.source,
      _.event.value
    )

  def materializeNodeCollateralWithdrawalsFromRaw[F[_]: Async: Hasher](
    entries: List[StrictMptRawEntry]
  ): F[NodeCollateralWithdrawals] =
    materializeRawEntries(
      entries,
      GlobalStateFieldId.NodeCollateralWithdrawals,
      "materialize NodeCollateralWithdrawals",
      _.event.value.source,
      _.event.value
    )
}
