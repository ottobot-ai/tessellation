package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedTokenLockSetCodec

import scodec.bits.ByteVector

/** Strict field-8 reader for native/global token locks.
  *
  * Field 8 is keyed by the lock source and accepts only native locks (`currencyId == None`). Prefix scans retain and validate the physical
  * key; point reads validate the same value grammar and never collapse malformed committed bytes into absence.
  */
object ActiveTokenLockMptReader {

  type ActiveTokenLocks = SortedMap[Address, SortedSet[Signed[TokenLock]]]

  private val context = "materialize native ActiveTokenLocks"

  private def malformed[F[_]: Async, A](physicalKey: Hex, reason: String): F[A] =
    Async[F].raiseError(StrictMptRead.MalformedConsensusMptValue(context, physicalKey, reason))

  private def inconsistent[F[_]: Async, A](physicalKey: Hex, reason: String): F[A] =
    Async[F].raiseError(StrictMptRead.InconsistentConsensusMptIndex(context, physicalKey, reason))

  private def validateValue[F[_]: Async: Hasher](
    physicalKey: Hex,
    locks: SortedSet[Signed[TokenLock]],
    rawBytes: ByteVector
  ): F[(Address, SortedSet[Signed[TokenLock]])] = {
    val canonicalBytes = signedTokenLockSetCodec.immutableBytes(locks)

    if (rawBytes != canonicalBytes)
      malformed(physicalKey, "non-canonical value encoding")
    else if (locks.isEmpty)
      inconsistent(physicalKey, "empty token-lock set")
    else {
      val sources = locks.iterator.map(_.value.source).toSet

      if (sources.size =!= 1)
        inconsistent(physicalKey, "mixed token-lock sources")
      else if (locks.exists(_.value.currencyId.nonEmpty))
        inconsistent(physicalKey, "field 8 accepts only native token locks with currencyId=None")
      else
        locks.toList.traverse(TokenLockReference.of[F]).flatMap { references =>
          val identities = references.map(_.hash)

          if (identities.distinct.size =!= identities.size)
            inconsistent(physicalKey, "duplicate unsigned token-lock identity")
          else (sources.head, locks).pure[F]
        }
    }
  }

  private def requireCanonicalKey[F[_]: Async: Hasher](
    physicalKey: Hex,
    source: Address
  ): F[Unit] =
    GlobalStateKey.toHex[F](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source)).flatMap { expectedKey =>
      if (physicalKey === expectedKey) ().pure[F]
      else inconsistent(physicalKey, s"key/value mismatch expected=${expectedKey.value} actual=${physicalKey.value}")
    }

  def readNative[F[_]: Async: Hasher](
    reader: GlobalStateReader[F],
    address: Address
  ): F[Option[SortedSet[Signed[TokenLock]]]] = {
    val key = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, address)

    for {
      physicalKey <- GlobalStateKey.toHex[F](key)
      read <- reader.getStrict[SortedSet[Signed[TokenLock]]](key)
      result <- read match {
        case StrictMptRead.Absent => none[SortedSet[Signed[TokenLock]]].pure[F]
        case StrictMptRead.Malformed(reason, _) => malformed[F, Option[SortedSet[Signed[TokenLock]]]](physicalKey, reason)
        case StrictMptRead.Present(locks, rawBytes) =>
          validateValue[F](physicalKey, locks, rawBytes).flatMap {
            case (source, validated) =>
              requireCanonicalKey[F](physicalKey, source) >>
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

  def materializeNative[F[_]: Async: Hasher](reader: GlobalStateReader[F]): F[ActiveTokenLocks] =
    for {
      prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveTokenLocks)
      entries <- reader.getAllForPrefixStrict[SortedSet[Signed[TokenLock]]](prefix)
      validated <- entries
        .sortBy(_.physicalKey.value)
        .foldLeftM((SortedSet.empty[Address], List.empty[(Address, SortedSet[Signed[TokenLock]])])) {
          case (_, StrictMptEntry(physicalKey, StrictMptRead.Absent)) =>
            Async[F].raiseError[(SortedSet[Address], List[(Address, SortedSet[Signed[TokenLock]])])](
              StrictMptRead.MissingConsensusMptValue(context, physicalKey)
            )

          case (_, StrictMptEntry(physicalKey, StrictMptRead.Malformed(reason, _))) =>
            malformed[F, (SortedSet[Address], List[(Address, SortedSet[Signed[TokenLock]])])](physicalKey, reason)

          case ((seen, acc), StrictMptEntry(physicalKey, StrictMptRead.Present(locks, rawBytes))) =>
            validateValue[F](physicalKey, locks, rawBytes).flatMap {
              case (source, _) if seen.contains(source) =>
                inconsistent[F, (SortedSet[Address], List[(Address, SortedSet[Signed[TokenLock]])])](
                  physicalKey,
                  s"duplicate logical token-lock source=$source"
                )
              case (source, validatedLocks) =>
                requireCanonicalKey[F](physicalKey, source).as((seen + source, (source -> validatedLocks) :: acc))
            }
        }
    } yield SortedMap.from(validated._2.reverse)
}
