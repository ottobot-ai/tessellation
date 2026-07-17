package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.CurrencyInfoReader
import io.constellationnetwork.schema.mpt.PartitionNamespace.{AddressNamespace, MetagraphNamespace}
import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference, CurrencyId}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import scodec.bits.ByteVector
import weaver.MutableIOSuite

/** Consensus corruption tests for the address-keyed unrolled CurrencySnapshotInfo fields 25 through 30. */
object MgAddressFieldStrictReconstructionSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
    } yield (hasher, sp, json)

  private val proofs = NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private def tokenLock(
    source: Address,
    currencyId: Option[CurrencyId],
    label: String
  ): Signed[TokenLock] =
    Signed(
      TokenLock(
        source = source,
        amount = TokenLockAmount(PosLong(200L)),
        fee = TokenLockFee(NonNegLong(0L)),
        parent = TokenLockReference(TokenLockOrdinal(NonNegLong(0L)), testHash(s"token-lock-$label")),
        currencyId = currencyId,
        unlockEpoch = EpochProgress(NonNegLong(700L)).some,
        replaceTokenLockRef = none
      ),
      proofs
    )

  private def encoded[V: ImmutableCodec](value: V): Array[Byte] =
    ImmutableCodec[V].immutableBytes(value).toArray

  /** `forcedEntries` permits explicit Absent/Malformed/Present fixtures that cannot arise from ordinary byte decoding. */
  private def reader(
    rawEntries: List[(Hex, Array[Byte])],
    forcedEntries: List[(Hex, StrictMptRead[Any])]
  ): CurrencyInfoReader[IO] =
    new CurrencyInfoReader[IO] {
      def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] =
        Map.empty[Hex, V].pure[IO]

      def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] = {
        val raw = rawEntries.collect {
          case (physicalKey, bytes) if physicalKey.value.startsWith(prefix.value) =>
            StrictMptEntry(physicalKey, StrictMptRead.fromStoredBytes[V](bytes))
        }
        val forced = forcedEntries.collect {
          case (physicalKey, read) if physicalKey.value.startsWith(prefix.value) =>
            StrictMptEntry(physicalKey, read.asInstanceOf[StrictMptRead[V]])
        }

        (raw ++ forced).pure[IO]
      }
    }

  private def canonicalHex(
    metagraph: Address,
    field: GlobalStateFieldId,
    holder: Address
  )(implicit hasher: Hasher[IO]): IO[Hex] =
    GlobalStateKey.toHex[IO](GlobalStateKey.metagraphEntry(metagraph, field, holder))

  private def reconstruct(
    metagraph: Address,
    rawEntries: List[(Hex, Array[Byte])] = Nil,
    forcedEntries: List[(Hex, StrictMptRead[Any])] = Nil
  )(implicit hasher: Hasher[IO]) =
    GlobalStateConverter.reconstructCurrencyInfoFrom[IO](metagraph, reader(rawEntries, forcedEntries))

  private def addressFieldValues(
    metagraph: Address,
    holder: Address
  ): List[(GlobalStateFieldId, Array[Byte])] = {
    import GlobalStateFieldId._

    List(
      MgBalances -> encoded(holder -> Balance(NonNegLong(555L))),
      MgLastTxRefs -> encoded(
        holder -> TransactionReference(TransactionOrdinal(NonNegLong(1L)), testHash("last-tx"))
      ),
      MgLastFeeTxRefs -> encoded(
        holder -> TransactionReference(TransactionOrdinal(NonNegLong(2L)), testHash("last-fee-tx"))
      ),
      MgLastAllowSpendRefs -> encoded(
        holder -> AllowSpendReference(AllowSpendOrdinal(NonNegLong(3L)), testHash("last-allow-spend"))
      ),
      MgLastTokenLockRefs -> encoded(
        holder -> TokenLockReference(TokenLockOrdinal(NonNegLong(4L)), testHash("last-token-lock"))
      ),
      MgActiveTokenLocks -> encoded(
        holder -> SortedSet(tokenLock(holder, CurrencyId(metagraph).some, "active"))
      )
    )
  }

  test("fields 25-30 reconstruct canonical address-keyed entries") { res =>
    implicit val (hasher, securityProvider, json) = res

    for {
      metagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      values = addressFieldValues(metagraph, holder)
      entries <- values.traverse { case (field, bytes) => canonicalHex(metagraph, field, holder).map(_ -> bytes) }
      reconstructed <- reconstruct(metagraph, entries.reverse)
      expectedLock = tokenLock(holder, CurrencyId(metagraph).some, "active")
    } yield
      expect.all(
        reconstructed.balances == SortedMap(holder -> Balance(NonNegLong(555L))),
        reconstructed.lastTxRefs == SortedMap(
          holder -> TransactionReference(TransactionOrdinal(NonNegLong(1L)), testHash("last-tx"))
        ),
        reconstructed.lastFeeTxRefs.contains(
          SortedMap(holder -> TransactionReference(TransactionOrdinal(NonNegLong(2L)), testHash("last-fee-tx")))
        ),
        reconstructed.lastAllowSpendRefs.contains(
          SortedMap(holder -> AllowSpendReference(AllowSpendOrdinal(NonNegLong(3L)), testHash("last-allow-spend")))
        ),
        reconstructed.lastTokenLockRefs.contains(
          SortedMap(holder -> TokenLockReference(TokenLockOrdinal(NonNegLong(4L)), testHash("last-token-lock")))
        ),
        reconstructed.activeTokenLocks.contains(SortedMap(holder -> SortedSet(expectedLock)))
      )
  }

  test("fields 25-30 reject an A physical key carrying a B logical value") { res =>
    implicit val (hasher, securityProvider, json) = res

    for {
      metagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      keyHolder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      valueHolder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      fixtures = addressFieldValues(metagraph, valueHolder)
      attempts <- fixtures.traverse {
        case (field, bytes) =>
          canonicalHex(metagraph, field, keyHolder).flatMap { physicalKey =>
            reconstruct(metagraph, List(physicalKey -> bytes)).attempt
          }
      }
    } yield
      expect(
        attempts.forall {
          case Left(error: StrictMptRead.InconsistentConsensusMptIndex) =>
            error.getMessage.contains("key/value mismatch")
          case _ => false
        }
      )
  }

  test("fields 25-30 reject wrong-contract and suffix placement") { res =>
    implicit val (hasher, securityProvider, json) = res

    for {
      metagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      wrongContract <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      fixtures = addressFieldValues(metagraph, holder)
      attempts <- fixtures.traverse {
        case (field, bytes) =>
          for {
            canonical <- canonicalHex(metagraph, field, holder)
            wrongContractKey <- GlobalStateKey.toHex[IO](
              GlobalStateKey(
                MetagraphNamespace(metagraph),
                field,
                AddressNamespace(wrongContract),
                AddressNamespace(holder)
              )
            )
            wrongContractResult <- reconstruct(metagraph, List(wrongContractKey -> bytes)).attempt
            suffixResult <- reconstruct(metagraph, List(Hex(canonical.value + "00") -> bytes)).attempt
          } yield List(wrongContractResult, suffixResult)
      }
    } yield
      expect(
        attempts.flatten.forall(_.left.exists {
          case error: StrictMptRead.InconsistentConsensusMptIndex => error.getMessage.contains("key/value mismatch")
          case _                                                  => false
        })
      )
  }

  test("strict address-field reconstruction distinguishes absent, malformed, and noncanonical values") { res =>
    implicit val (hasher, securityProvider, json) = res
    import GlobalStateFieldId.MgBalances

    for {
      metagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      physicalKey <- canonicalHex(metagraph, MgBalances, holder)
      value = holder -> Balance(NonNegLong(555L))
      absent <- reconstruct(metagraph, forcedEntries = List(physicalKey -> StrictMptRead.Absent)).attempt
      malformed <- reconstruct(
        metagraph,
        forcedEntries = List(physicalKey -> StrictMptRead.Malformed("fixture malformed", ByteVector(0x7f).some))
      ).attempt
      noncanonical <- reconstruct(
        metagraph,
        forcedEntries = List(physicalKey -> StrictMptRead.Present(value, ByteVector(0x00)))
      ).attempt
    } yield
      expect.all(
        absent.left.exists(_.isInstanceOf[StrictMptRead.MissingConsensusMptValue]),
        malformed.left.exists {
          case error: StrictMptRead.MalformedConsensusMptValue => error.getMessage.contains("fixture malformed")
          case _                                               => false
        },
        noncanonical.left.exists {
          case error: StrictMptRead.MalformedConsensusMptValue => error.getMessage.contains("non-canonical value encoding")
          case _                                               => false
        }
      )
  }

  test("duplicate logical address keys fail deterministically in physical-key order") { res =>
    implicit val (hasher, securityProvider, json) = res
    import GlobalStateFieldId.MgBalances

    for {
      metagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      canonical <- canonicalHex(metagraph, MgBalances, holder)
      suffix = Hex(canonical.value + "00")
      bytes = encoded(holder -> Balance(NonNegLong(555L)))
      forward <- reconstruct(metagraph, List(canonical -> bytes, suffix -> bytes)).attempt
      reverse <- reconstruct(metagraph, List(suffix -> bytes, canonical -> bytes)).attempt
      forwardMessage = forward.left.toOption.map(_.getMessage)
      reverseMessage = reverse.left.toOption.map(_.getMessage)
    } yield
      expect.all(
        forward.left.exists(_.isInstanceOf[StrictMptRead.InconsistentConsensusMptIndex]),
        forwardMessage.exists(_.contains("duplicate logical key")),
        forwardMessage == reverseMessage
      )
  }

  test("field 30 enforces nonempty source and owning-metagraph currency scope") { res =>
    implicit val (hasher, securityProvider, json) = res
    import GlobalStateFieldId.MgActiveTokenLocks

    for {
      metagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      otherHolder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      otherMetagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      physicalKey <- canonicalHex(metagraph, MgActiveTokenLocks, holder)
      empty <- reconstruct(
        metagraph,
        List(physicalKey -> encoded(holder -> SortedSet.empty[Signed[TokenLock]]))
      ).attempt
      wrongSource <- reconstruct(
        metagraph,
        List(
          physicalKey -> encoded(
            holder -> SortedSet(tokenLock(otherHolder, CurrencyId(metagraph).some, "wrong-source"))
          )
        )
      ).attempt
      missingCurrency <- reconstruct(
        metagraph,
        List(physicalKey -> encoded(holder -> SortedSet(tokenLock(holder, none, "missing-currency"))))
      ).attempt
      wrongCurrency <- reconstruct(
        metagraph,
        List(
          physicalKey -> encoded(
            holder -> SortedSet(tokenLock(holder, CurrencyId(otherMetagraph).some, "wrong-currency"))
          )
        )
      ).attempt
      validLock = tokenLock(holder, CurrencyId(metagraph).some, "valid")
      alternateProofs = NonEmptySet.one(SignatureProof(Id(Hex("01")), Signature(Hex("02"))))
      duplicateUnsigned <- reconstruct(
        metagraph,
        List(physicalKey -> encoded(holder -> SortedSet(validLock, validLock.copy(proofs = alternateProofs))))
      ).attempt
      valid <- reconstruct(metagraph, List(physicalKey -> encoded(holder -> SortedSet(validLock))))
    } yield
      expect.all(
        empty.left.exists(_.getMessage.contains("empty token-lock set")),
        wrongSource.left.exists(_.getMessage.contains("token-lock source/holder mismatch")),
        missingCurrency.left.exists(_.getMessage.contains("token-lock currency scope mismatch")),
        wrongCurrency.left.exists(_.getMessage.contains("token-lock currency scope mismatch")),
        duplicateUnsigned.left.exists(_.getMessage.contains("duplicate unsigned token-lock identity")),
        valid.activeTokenLocks.contains(SortedMap(holder -> SortedSet(validLock)))
      )
  }
}
