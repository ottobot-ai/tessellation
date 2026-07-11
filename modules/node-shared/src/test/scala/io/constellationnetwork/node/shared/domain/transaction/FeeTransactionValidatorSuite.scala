package io.constellationnetwork.node.shared.domain.transaction

import cats.data.Validated.{Invalid, Valid}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.currency.dataApplication.FeeTransaction
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.transaction.FeeTransactionValidator._
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.SignedValidator.{InvalidSignatures, NotSignedExclusivelyByAddressOwner}
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.auto._
import weaver.MutableIOSuite

object FeeTransactionValidatorSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, sp)

  test("validate accepts a fee transaction signed by its source") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      transaction = FeeTransaction(
        sourceKeyPair.getPublic.toAddress,
        destinationKeyPair.getPublic.toAddress,
        Amount(10L),
        Hash.empty
      )
      signed <- Signed.forAsyncHasher(transaction, sourceKeyPair)
      result <- FeeTransactionValidator.make[IO](SignedValidator.make[IO]).validate(signed)
    } yield expect.same(Valid(signed), result)
  }

  test("validate rejects a source-identified proof whose signature bytes do not match the fee transaction") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      transaction = FeeTransaction(
        sourceKeyPair.getPublic.toAddress,
        destinationKeyPair.getPublic.toAddress,
        Amount(10L),
        Hash.empty
      )
      signed <- Signed.forAsyncHasher(transaction, sourceKeyPair)
      tampered = signed.copy(value = transaction.copy(amount = Amount(11L)))
      result <- FeeTransactionValidator.make[IO](SignedValidator.make[IO]).validate(tampered)
    } yield expect(result match {
      case Invalid(errors) => errors.exists {
          case InvalidSigned(InvalidSignatures(proofs)) => proofs === tampered.proofs
          case _                                         => false
        }
      case Valid(_) => false
    })
  }

  test("validate rejects a cryptographically valid signature from a non-source key") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      otherKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      transaction = FeeTransaction(
        sourceKeyPair.getPublic.toAddress,
        destinationKeyPair.getPublic.toAddress,
        Amount(10L),
        Hash.empty
      )
      signedByOther <- Signed.forAsyncHasher(transaction, otherKeyPair)
      result <- FeeTransactionValidator.make[IO](SignedValidator.make[IO]).validate(signedByOther)
    } yield expect(result match {
      case Invalid(errors) => errors.exists {
          case NotSignedBySourceAddressOwner => true
          case InvalidSigned(NotSignedExclusivelyByAddressOwner) => true
          case _ => false
        }
      case Valid(_) => false
    })
  }
}
