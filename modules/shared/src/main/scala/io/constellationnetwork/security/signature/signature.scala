package io.constellationnetwork.security.signature

import java.security.{KeyPair, PrivateKey}

import cats.Applicative
import cats.effect.Async
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._

import io.constellationnetwork.ext.cats.data.OrderBasedOrdering
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import derevo.cats.{order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import derevo.scalacheck.arbitrary
import io.circe.Encoder
import io.estatico.newtype.macros.newtype
import io.estatico.newtype.ops._
import org.typelevel.log4cats.slf4j.Slf4jLogger

import Signing.{signData, verifySignature}

object signature {

  @derive(arbitrary, decoder, encoder, show, order)
  @newtype
  case class Signature(value: Hex)

  object Signature {

    def fromHash[F[_]: Async: SecurityProvider](privateKey: PrivateKey, hash: Hash): F[Signature] =
      signData(hash.getBytes)(privateKey).map(raw => Signature(Hex.fromBytes(raw)))

    private[security] def fromDigest[F[_]: Async: SecurityProvider](
      privateKey: PrivateKey,
      digest: ConsensusDigest
    ): F[Signature] =
      signData(digest.toByteVector.toArray)(privateKey).map(raw => Signature(Hex.fromBytes(raw)))

  }

  @derive(arbitrary, decoder, encoder, show, order)
  case class SignatureProof(id: Id, signature: Signature)

  object SignatureProof {

    implicit object OrderingInstance extends OrderBasedOrdering[SignatureProof]

    def fromHash[F[_]: Async: SecurityProvider](keyPair: KeyPair, hash: Hash): F[SignatureProof] =
      for {
        id <- PeerId._Id.get(PeerId.fromPublic(keyPair.getPublic)).pure[F]
        signature <- Signature.fromHash(keyPair.getPrivate, hash)
      } yield SignatureProof(id, signature)

    private[security] def fromDigest[F[_]: Async: SecurityProvider](
      keyPair: KeyPair,
      digest: ConsensusDigest
    ): F[SignatureProof] =
      for {
        id <- PeerId._Id.get(PeerId.fromPublic(keyPair.getPublic)).pure[F]
        signature <- Signature.fromDigest(keyPair.getPrivate, digest)
      } yield SignatureProof(id, signature)

    def fromData[F[_]: Async: SecurityProvider: Hasher, A: Encoder](
      keyPair: KeyPair
    )(data: A): F[SignatureProof] = data.hash.flatMap(SignatureProof.fromHash(keyPair, _))

  }

  def verifySignatureProof[F[_]: Async: SecurityProvider](
    hash: Hash,
    signatureProof: SignatureProof
  ): F[Boolean] =
    verifySignatureProofBytes(hash.getBytes, signatureProof)

  private[security] def verifySignatureProof[F[_]: Async: SecurityProvider](
    digest: ConsensusDigest,
    signatureProof: SignatureProof
  ): F[Boolean] =
    verifySignatureProofBytes(digest.toByteVector.toArray, signatureProof)

  private def verifySignatureProofBytes[F[_]: Async: SecurityProvider](
    bytes: Array[Byte],
    signatureProof: SignatureProof
  ): F[Boolean] = {
    val verifyResult = for {
      signatureBytes <- Async[F].delay(signatureProof.signature.coerce.toBytes)
      publicKey <- signatureProof.id.hex.toPublicKey
      result <- verifySignature(bytes, signatureBytes)(publicKey)
    } yield result

    verifyResult.handleErrorWith { err =>
      Slf4jLogger.getLogger[F].error(err)(s"Failed to verify signature for peer ${signatureProof.id.show}") >>
        Applicative[F].pure(false)
    }

  }

}
