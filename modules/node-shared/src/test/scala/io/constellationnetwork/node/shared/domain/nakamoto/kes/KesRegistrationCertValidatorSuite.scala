package io.constellationnetwork.node.shared.domain.nakamoto.kes

import java.security.KeyPair

import cats.data.NonEmptySet
import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits.catsSyntaxValidatedIdBinCompat0

import scala.collection.immutable.SortedSet

import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationReference}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Exercises every rejection path in [[KesRegistrationCertValidator]] (Slice 10 / #179). Each test isolates one validator concern and
  * leaves all other inputs in a well-formed state, so a failure precisely identifies which check fired.
  */
object KesRegistrationCertValidatorSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO], KeyPair, PeerId)

  def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
    implicit0(sp: SecurityProvider[IO]) <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
    kp <- KeyPairGenerator.makeKeyPair[IO].asResource
    operatorId = PeerId.fromPublic(kp.getPublic)
  } yield (j, h, sp, kp, operatorId)

  private val defaultVkBytes: Hex = Hex("00112233445566778899aabbccddeeff")
  private val defaultEpoch: EpochProgress = EpochProgress(NonNegLong(100L))
  private val futureEpoch: EpochProgress = EpochProgress(NonNegLong(200L))

  private def mkCert(
    operatorId: PeerId,
    effectiveFromEpoch: EpochProgress = futureEpoch,
    ordinal: KesRegistrationOrdinal = KesRegistrationOrdinal.first,
    parent: KesRegistrationReference = KesRegistrationReference.empty,
    kesMasterVK: Hex = defaultVkBytes,
    kesMasterVKStep: Int = 0,
    offset: Long = 0L
  ): KesRegistrationCert =
    KesRegistrationCert(
      operatorPeerId = operatorId,
      kesMasterVK = kesMasterVK,
      kesMasterVKStep = kesMasterVKStep,
      offset = offset,
      effectiveFromEpoch = effectiveFromEpoch,
      ordinal = ordinal,
      parent = parent
    )

  private def mkSeedlist(peerIds: PeerId*)(implicit sp: SecurityProvider[IO]): IO[Option[Set[SeedlistEntry]]] =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      otherEntry = SeedlistEntry(PeerId.fromPublic(keyPair.getPublic), None, None, None, None)
    } yield Some(peerIds.map(SeedlistEntry(_, None, None, None, None)).toSet + otherEntry)

  private def mkValidator(seedlist: Option[Set[SeedlistEntry]] = None)(
    implicit S: SecurityProvider[IO],
    H: Hasher[IO]
  ): KesRegistrationCertValidator[IO] = {
    val signedValidator = SignedValidator.make[IO]
    make[IO](signedValidator, seedlist)
  }

  test("accepts a well-formed cert from a seedlisted operator") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, defaultEpoch)
    } yield expect.same(Valid(signed), result)
  }

  test("accepts when seedlist is None (no seedlist gating)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId)
    for {
      signed <- forAsyncHasher(cert, kp)
      v = mkValidator(None)
      result <- v.validate(signed, KesRegistrationReference.empty, defaultEpoch)
    } yield expect.same(Valid(signed), result)
  }

  test("rejects when the cert is signed by a key that is not the operator (SignerOperatorMismatch)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId)
    for {
      otherKp <- KeyPairGenerator.makeKeyPair[IO]
      // Signed by a different key — operatorPeerId in the cert points at `kp`, but the proof is by `otherKp`.
      signed <- forAsyncHasher(cert, otherKp)
      seedlist <- mkSeedlist(operatorId, PeerId.fromPublic(otherKp.getPublic))
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, defaultEpoch)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: SignerOperatorMismatch => true
            case _                         => false
          }
        case _ => false
      })
  }

  test("rejects when the operator is not in the seedlist (UnauthorizedOperator)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist() // operator NOT in seedlist
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, defaultEpoch)
    } yield expect.same(UnauthorizedOperator(operatorId).invalidNec, result)
  }

  test("rejects when the cert has more than one signature (TooManySignatures)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId)
    for {
      otherKp <- KeyPairGenerator.makeKeyPair[IO]
      signed1 <- forAsyncHasher(cert, kp)
      signed2 <- forAsyncHasher(cert, otherKp)
      multiSigned = signed1.addProof(signed2.proofs.head)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(multiSigned, KesRegistrationReference.empty, defaultEpoch)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case TooManySignatures(_) => true
            case _                    => false
          }
        case _ => false
      })
  }

  test("rejects when the cert ordinal is not strictly greater than lastRef (NonMonotonicOrdinal)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    // lastRef carries ordinal=5; we submit a cert with ordinal=5 (equal — must be strictly greater).
    val lastRef = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(5L)), io.constellationnetwork.security.hash.Hash.empty)
    val cert = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(5L)), parent = lastRef)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, lastRef, defaultEpoch)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: NonMonotonicOrdinal => true
            case _                      => false
          }
        case _ => false
      })
  }

  test("rejects when the parent ref does not match the lastRef (InvalidParent)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val lastRef = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(5L)), io.constellationnetwork.security.hash.Hash("aa" * 32))
    // Parent points to the wrong hash.
    val cert = mkCert(
      operatorId,
      ordinal = KesRegistrationOrdinal(NonNegLong(6L)),
      parent = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(5L)), io.constellationnetwork.security.hash.Hash("bb" * 32))
    )
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, lastRef, defaultEpoch)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: InvalidParent => true
            case _                => false
          }
        case _ => false
      })
  }

  test("rejects when effectiveFromEpoch <= currentEpoch (NotForwardActivation)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    // effectiveFromEpoch == currentEpoch is rejected (strict inequality required).
    val cert = mkCert(operatorId, effectiveFromEpoch = defaultEpoch)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, defaultEpoch)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: NotForwardActivation => true
            case _                       => false
          }
        case _ => false
      })
  }

  test("rejects when kesMasterVK is empty (MalformedVk)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, kesMasterVK = Hex(""))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, defaultEpoch)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: MalformedVk => true
            case _              => false
          }
        case _ => false
      })
  }

  test("rejects when kesMasterVKStep is negative (MalformedVk)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, kesMasterVKStep = -1)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, defaultEpoch)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: MalformedVk => true
            case _              => false
          }
        case _ => false
      })
  }

  test("rejects when offset is negative (MalformedVk)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, offset = -1L)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, defaultEpoch)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: MalformedVk => true
            case _              => false
          }
        case _ => false
      })
  }

  test("rejectAll returns Rejected for every input") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId)
    for {
      signed <- forAsyncHasher(cert, kp)
      v = rejectAll[IO]
      result <- v.validate(signed, KesRegistrationReference.empty, defaultEpoch)
    } yield expect.same((Rejected: KesRegistrationCertValidationError).invalidNec, result)
  }

  test("monotonicity property — replays of the same ordinal are always rejected") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    // Build a chain of 3 sequential certs; replaying any of them after the chain has advanced must be rejected.
    val cert1 = mkCert(operatorId, ordinal = KesRegistrationOrdinal.first)
    for {
      signed1 <- forAsyncHasher(cert1, kp)
      hashed1 <- signed1.toHashed
      ref1 = KesRegistrationReference.of(hashed1)
      cert2 = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(2L)), parent = ref1)
      signed2 <- forAsyncHasher(cert2, kp)
      hashed2 <- signed2.toHashed
      ref2 = KesRegistrationReference.of(hashed2)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)

      // After accepting cert1, ref1 is the lastRef; replaying signed1 with the new lastRef must fail.
      r1 <- v.validate(signed1, ref1, defaultEpoch)
      // After accepting cert2, ref2 is lastRef; replaying signed1 and signed2 both fail.
      r2 <- v.validate(signed1, ref2, defaultEpoch)
      r3 <- v.validate(signed2, ref2, defaultEpoch)
    } yield
      expect.all(
        r1.isInvalid,
        r2.isInvalid,
        r3.isInvalid
      )
  }
}
