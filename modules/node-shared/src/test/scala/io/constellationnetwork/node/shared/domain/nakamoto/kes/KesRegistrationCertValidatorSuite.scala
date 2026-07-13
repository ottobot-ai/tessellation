package io.constellationnetwork.node.shared.domain.nakamoto.kes

import java.security.KeyPair

import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits.catsSyntaxValidatedIdBinCompat0

import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator._
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.SignedValidator
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Exercises every rejection path in [[KesRegistrationCertValidator]]. Each test isolates one validator concern and leaves all other inputs
  * in a well-formed state, so a failure precisely identifies which check fired.
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

  private val defaultVkBytes: Hex = Hex("11" * KesMasterVerificationKeyLength)
  private val defaultVrfPublicKey: Hex = Hex("22" * 32)
  private val registrationParentHash: Hash = Hash("aa" * 32)
  private val defaultPeriod: EtaPeriod = EtaPeriod(100L)
  private val defaultContext: RegistrationEvaluationContext =
    RegistrationEvaluationContext(registrationParentHash, defaultPeriod)
  private val futurePeriod: EtaPeriod = EtaPeriod(200L)

  private def mkCert(
    operatorId: PeerId,
    effectiveFromPeriod: EtaPeriod = futurePeriod,
    ordinal: KesRegistrationOrdinal = KesRegistrationOrdinal.first,
    parent: KesRegistrationReference = KesRegistrationReference.empty,
    kesMasterVK: Hex = defaultVkBytes,
    kesMasterVKStep: Int = 0,
    offset: Option[Long] = None,
    vrfPublicKey: Hex = defaultVrfPublicKey,
    gl0ParentHash: Hash = registrationParentHash
  ): KesRegistrationCert =
    KesRegistrationCert(
      operatorPeerId = operatorId,
      kesMasterVK = kesMasterVK,
      kesMasterVKStep = kesMasterVKStep,
      offset = offset.getOrElse(effectiveFromPeriod.value),
      vrfPublicKey = vrfPublicKey,
      effectiveFromPeriod = effectiveFromPeriod,
      registrationParentHash = gl0ParentHash,
      ordinal = ordinal,
      parent = parent
    )

  private def mkSeedlist(peerIds: PeerId*)(implicit sp: SecurityProvider[IO]): IO[Option[Set[SeedlistEntry]]] =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      otherEntry = SeedlistEntry(PeerId.fromPublic(keyPair.getPublic), None, None, None, None)
    } yield Some(peerIds.map(SeedlistEntry(_, None, None, None, None)).toSet + otherEntry)

  private def mkValidator(seedlist: Option[Set[SeedlistEntry]])(
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
      result <- v.validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield expect.same(Valid(signed), result)
  }

  test("accepts when seedlist is None (no seedlist gating)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId)
    for {
      signed <- forAsyncHasher(cert, kp)
      v = mkValidator(None)
      result <- v.validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
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
      result <- v.validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
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
      result <- v.validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
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
      result <- v.validate(multiSigned, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
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

  test("rejects a replayed registration ordinal") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    // lastRef carries ordinal=5; we submit a cert with ordinal=5 (equal — must be strictly greater).
    val lastRef = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(5L)), io.constellationnetwork.security.hash.Hash.empty)
    val cert = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(5L)), parent = lastRef)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, lastRef, EtaPeriod.Zero, defaultContext)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: InvalidRegistrationOrdinal => true
            case _                             => false
          }
        case _ => false
      })
  }

  test("rejects a skipped registration ordinal") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val lastRef = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(5L)), Hash("cc" * 32))
    val cert = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(7L)), parent = lastRef)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, lastRef, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case InvalidRegistrationOrdinal(_, expected) => expected == KesRegistrationOrdinal(NonNegLong(6L))
            case _                                       => false
          }
        case _ => false
      })
  }

  test("requires the first registration to use ordinal one") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(2L)))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case InvalidRegistrationOrdinal(_, expected) => expected == KesRegistrationOrdinal.first
            case _                                       => false
          }
        case _ => false
      })
  }

  test("fails closed when the next registration ordinal would overflow") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val maxOrdinal = KesRegistrationOrdinal(NonNegLong(Long.MaxValue))
    val lastRef = KesRegistrationReference(maxOrdinal, Hash("cc" * 32))
    val cert = mkCert(operatorId, ordinal = maxOrdinal, parent = lastRef)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, lastRef, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case RegistrationOrdinalOverflow(`maxOrdinal`) => true
            case _                                         => false
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
      result <- v.validate(signed, lastRef, EtaPeriod.Zero, defaultContext)
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

  test("rejects activation at N+1 (InsufficientActivationDelay)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, effectiveFromPeriod = EtaPeriod(101L))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: InsufficientActivationDelay => true
            case _                              => false
          }
        case _ => false
      })
  }

  test("accepts activation at exactly N+2") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, effectiveFromPeriod = EtaPeriod(102L))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield expect.same(Valid(signed), result)
  }

  test("fails closed when computing N+2 would overflow") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val evaluationPeriod = EtaPeriod(Long.MaxValue - 1L)
    val context = RegistrationEvaluationContext(registrationParentHash, evaluationPeriod)
    val cert = mkCert(operatorId, effectiveFromPeriod = EtaPeriod(Long.MaxValue))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, context)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case ActivationPeriodOverflow(`evaluationPeriod`) => true
            case _                                            => false
          }
        case _ => false
      })
  }

  test("fails closed for a negative inclusion period") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val invalidPeriod = EtaPeriod(-1L)
    val context = RegistrationEvaluationContext(registrationParentHash, invalidPeriod)
    val cert = mkCert(operatorId)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, context)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case InvalidInclusionPeriod(`invalidPeriod`) => true
            case _                                       => false
          }
        case _ => false
      })
  }

  test("rejects a signed registration bound to a different GL0 parent") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val wrongParent = Hash("bb" * 32)
    val cert = mkCert(operatorId, gl0ParentHash = wrongParent)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case InvalidRegistrationParent(`wrongParent`, `registrationParentHash`) => true
            case _                                                                  => false
          }
        case _ => false
      })
  }

  test("rejects a KES master verification key shorter than 32 bytes") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, kesMasterVK = Hex("11" * 31))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
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

  test("rejects a KES master verification key longer than 32 bytes") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, kesMasterVK = Hex("11" * 33))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) => errors.exists(_.isInstanceOf[MalformedVk])
        case _               => false
      })
  }

  test("rejects a malformed non-hex KES master verification key") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, kesMasterVK = Hex("zz"))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) => errors.exists(_.isInstanceOf[MalformedVk])
        case _               => false
      })
  }

  test("rejects when a fresh KES master key does not start at step zero") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, kesMasterVKStep = 1)
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: InconsistentKesActivation => true
            case _                            => false
          }
        case _ => false
      })
  }

  test("rejects when KES offset is negative") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, offset = Some(-1L))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: InconsistentKesActivation => true
            case _                            => false
          }
        case _ => false
      })
  }

  test("rejects when KES offset does not equal the shared activation period") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, effectiveFromPeriod = EtaPeriod(200L), offset = Some(199L))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: InconsistentKesActivation => true
            case _                            => false
          }
        case _ => false
      })
  }

  test("rejects a VRF public key shorter than 32 bytes") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, vrfPublicKey = Hex("22" * 31))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: MalformedVrfPublicKey => true
            case _                        => false
          }
        case _ => false
      })
  }

  test("rejects a VRF public key longer than 32 bytes") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, vrfPublicKey = Hex("22" * 33))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: MalformedVrfPublicKey => true
            case _                        => false
          }
        case _ => false
      })
  }

  test("rejects a non-hex VRF public key") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, vrfPublicKey = Hex("not-hex"))
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      result <- mkValidator(seedlist).validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield
      expect(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: MalformedVrfPublicKey => true
            case _                        => false
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
      result <- v.validate(signed, KesRegistrationReference.empty, EtaPeriod.Zero, defaultContext)
    } yield expect.same((Rejected: KesRegistrationCertValidationError).invalidNec, result)
  }

  test("rejects when effectiveFromPeriod < prior cert's effectiveFromPeriod (NonMonotonicEffectiveFromPeriod)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    // Both satisfy the N+2 delay from currentPeriod (100), so only the monotonic-period check fires.
    val priorEffective = EtaPeriod(300L)
    val lastRef = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(1L)), io.constellationnetwork.security.hash.Hash("aa" * 32))
    val cert = mkCert(
      operatorId,
      effectiveFromPeriod = EtaPeriod(250L),
      ordinal = KesRegistrationOrdinal(NonNegLong(2L)),
      parent = lastRef
    )
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, lastRef, priorEffective, defaultContext)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: NonMonotonicEffectiveFromPeriod => true
            case _                                  => false
          }
        case _ => false
      })
  }

  test("rejects when effectiveFromPeriod == prior cert's effectiveFromPeriod (strict monotone)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val priorEffective = EtaPeriod(300L)
    val lastRef = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(1L)), io.constellationnetwork.security.hash.Hash("aa" * 32))
    // Ordinal increases while effectiveFromPeriod stays the same, so the rotation must still reject.
    val cert = mkCert(
      operatorId,
      effectiveFromPeriod = priorEffective,
      ordinal = KesRegistrationOrdinal(NonNegLong(2L)),
      parent = lastRef
    )
    for {
      signed <- forAsyncHasher(cert, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed, lastRef, priorEffective, defaultContext)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: NonMonotonicEffectiveFromPeriod => true
            case _                                  => false
          }
        case _ => false
      })
  }

  test("accepts when effectiveFromPeriod > prior cert's effectiveFromPeriod (strict monotone passes)") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val priorEffective = EtaPeriod(150L)
    val lastRef = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(1L)), io.constellationnetwork.security.hash.Hash("aa" * 32))
    val cert = mkCert(
      operatorId,
      effectiveFromPeriod = EtaPeriod(300L),
      ordinal = KesRegistrationOrdinal(NonNegLong(2L)),
      parent = lastRef
    )
    for {
      signed <- forAsyncHasher(cert, kp)
      hashedFirst <- signed.toHashed
      _ = hashedFirst // unused suppression
      // We use a hand-rolled lastRef hash since the test's purpose is the monotonicity check, not chain-link verification —
      // the parent ref must match `lastRef` exactly. Use the same hash.
      certWithMatchingParent = cert.copy(parent = lastRef)
      signedMatched <- forAsyncHasher(certWithMatchingParent, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signedMatched, lastRef, priorEffective, defaultContext)
    } yield expect.same(Valid(signedMatched), result)
  }

  test(
    "Risk-5 regression — chain {ord=1, eff=300; ord=2, eff=200} is REJECTED, preventing the genesis-fallback hole"
  ) { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    // Build cert1 (ord=1, eff=300) signed-and-hashed so its real ref can chain cert2.
    val cert1 = mkCert(operatorId, effectiveFromPeriod = EtaPeriod(300L), ordinal = KesRegistrationOrdinal.first)
    for {
      signed1 <- forAsyncHasher(cert1, kp)
      hashed1 <- signed1.toHashed
      ref1 = KesRegistrationReference.of(hashed1)
      // Attempted cert2: ord=2 (monotone-ordinal passes) but eff=200 < cert1.eff=300 — must be rejected by the new check.
      cert2 = mkCert(
        operatorId,
        effectiveFromPeriod = EtaPeriod(200L),
        ordinal = KesRegistrationOrdinal(NonNegLong(2L)),
        parent = ref1
      )
      signed2 <- forAsyncHasher(cert2, kp)
      seedlist <- mkSeedlist(operatorId)
      v = mkValidator(seedlist)
      result <- v.validate(signed2, ref1, cert1.effectiveFromPeriod, defaultContext)
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case _: NonMonotonicEffectiveFromPeriod => true
            case _                                  => false
          }
        case _ => false
      })
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
      r1 <- v.validate(signed1, ref1, EtaPeriod.Zero, defaultContext)
      // After accepting cert2, ref2 is lastRef; replaying signed1 and signed2 both fail.
      r2 <- v.validate(signed1, ref2, EtaPeriod.Zero, defaultContext)
      r3 <- v.validate(signed2, ref2, EtaPeriod.Zero, defaultContext)
    } yield
      expect.all(
        r1.isInvalid,
        r2.isInvalid,
        r3.isInvalid
      )
  }
}
