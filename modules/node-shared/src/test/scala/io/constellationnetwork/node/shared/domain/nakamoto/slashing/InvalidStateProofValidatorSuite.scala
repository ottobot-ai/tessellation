package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.slashing.{InvalidStateProofEvidence, InvalidStateProofRejection}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** WATCHTOWER fraud-proof — coverage for the DETERMINISTIC dispute verdict ([[InvalidStateProofValidator]]).
  *
  * The load-bearing properties under test (the safety + determinism non-negotiables):
  *   - '''A single honest node catches a quorum-signed wrong root''': when the re-derivation does NOT reproduce the committee-attested
  *     root, the verdict is UPHELD.
  *   - '''An honest committee is never slashed''': when the re-derivation REPRODUCES the attested root, the verdict is NOT upheld
  *     (`DisputeNotUpheld`) — regardless of what the challenger CLAIMS in the envelope (the verdict recomputes, never trusts).
  *   - '''Determinism''': the verdict is a pure function of the evidence + the injected re-derivation; two runs over the same inputs agree.
  *   - The header / hash-binding / challenger-signature / double-slash rejections each fire on their own malformation.
  */
object InvalidStateProofValidatorSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp)

  private val shardZero: ShardId = ShardId(0).get
  private val epochZero: EtaPeriod = EtaPeriod(0L)
  private val anchor: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L))
  private val mgA: Address = Address.fromBytes("mgA".getBytes("UTF-8"))

  /** A committee signer with a fake (structurally-valid) signature set — the verdict never re-verifies these (the checkpoint signatures are
    * out of scope for the verdict, which re-derives the root), so fakes suffice for THIS suite.
    */
  private def committeeSig(idx: Int): CommitteeMemberSignature =
    CommitteeMemberSignature(PeerId(Hex(f"${idx}%02x" * 64)), Hex("aa" * 80), Hex("bb" * 64), Hex("cc" * 128), 0)

  /** A minimal one-MG checkpoint carrying `attestedRoot` as `mgA`'s `perMetagraphMptRoots`, signed by `nSigners` committee members. */
  private def mkCheckpoint(attestedRoot: Hash, nSigners: Int): ShardCheckpoint = {
    val bin: Signed[StateChannelSnapshotBinary] =
      Signed(
        StateChannelSnapshotBinary(Hash.empty, Array.emptyByteArray, SnapshotFee.MinValue),
        NonEmptySet.of(SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70))))
      )
    val delta = ShardDerivedStateDelta.empty.copy(
      perMetagraphMptRoots = SortedMap(mgA -> attestedRoot),
      includedSnapshots = SortedMap(mgA -> NonEmptyList.of(bin))
    )
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = Hash("0" * 64),
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = anchor,
      slot = Slot.unsafeApply(1L),
      derivedStateDelta = delta,
      emittedReceipts = List.empty,
      committeeSignatures = NonEmptyList.fromListUnsafe((1 to nSigners).toList.map(committeeSig)),
      epoch = epochZero
    )
  }

  /** Build a fraud proof + matching evidence with a REAL challenger Ed25519 signature over the canonical preimage. */
  private def mkEvidence(
    cp: ShardCheckpoint,
    challengerKp: KeyPair,
    submitterId: PeerId,
    claimed: Hash,
    challengerRoot: Hash
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[InvalidStateProofEvidence] =
    h.hash(cp.signingPreimage).flatMap { cpHash =>
      val unsigned = FraudProofEnvelope(
        shardId = cp.shardId,
        disputedCheckpointHash = cpHash,
        metagraphAddress = mgA,
        gl0AnchorOrdinal = cp.gl0AnchorOrdinal,
        claimedDerivation = claimed,
        challengerDerivation = challengerRoot,
        reexecutionWitness = Hex(challengerRoot.value),
        challengerSignature = Hex(""),
        submitterId = submitterId
      )
      h.hash(unsigned.signingPreimage).flatMap { digest =>
        Signing.signData[IO](digest.getBytes)(challengerKp.getPrivate).map { sig =>
          val fp = unsigned.copy(challengerSignature = Hex.fromBytes(sig))
          InvalidStateProofEvidence(
            shardId = cp.shardId,
            disputedCheckpoint = cp,
            metagraphAddress = mgA,
            attestedRoot = cp.derivedStateDelta.perMetagraphMptRoots.getOrElse(mgA, Hash.empty),
            fraudProof = fp
          )
        }
      }
    }

  private def challengerSetup(implicit sp: SecurityProvider[IO]): IO[(KeyPair, PeerId)] =
    KeyPairGenerator.makeKeyPair[IO].map(kp => (kp, PeerId.fromPublic(kp.getPublic)))

  private val attested: Hash = Hash("a" * 64)
  private val honestDifferent: Hash = Hash("b" * 64) // honest re-derivation ≠ attested ⇒ UPHELD

  test("UPHELD: a single honest re-derivation that differs from the quorum-attested root upholds the dispute") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val cp = mkCheckpoint(attested, nSigners = 6)
      // reDerive returns a DIFFERENT root than attested ⇒ committee deviated ⇒ upheld.
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(honestDifferent)
      val validator = InvalidStateProofValidator.make[IO](reDerive, InvalidStateProofSlashedReader.neverSlashed[IO])
      for {
        (kp, pid) <- challengerSetup
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        res <- validator.validate(ev)
      } yield expect(res.isRight)
  }

  test(
    "NOT upheld (honest-committee floor): re-derivation reproducing the attested root yields DisputeNotUpheld — even if the challenger LIES"
  ) {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val cp = mkCheckpoint(attested, nSigners = 6)
      // Honest re-derivation REPRODUCES the attested root ⇒ committee did NOT deviate. The challenger still CLAIMS a different
      // root (honestDifferent) in the envelope — the verdict must ignore that claim and recompute ⇒ NOT upheld.
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(attested)
      val validator = InvalidStateProofValidator.make[IO](reDerive, InvalidStateProofSlashedReader.neverSlashed[IO])
      for {
        (kp, pid) <- challengerSetup
        ev <- mkEvidence(cp, kp, pid, claimed = honestDifferent, challengerRoot = honestDifferent)
        res <- validator.validate(ev)
      } yield expect(res == Left(InvalidStateProofRejection.DisputeNotUpheld(attested, attested)))
  }

  test("determinism: the verdict is identical across repeated runs over the same evidence + pure re-derivation") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val cp = mkCheckpoint(attested, nSigners = 6)
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(honestDifferent)
      val validator = InvalidStateProofValidator.make[IO](reDerive, InvalidStateProofSlashedReader.neverSlashed[IO])
      for {
        (kp, pid) <- challengerSetup
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        r1 <- validator.validate(ev)
        r2 <- validator.validate(ev)
      } yield expect(r1.isRight && r2.isRight && r1.map(_ => ()) == r2.map(_ => ()))
  }

  test("reject: invalid challenger signature (tampered) ⇒ InvalidChallengerSignature, never reaching the verdict") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val cp = mkCheckpoint(attested, nSigners = 6)
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(honestDifferent)
      val validator = InvalidStateProofValidator.make[IO](reDerive, InvalidStateProofSlashedReader.neverSlashed[IO])
      for {
        (kp, pid) <- challengerSetup
        ev0 <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        // Corrupt the signature.
        ev = ev0.copy(fraudProof = ev0.fraudProof.copy(challengerSignature = Hex("00" * 64)))
        res <- validator.validate(ev)
      } yield expect(res == Left(InvalidStateProofRejection.InvalidChallengerSignature))
  }

  test("reject: disputed-checkpoint hash mismatch between the carried checkpoint and the fraud proof") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val cp = mkCheckpoint(attested, nSigners = 6)
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(honestDifferent)
      val validator = InvalidStateProofValidator.make[IO](reDerive, InvalidStateProofSlashedReader.neverSlashed[IO])
      for {
        (kp, pid) <- challengerSetup
        ev0 <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        // Point the fraud proof at a different checkpoint hash (and re-sign so the sig check passes first).
        wrongHash = Hash("f" * 64)
        reUnsigned = ev0.fraudProof.copy(disputedCheckpointHash = wrongHash, challengerSignature = Hex(""))
        digest <- h.hash(reUnsigned.signingPreimage)
        sig <- Signing.signData[IO](digest.getBytes)(kp.getPrivate)
        ev = ev0.copy(fraudProof = reUnsigned.copy(challengerSignature = Hex.fromBytes(sig)))
        res <- validator.validate(ev)
      } yield expect(res match { case Left(_: InvalidStateProofRejection.CheckpointHashMismatch) => true; case _ => false })
  }

  test("FAIL-CLOSED: reDerive returning the Hash.empty cannot-re-derive sentinel ⇒ dispute NOT upheld (never a slash)") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val cp = mkCheckpoint(attested, nSigners = 6)
      // `Hash.empty` is the production wiring's fail-closed "cannot re-derive" sentinel (`reExecDerivationWithDiff` returned
      // None — the pinned diff-base is unresolvable below this node's retention / not reached, or the derivation OMITted).
      // "This node can't check" is NOT evidence the committee deviated: upholding here would 100%-slash an honest committee
      // on a local retention miss. The verdict must fail closed — reject the dispute, never uphold.
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(Hash.empty)
      val validator = InvalidStateProofValidator.make[IO](reDerive, InvalidStateProofSlashedReader.neverSlashed[IO])
      for {
        (kp, pid) <- challengerSetup
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        res <- validator.validate(ev)
      } yield
        expect(
          res == Left(InvalidStateProofRejection.CannotRederive(mgA)),
          s"a dispute this node cannot re-derive must NEVER be upheld (fail-closed CannotRederive, no slash) — got $res"
        )
  }

  test("FAIL-CLOSED: Hash.empty sentinel with NO attested root for the MG ⇒ still CannotRederive (never the None-attested UPHELD branch)") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      // A structurally-invalid checkpoint (binaries included, no attested root) would normally be UPHELD — but ONLY on an
      // affirmative honest re-derivation. With the cannot-re-derive sentinel the verdict must STILL fail closed.
      val cpNoRoot = {
        val cp0 = mkCheckpoint(attested, nSigners = 3)
        cp0.copy(derivedStateDelta = cp0.derivedStateDelta.copy(perMetagraphMptRoots = SortedMap.empty))
      }
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(Hash.empty)
      val validator = InvalidStateProofValidator.make[IO](reDerive, InvalidStateProofSlashedReader.neverSlashed[IO])
      for {
        (kp, pid) <- challengerSetup
        ev <- mkEvidence(cpNoRoot, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        res <- validator.validate(ev)
      } yield expect(res == Left(InvalidStateProofRejection.CannotRederive(mgA)), s"got $res")
  }

  test("reject: already-slashed (double-slash guard) on (shardId, disputedCheckpointHash)") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val cp = mkCheckpoint(attested, nSigners = 6)
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(honestDifferent)
      for {
        (kp, pid) <- challengerSetup
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        cpHash = ev.fraudProof.disputedCheckpointHash
        reader = InvalidStateProofSlashedReader.fromSet[IO](Set((shardZero, cpHash)))
        validator = InvalidStateProofValidator.make[IO](reDerive, reader)
        res <- validator.validate(ev)
      } yield expect(res == Left(InvalidStateProofRejection.AlreadySlashed(shardZero, cpHash)))
  }
}
