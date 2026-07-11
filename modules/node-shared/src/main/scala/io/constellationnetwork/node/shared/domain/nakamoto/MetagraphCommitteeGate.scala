package io.constellationnetwork.node.shared.domain.nakamoto

import java.security.KeyPair

import cats.Functor
import cats.effect.kernel.{Async, Clock, Temporal}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger

/** Slice S3 — the load-bearing pre-inclusion gate for metagraph state-channel binaries.
  *
  * Sits in front of `processMetagraphBinary` and gates admission on `≥ kQuorum` committee attestations
  * (`MetagraphAttestationAggregator.thresholdReached`). This converts the warn-only S2/S2.5 aggregator into a cluster-coordination
  * primitive: non-committee operators skip the binary entirely (sharding payoff), and a binary cannot enter a global snapshot until its
  * committee has spoken.
  *
  * '''Draw/quorum decouple.''' The committee DRAW (`isInCommittee` sender-side / `verifyMembership` receiver-side) uses `kDraw`; the admit
  * quorum the gate polls for uses `kQuorum`. Both are cluster-uniform HOCON (`nakamoto.committee.{kDraw,kQuorum}`) so the draw is
  * byte-identical sender↔receiver and every node admits at the same count. They are deliberately separate: when expected-committee ==
  * admit-quorum, the binomial committee draw left ~36% of binaries with a committee too small to reach the quorum → gate timeout →
  * re-buffer churn → gl0 trailed ml0. With `kDraw = N` the threshold saturates (committee = everyone) so `P(|committee| ≥ kQuorum) = 1`.
  *
  * '''Sender path (`attestAndAdmit`).''' When this node observes a new `Signed[StateChannelSnapshotBinary]`:
  *   1. Compute `committeeSortition.isInCommittee(vrfSk, eta, metagraphAddress, parentHash, sigmaOperatorKey, kDraw)`. 2. If we're in the
  *      committee: build a `pb.MetagraphAttestation` carrying the VRF proof + an Ed25519 long-term-key sig + a REQUIRED KES product sig
  *      (Slice 9 path), record it locally in the aggregator, and gossip via the sidecar's `PublishMetagraphAttestation` RPC. 3. Either way,
  *      poll `aggregator.thresholdReached(...)` until it returns `true` OR the timeout fires. On threshold → admit (return `true`). On
  *      timeout → drop (return `false`).
  *
  * '''Receiver path (`recordReceivedAttestation`).''' For each `pb.MetagraphAttestation` arriving on the sidecar:
  *   1. Verify the long-term Ed25519 signature over the canonical message bytes (same Hasher pipeline TipAttestation uses — domain values
  *      JSON-encoded → `Hasher[F].hash` → byte form of the Hash). 2. Require both the KES master key and VRF verification key to be present
  *      in the genesis-loaded operator registries. Missing registration rejects; an Ed25519 signature does not authorize an unregistered
  *      committee identity. 3. Require the wire-carried VRF key to byte-match the registered key, then verify the committee proof under the
  *      registered key: `CommitteeSortition.verifyMembership(registeredVrfVk, eta, metagraphAddress, parentHash, sigmaSender, kDraw,
  *      proof)`. The sender cannot grind a replacement key in-band. Note σ_sender is looked up against the StakeRegistry — the sender's
  *      stake, not ours. The N-2 staging will land later (#180); for now we read live stake, which is byte-equivalent under the
  *      no-mid-epoch-stake-change rule the test cluster runs under. 4. If all three verifies pass: `aggregator.record(metagraphAddress,
  *      parentHash, binaryHash, peerId)`. Otherwise WARN and drop. The committee VRF check uses `kDraw` (the DRAW target), matching the
  *      sender's `isInCommittee(... kDraw)`.
  *
  * '''Pruning.''' `pruneParents(addr, parents)` is a pass-through to the underlying aggregator. Callers wire this at gl0 finality (see
  * `SnapshotLeaderLoop`): when a gl0 snapshot finalizes, iterate its `stateChannelSnapshots` and prune those `(metagraphAddress,
  * parentHash)` pairs out of the aggregator. The map then no longer grows monotonically.
  *
  * '''Config.'''
  *   - `nakamoto.committee.k-draw` (env `NAKAMOTO_COMMITTEE_K_DRAW`) — committee DRAW target, cluster-uniform HOCON. `kDraw = N` puts
  *     everyone in every committee (threshold saturates) so `P(|committee| ≥ kQuorum) = 1`; smaller values give genuine sortition.
  *   - `nakamoto.committee.k-quorum` (env `NAKAMOTO_COMMITTEE_K_QUORUM`) — admit quorum, cluster-uniform HOCON. The exact distinct-attester
  *     count the gate waits for. Invariant (fail-fast at load): `0 < kQuorum <= kDraw`. 8-node testnet default: `kDraw = 8, kQuorum = 6`.
  *   - `NAKAMOTO_COMMITTEE_GATE_TIMEOUT_MS` — poll timeout for the sender path. Default 30000ms. Beyond this the binary is dropped
  *     (WARN-logged) and a counter `dag_nakamoto_committee_gate_dropped_total` is incremented.
  *   - `NAKAMOTO_COMMITTEE_GATE_POLL_INTERVAL_MS` — how often the gate re-checks the threshold while waiting. Default 250ms (a fraction of
  *     the slot cadence; coarser would add latency, finer would just spin).
  */
trait MetagraphCommitteeGate[F[_]] {

  /** Sender path: maybe-attest + wait for committee threshold + return whether to admit the binary.
    *
    * `eta` is the current epoch-randomness seed for the metagraph's parent's epoch (see `EtaCalculation`). The caller is responsible for
    * sourcing it via the same code path the leader VRF uses — typically `epochStateRef.get` + `EtaCalculation.computeEta` against the chain
    * store. `sigmaOperatorKey` is THIS node's admission-draw weight. Production currently supplies uniform `1/N` through
    * `StakeRegistry.committeeStake`; it is not economic stake and does not use the N-2 stake distribution.
    *
    * Returns `true` if the committee threshold reached and the caller should proceed with the existing accept path; `false` if the timeout
    * fired and the binary should be dropped.
    */
  def attestAndAdmit(
    metagraphAddress: Address,
    parentHash: Hash,
    binaryHash: Hash,
    eta: Array[Byte],
    sigmaOperatorKey: Ratio
  ): F[Boolean]

  /** Receiver path: parse a `pb.MetagraphAttestation` arriving on the sidecar, verify Ed25519 + KES + committee VRF, and record on success.
    * Verification failures are logged at WARN and dropped — never thrown.
    *
    * `lookupSenderStake` returns the σ_sender to feed into committee-VRF verification. Implemented as a callback so the receiver path
    * doesn't have to take a `StakeRegistry[F]` dependency directly (it does, via the impl wiring, but the trait stays agnostic to make
    * tests cheap).
    */
  def recordReceivedAttestation(
    att: MetagraphCommitteeGate.IncomingAttestation,
    eta: Array[Byte],
    lookupSenderStake: PeerId => F[Ratio]
  ): F[Unit]

  /** Drop tally state for `(metagraphAddress, p)` pairs that have moved past the live decision window — typically called from the gl0
    * finality hook with the set of `lastSnapshotHash` values found in the just-finalized `stateChannelSnapshots`.
    */
  def pruneParents(metagraphAddress: Address, parents: Set[Hash]): F[Unit]
}

object MetagraphCommitteeGate {

  /** Timeout (ms) the sender path waits for the committee threshold before dropping the binary. Read once at JVM start; in-flight tuning
    * requires a restart. Default 30000ms — generous enough for 8-node testnets where K=N puts the threshold trivially reachable, while
    * bounding the wait for adversarial / partitioned cases.
    */
  val DefaultGateTimeoutMs: Long =
    sys.env.get("NAKAMOTO_COMMITTEE_GATE_TIMEOUT_MS").flatMap(_.toLongOption).getOrElse(30000L)

  /** Sender-path polling interval (ms). Default 250ms — a quarter-second is well below the slot tick yet far enough apart that the poll
    * loop doesn't spin.
    */
  val DefaultPollIntervalMs: Long =
    sys.env.get("NAKAMOTO_COMMITTEE_GATE_POLL_INTERVAL_MS").flatMap(_.toLongOption).getOrElse(250L)

  /** Pre-parsed inbound attestation. The wire-level `pb.MetagraphAttestation` is converted to this domain shape at the NakamotoSyncDaemon
    * boundary so the receiver-side core never touches the proto types. Lets tests construct an `IncomingAttestation` directly without
    * standing up the sidecar.
    */
  final case class IncomingAttestation(
    senderPeerId: PeerId,
    senderVrfVk: Array[Byte],
    metagraphAddress: Address,
    parentHash: Hash,
    binaryHash: Hash,
    committeeVrfProof: Array[Byte],
    longTermSignature: Array[Byte],
    kesSignature: Array[Byte],
    senderTreeStep: Int
  )

  /** Result of a single sender-side attempt. Exposed in the publisher path so tests can assert "we did/did-not emit an attestation" without
    * scraping logs.
    */
  sealed trait SenderOutcome extends Product with Serializable
  object SenderOutcome {
    case object NotInCommittee extends SenderOutcome
    final case class Attested(proof: Array[Byte], output: Array[Byte]) extends SenderOutcome
    case object InvalidVrfIdentity extends SenderOutcome

    /** The metagraph parent ordinal could not be resolved (see `MetagraphParentOrdinalResolver`). We skip publishing — emitting an
      * attestation with a guessed period would be byte-asymmetric to peers who can resolve the right one, so the receivers would reject it
      * anyway and we'd just spam the wire.
      */
    case object UnknownParentOrdinal extends SenderOutcome
  }

  /** Outcome of the `recordReceivedAttestation` path. Tests assert these values; the daemon path consumes the unit variant of the receiver
    * (mapped to `F[Unit]` to keep the trait small).
    */
  sealed trait ReceiverOutcome extends Product with Serializable
  object ReceiverOutcome {
    case object Recorded extends ReceiverOutcome
    case object EmptyLongTermSig extends ReceiverOutcome
    case object InvalidLongTermSig extends ReceiverOutcome
    case object EmptyKesSig extends ReceiverOutcome
    case object InvalidKesSig extends ReceiverOutcome
    case object UnregisteredVrfIdentity extends ReceiverOutcome
    case object MismatchedVrfIdentity extends ReceiverOutcome
    case object InvalidCommitteeVrf extends ReceiverOutcome

    /** The metagraph parent ordinal lookup returned `None`. Either (a) the gl0 GSI hasn't yet observed a tip for this metagraph
      * (pre-bootstrap window), or (b) the incoming binary's `parentHash` doesn't match the gl0-recorded
      * `lastStateChannelSnapshotHashes[mg]` — see `MetagraphParentOrdinalResolver`. Fail-closed: drop the attestation.
      */
    case object UnknownParentOrdinal extends ReceiverOutcome
  }

  /** The pre-message that gets Hasher-hashed for both the long-term Ed25519 signature and (separately) any future domain-separation.
    * Producer and verifier MUST agree on this case class's Circe encoding for the signature to round-trip. Keep it tight — exactly the
    * fields a slashing validator (S4) needs to identify the attestation.
    */
  final case class AttestationMessage(
    senderPeerIdHex: String,
    metagraphAddress: Address,
    parentHash: Hash,
    binaryHash: Hash
  )

  object AttestationMessage {
    import io.circe.Encoder
    import io.circe.generic.semiauto.deriveEncoder
    implicit val encoder: Encoder[AttestationMessage] = deriveEncoder
  }

  /** Trait describing the verification of a KES signature for a metagraph attestation. Decoupled from the dag-l0 implementation
    * (`KesGossipVerification`) so the gate can be tested without that file pulled in.
    *
    * '''Contract.''' Returns `true` iff the KES signature verifies under the registered master VK at the tree-internal `kesStep` supplied
    * by the call site (wire-carried from the sender). Returns `false` on empty sig, decode failure, or step-out-of-range. No chain-state
    * lookup is involved — the receiver does not need to know the operator's activation offset or the global eta rotation cadence to verify.
    */
  trait KesVerifier[F[_]] {
    def verify(
      messageBytes: Array[Byte],
      kesSigBytes: Array[Byte],
      attesterId: PeerId,
      attesterHex: Hex,
      kesStep: Int
    ): F[Boolean]
  }

  /** Algebra describing the KES product signer (sender side) for a metagraph attestation. Mirrors `KesVerifier` — the gate doesn't depend
    * on the concrete `OperationalKeyMakerAlgebra` so the test path can stub it. The default impl in `GlobalSnapshotConsensus` adapts
    * `operationalKeyMaker.currentPeriod` + `operationalKeyMaker.signAt`.
    *
    * Sign at the operator's CURRENT KES tree-internal step (`currentPeriod`) and report that step back to the caller via the wire so the
    * receiver can verify non-interactively. The caller is responsible for embedding `currentPeriod` on the wire alongside the signature.
    */
  trait KesSigner[F[_]] {

    /** The KES product step currently held by the in-memory key (offset from the operator's activation period). Sender embeds this on the
      * wire so receivers know which tree-internal step to verify against.
      */
    def currentPeriod: F[Int]

    /** Sign `message` at `kesStep`. Returns empty bytes on signer failure — the receiver-side gate treats empty as `EmptyKesSig` and
      * rejects. Callers should always pass `currentPeriod` here; signing at any other step is an error (`StepNotMonotonic` for past steps,
      * or destroys forward-secrecy for future steps).
      */
    def signAt(kesStep: Int, message: Array[Byte]): F[Array[Byte]]
  }

  /** Algebra describing how to publish a `MetagraphAttestation` to the sidecar. The default impl in `GlobalSnapshotConsensus` wraps
    * `SidecarClient.publishMetagraphAttestation`; tests use a `Ref`-backed stub.
    *
    * The wire-shape is isolated from the gate's pure logic — the gate computes everything, the publisher just sends the bytes. Failures are
    * logged + swallowed inside the impl (matches the existing `publishAttestation` failure model). `kesStep` is the sender's
    * `OperationalKeyMakerAlgebra.currentPeriod` at sign time — embedded on the wire so the receiver verifies non-interactively.
    */
  trait Publisher[F[_]] {
    def publish(
      senderPeerIdBytes: Array[Byte],
      metagraphAddress: String,
      parentHashBytes: Array[Byte],
      binaryHashBytes: Array[Byte],
      committeeVrfProof: Array[Byte],
      longTermSignature: Array[Byte],
      kesSignature: Array[Byte],
      vrfPublicKey: Array[Byte],
      kesStep: Int
    ): F[Unit]
  }

  /** Compute the bytes to be signed (long-term Ed25519) / hashed (canonical attestation hash). Pure — same input → same bytes — so sender
    * and receiver derive byte-identical messages. Routes through `Hasher[F]` to keep the serialization surface unified (no parallel
    * Blake2b/byte-concat surface; see `feedback_use_hasher_no_manual_serialize`).
    */
  def messageBytes[F[_]: Functor: Hasher](
    senderPeerId: PeerId,
    metagraphAddress: Address,
    parentHash: Hash,
    binaryHash: Hash
  ): F[Array[Byte]] = {
    val msg = AttestationMessage(senderPeerId.value.value, metagraphAddress, parentHash, binaryHash)
    Hasher[F].hash(msg).map(_.getBytes)
  }

  /** Construct a gate. Wires:
    *   - `sortition` for the VRF threshold check (sender) + membership verify (receiver),
    *   - `vrfRegistry` as the sole authority for operator VRF identities. The sender may self-count only when its configured VK exactly
    *     matches its registration and its proof verifies under that registered VK. The receiver rejects missing/mismatched registrations
    *     and never verifies under the wire-selected key,
    *   - `aggregator` for the per-binary tally + threshold polling + pruning,
    *   - `kesSigner` / `kesVerifier` for the REQUIRED KES product signature path. Sender queries `kesSigner.currentPeriod` for the
    *     tree-internal step it can sign at right now, signs at that step, and embeds the step on the wire (proto field `sender_tree_step`).
    *     The receiver verifies using the wire-carried step — no chain-state lookup, no offset arithmetic. This makes the verifier
    *     non-interactive (KES freshness/replay protection is provided separately by the `parentHash` + `binaryHash` fields and the
    *     per-binary aggregator dedup, not by step bookkeeping).
    *   - `publisher` for the sidecar `PublishMetagraphAttestation` RPC.
    *
    * '''eta is supplied by the caller — the gate no longer re-resolves the parent ordinal (#213/#290).''' Both `attestAndAdmit` (sender)
    * and `recordReceivedAttestation` (receiver) take `eta` as a parameter. The caller (the daemon / admission processor) resolves the
    * metagraph parent ordinal from the incoming binary's OWN content via `MetagraphParentOrdinalResolver.resolveFromBinary` and computes
    * the eta from it BEFORE invoking the gate — so the gate already receives the correct, content-derived, cross-node-deterministic eta.
    * The gate used to additionally re-resolve the ordinal via a GSI-only `parentOrdinalFor` (used only to gate `Some`/`None` + a log line;
    * the resolved value never fed the VRF/eta/signature). That GSI re-resolve returned `None` on the empty-currency-partition path — the
    * exact deadlock this removal fixes — vetoing a publish/record whose eta the caller had already computed correctly. Safety is unchanged:
    * the receiver's eta is derived from the binary's content (by the caller), so a forged ordinal yields a wrong eta and the committee-VRF
    * verify below rejects it.
    *
    * `selfPeerId`, `selfVrfSk`, `keyPair` are this operator's identity keys: VRF SK for the committee draw, long-term Ed25519 key
    * (`keyPair.getPrivate`) for the outer signature.
    */
  def make[F[_]: Async: SecurityProvider: Hasher: Logger: Metrics](
    selfPeerId: PeerId,
    selfVrfSk: Array[Byte],
    selfVrfVk: Array[Byte],
    keyPair: KeyPair,
    sortition: CommitteeSortition[F],
    vrfRegistry: VrfRegistry[F],
    aggregator: MetagraphAttestationAggregator[F],
    kesSigner: KesSigner[F],
    kesVerifier: KesVerifier[F],
    publisher: Publisher[F],
    kDraw: Int,
    kQuorum: Int,
    gateTimeoutMs: Long,
    pollIntervalMs: Long
  ): MetagraphCommitteeGate[F] =
    new MetagraphCommitteeGate[F] {

      private val logger: Logger[F] = Logger[F]

      def attestAndAdmit(
        metagraphAddress: Address,
        parentHash: Hash,
        binaryHash: Hash,
        eta: Array[Byte],
        sigmaOperatorKey: Ratio
      ): F[Boolean] =
        for {
          // Sender path — fire-and-forget the committee attestation if we're in this committee.
          // Wrapped in handleError so a sign/publish failure never blocks the gate poll loop.
          senderOutcome <- senderPath(metagraphAddress, parentHash, binaryHash, eta, sigmaOperatorKey).handleErrorWith { err =>
            logger
              .warn(s"⚠️ committee-attestation sender path failed for $metagraphAddress: ${err.getMessage}")
              .as(SenderOutcome.NotInCommittee: SenderOutcome)
          }
          _ <- emitSortitionSelfOutcome(senderOutcome)
          admitted <- waitForThreshold(metagraphAddress, parentHash, binaryHash)
          _ <- Metrics[F].incrementCounter(
            "dag_nakamoto_committee_admit_total",
            Seq(Metrics.unsafeLabelName("outcome") -> (if (admitted) "accepted" else "timeout"))
          )
        } yield admitted

      /** Bumps `dag_nakamoto_committee_sortition_self_outcome_total{outcome}` — one of `in_committee` / `not_in_committee` /
        * `unknown_parent`. Useful to confirm the per-operator sortition rate matches `K_target / N` over a long run (uniformity sanity
        * check).
        */
      private def emitSortitionSelfOutcome(outcome: SenderOutcome): F[Unit] = {
        val label = outcome match {
          case SenderOutcome.Attested(_, _)       => "in_committee"
          case SenderOutcome.NotInCommittee       => "not_in_committee"
          case SenderOutcome.InvalidVrfIdentity   => "invalid_vrf_identity"
          case SenderOutcome.UnknownParentOrdinal => "unknown_parent"
        }
        Metrics[F].incrementCounter(
          "dag_nakamoto_committee_sortition_self_outcome_total",
          Seq(Metrics.unsafeLabelName("outcome") -> label)
        )
      }

      private def senderPath(
        metagraphAddress: Address,
        parentHash: Hash,
        binaryHash: Hash,
        eta: Array[Byte],
        sigmaOperatorKey: Ratio
      ): F[SenderOutcome] =
        vrfRegistry.getVrfVk(selfPeerId).flatMap {
          case None =>
            logger
              .warn(s"committee-gate refusing self-attestation: no registered VRF key for ${selfPeerId.value.value.take(16)}...")
              .as(SenderOutcome.InvalidVrfIdentity: SenderOutcome)
          case Some(registeredVrfVk) if !java.security.MessageDigest.isEqual(registeredVrfVk, selfVrfVk) =>
            logger
              .warn(
                s"committee-gate refusing self-attestation: configured VRF key does not match registration for ${selfPeerId.value.value.take(16)}..."
              )
              .as(SenderOutcome.InvalidVrfIdentity: SenderOutcome)
          case Some(registeredVrfVk) =>
            sortition
              .isInCommittee(selfVrfSk, eta, metagraphAddress, parentHash, sigmaOperatorKey, kDraw)
              .flatMap {
                case None =>
                  logger
                    .debug(
                      s"⏭️ committee-gate self not-in-committee mg=$metagraphAddress parent=${parentHash.value.take(12)}... binary=${binaryHash.value
                          .take(12)}..."
                    )
                    .as(SenderOutcome.NotInCommittee: SenderOutcome)
                case Some((proof, output)) =>
                  // Do not self-count until the generated proof verifies under the registered
                  // identity. This catches a local SK/VK configuration mismatch before it creates
                  // a tally that every remote validator will reject.
                  sortition
                    .verifyMembershipDetailed(
                      registeredVrfVk,
                      eta,
                      metagraphAddress,
                      parentHash,
                      sigmaOperatorKey,
                      kDraw,
                      proof
                    )
                    .flatMap {
                      case CommitteeSortition.VerifyOutcome.Valid =>
                        publishSelfAttestation(
                          metagraphAddress,
                          parentHash,
                          binaryHash,
                          eta,
                          sigmaOperatorKey,
                          proof,
                          output,
                          registeredVrfVk
                        )
                      case invalid =>
                        logger
                          .warn(
                            s"committee-gate refusing self-attestation: proof does not verify under registered VRF key " +
                              s"for ${selfPeerId.value.value.take(16)}... reason=$invalid"
                          )
                          .as(SenderOutcome.InvalidVrfIdentity: SenderOutcome)
                    }
              }
        }

      private def publishSelfAttestation(
        metagraphAddress: Address,
        parentHash: Hash,
        binaryHash: Hash,
        eta: Array[Byte],
        sigmaOperatorKey: Ratio,
        proof: Array[Byte],
        output: Array[Byte],
        registeredVrfVk: Array[Byte]
      ): F[SenderOutcome] =
        for {
          _ <- aggregator.record(metagraphAddress, parentHash, binaryHash, selfPeerId)
          msgBytes <- messageBytes[F](selfPeerId, metagraphAddress, parentHash, binaryHash)
          edSig <- Signing.signData[F](msgBytes)(keyPair.getPrivate)
          // Sign at the CURRENT KES tree-internal step the in-memory key holds. Embedding
          // this step on the wire (proto `sender_tree_step`) lets the receiver verify
          // non-interactively — no chain-state lookup, no offset/eta-period derivation.
          kesStep <- kesSigner.currentPeriod
          kesSig <- kesSigner.signAt(kesStep, msgBytes)
          _ <- publisher.publish(
            senderPeerIdBytes = selfPeerId.value.toBytes,
            metagraphAddress = metagraphAddress.value.value,
            parentHashBytes = parentHash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            binaryHashBytes = binaryHash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            committeeVrfProof = proof,
            longTermSignature = edSig,
            kesSignature = kesSig,
            vrfPublicKey = registeredVrfVk,
            kesStep = kesStep
          )
          _ <- logger.info(
            s"📢 committee-attested mg=$metagraphAddress parent=${parentHash.value.take(12)}... binary=${binaryHash.value
                .take(12)}... kDraw=$kDraw kQuorum=$kQuorum kesStep=$kesStep " +
              s"sentEtaFull=${eta.map("%02x".format(_)).mkString} " +
              s"sentVrfVkFull=${registeredVrfVk.map("%02x".format(_)).mkString} " +
              s"sentProofFull=${proof.map("%02x".format(_)).mkString} " +
              s"sentSigma=$sigmaOperatorKey"
          )
        } yield SenderOutcome.Attested(proof, output): SenderOutcome

      private def waitForThreshold(
        metagraphAddress: Address,
        parentHash: Hash,
        binaryHash: Hash
      ): F[Boolean] =
        for {
          startMs <- Clock[F].realTime.map(_.toMillis)
          deadlineMs = startMs + gateTimeoutMs
          result <- pollUntil(metagraphAddress, parentHash, binaryHash, deadlineMs)
          _ <-
            if (result) Async[F].unit
            else
              logger.warn(
                s"⛔ committee-gate timeout mg=$metagraphAddress parent=${parentHash.value.take(12)}... binary=${binaryHash.value
                    .take(12)}... kDraw=$kDraw kQuorum=$kQuorum timeoutMs=$gateTimeoutMs — dropping binary"
              )
        } yield result

      private def pollUntil(
        metagraphAddress: Address,
        parentHash: Hash,
        binaryHash: Hash,
        deadlineMs: Long
      ): F[Boolean] =
        aggregator.thresholdReached(metagraphAddress, parentHash, binaryHash, kQuorum).flatMap { reached =>
          if (reached) {
            logger
              .info(
                s"✅ committee-gate threshold reached mg=$metagraphAddress parent=${parentHash.value.take(12)}... binary=${binaryHash.value
                    .take(12)}..."
              )
              .as(true)
          } else
            Clock[F].realTime.map(_.toMillis).flatMap { now =>
              if (now >= deadlineMs) Async[F].pure(false)
              else
                Temporal[F].sleep(pollIntervalMs.millis) >>
                  pollUntil(metagraphAddress, parentHash, binaryHash, deadlineMs)
            }
        }

      def recordReceivedAttestation(
        att: IncomingAttestation,
        eta: Array[Byte],
        lookupSenderStake: PeerId => F[Ratio]
      ): F[Unit] =
        // DEDUP-BEFORE-VERIFY: gossip re-delivers the same attestation constantly. Re-running the three
        // crypto verifies (Ed25519 + KES + VRF-membership) for a re-delivery of an ALREADY verified+recorded
        // (mg, parent, binary, sender) was CPU-saturating gl0 under multi-metagraph×sharding (load 156,
        // attestation-finality grinding to >70s/snapshot, step-like progression). The tally is unchanged by a
        // re-delivery, so this short-circuit is determinism-neutral. Safe: a NEW or forged attestation for an
        // un-recorded key still takes the full-verify path below (a forged sig fails there; equivocation has a
        // different binaryHash ⇒ different key ⇒ not deduped).
        aggregator.alreadyRecorded(att.metagraphAddress, att.parentHash, att.binaryHash, att.senderPeerId).flatMap {
          case true =>
            logger.debug(
              s"committee-attestation DEDUP (already recorded, skipped re-verify) from=${att.senderPeerId.value.value
                  .take(16)}... mg=${att.metagraphAddress} parent=${att.parentHash.value.take(12)}..."
            )
          case false =>
            verifyReceived(att, eta, lookupSenderStake).flatMap {
              case ReceiverOutcome.Recorded =>
                aggregator
                  .record(att.metagraphAddress, att.parentHash, att.binaryHash, att.senderPeerId)
                  .flatMap { count =>
                    logger.info(
                      s"📨 committee-attestation recorded from=${att.senderPeerId.value.value.take(16)}... mg=${att.metagraphAddress} parent=${att.parentHash.value
                          .take(12)}... count=$count"
                    )
                  }
              case other =>
                logger.warn(
                  s"⚠️ committee-attestation rejected (reason=$other) from=${att.senderPeerId.value.value
                      .take(16)}... mg=${att.metagraphAddress} parent=${att.parentHash.value.take(12)}..."
                )
            }
        }

      /** Pure-ish verifier used by `recordReceivedAttestation` — returns the `ReceiverOutcome` ADT so callers and tests can observe which
        * gate fired. The corresponding side-effect (record + log) is in the caller.
        */
      private def verifyReceived(
        att: IncomingAttestation,
        eta: Array[Byte],
        lookupSenderStake: PeerId => F[Ratio]
      ): F[ReceiverOutcome] =
        if (att.longTermSignature.isEmpty)
          Async[F].pure(ReceiverOutcome.EmptyLongTermSig: ReceiverOutcome)
        else if (att.kesSignature.isEmpty)
          Async[F].pure(ReceiverOutcome.EmptyKesSig: ReceiverOutcome)
        else
          // Verify directly against the caller-supplied `eta`. #213/#290: the gate no longer
          // re-resolves the metagraph parent ordinal here — the daemon resolves it from the
          // incoming binary's OWN content (`MetagraphParentOrdinalResolver.resolveFromBinary`)
          // and passes the content-derived eta in. The old GSI-only re-resolve returned `None`
          // on the empty-currency-partition path and dropped every attestation as
          // `UnknownParentOrdinal` (the receiver-side half of the deadlock). The resolved value
          // never affected verification — only the eta does, and that is now correct by
          // construction. Safety: a forged ordinal yields a wrong (content-derived) eta upstream,
          // so the committee-VRF verify below fails — the receiver trusts the binary's content,
          // never the sender's claimed ordinal.
          for {
            msgBytes <- messageBytes[F](att.senderPeerId, att.metagraphAddress, att.parentHash, att.binaryHash)
            senderPubKey <- att.senderPeerId.value.toPublicKey[F]
            edOk <- Signing.verifySignature[F](msgBytes, att.longTermSignature)(senderPubKey)
            outcome <-
              if (!edOk) Async[F].pure(ReceiverOutcome.InvalidLongTermSig: ReceiverOutcome)
              else
                vrfRegistry.getVrfVk(att.senderPeerId).flatMap {
                  case None =>
                    Async[F].pure(ReceiverOutcome.UnregisteredVrfIdentity: ReceiverOutcome)
                  case Some(registeredVrfVk) if !java.security.MessageDigest.isEqual(registeredVrfVk, att.senderVrfVk) =>
                    Async[F].pure(ReceiverOutcome.MismatchedVrfIdentity: ReceiverOutcome)
                  case Some(registeredVrfVk) =>
                    for {
                      kesOk <- kesVerifier.verify(
                        messageBytes = msgBytes,
                        kesSigBytes = att.kesSignature,
                        attesterId = att.senderPeerId,
                        attesterHex = att.senderPeerId.value,
                        kesStep = att.senderTreeStep
                      )
                      result <-
                        if (!kesOk) Async[F].pure(ReceiverOutcome.InvalidKesSig: ReceiverOutcome)
                        else
                          lookupSenderStake(att.senderPeerId).flatMap { sigmaSender =>
                            sortition
                              .verifyMembershipDetailed(
                                registeredVrfVk,
                                eta,
                                att.metagraphAddress,
                                att.parentHash,
                                sigmaSender,
                                kDraw,
                                att.committeeVrfProof
                              )
                              .flatMap {
                                case CommitteeSortition.VerifyOutcome.Valid =>
                                  Async[F].pure(ReceiverOutcome.Recorded: ReceiverOutcome)
                                case CommitteeSortition.VerifyOutcome.InvalidProof =>
                                  // The proof is checked against the registry key, never the in-band key.
                                  logger.warn(
                                    s"🔬 InvalidCommitteeVrf [InvalidProof] mg=${att.metagraphAddress} parent=${att.parentHash.value
                                        .take(12)}... binary=${att.binaryHash.value.take(12)}... " +
                                      s"etaFull=${eta.map("%02x".format(_)).mkString} " +
                                      s"registeredVrfVkFull=${registeredVrfVk.map("%02x".format(_)).mkString} " +
                                      s"proofFull=${att.committeeVrfProof.map("%02x".format(_)).mkString} " +
                                      s"sigmaSender=$sigmaSender kDraw=$kDraw from=${att.senderPeerId.value.value.take(16)}..."
                                  ) >>
                                    Async[F].pure(ReceiverOutcome.InvalidCommitteeVrf: ReceiverOutcome)
                                case CommitteeSortition.VerifyOutcome.BelowThreshold(testValue, thresh) =>
                                  logger.warn(
                                    s"🔬 InvalidCommitteeVrf [BelowThreshold] mg=${att.metagraphAddress} parent=${att.parentHash.value
                                        .take(12)}... binary=${att.binaryHash.value.take(12)}... " +
                                      s"testValue=$testValue threshold=$thresh " +
                                      s"sigmaSender=$sigmaSender kDraw=$kDraw from=${att.senderPeerId.value.value.take(16)}..."
                                  ) >>
                                    Async[F].pure(ReceiverOutcome.InvalidCommitteeVrf: ReceiverOutcome)
                              }
                          }
                    } yield result
                }
          } yield outcome

      def pruneParents(metagraphAddress: Address, parents: Set[Hash]): F[Unit] =
        aggregator.pruneParents(metagraphAddress, parents)
    }
}
