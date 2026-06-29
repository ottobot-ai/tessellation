package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.effect.Sync
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardId, ShardOrdinal}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

/** Per-shard slot leader election within a shard committee (hierarchical-shard-checkpoints v1, §5.3 + §5.6).
  *
  * Reuses [[EligibilityChecker.checkEligibility]] — the existing Taktikos LDD-snowplow VRF eligibility primitive — scoped to a shard
  * committee rather than to the whole gl0 operator set. The only per-shard differences are:
  *
  *   1. '''Eta is shard-scoped.''' We do NOT feed gl0's `eta` straight in; instead we derive `shardEta =
  *      Hasher.hash(ShardEtaInput("shard-eta", shardId, gl0Eta))`. Domain-separated so a per-shard leader VRF can't leak gl0 leader
  *      eligibility and so two shards electing at the same slot+gl0-eta produce independent draws (§5.6). 2. '''Relative stake is
  *      in-committee.''' σ_i is the operator's stake share within the committee (uniform `1 / K_S` per v1 stable-σ rule from
  *      `[[project-216-committee-stake-drift-fix]]`), NOT their share of the whole gl0 active set.
  *
  * '''Determinism.''' Everything routes through `Hasher[F]` for hashing and `EligibilityChecker` for threshold arithmetic — no hand-rolled
  * Blake2b, no Double, no second serialization surface. Two honest nodes given the same `(shardId, gl0Eta, slot, slotGap, σ, lddConfig)`
  * produce byte-equivalent VRF inputs and byte-equivalent thresholds.
  *
  * '''Why this trait wraps `checkEligibility` rather than re-implementing the primitive.''' The hard rule from
  * `[[feedback-use-hasher-no-manual-serialize]]` and the design constraint in §5.3 is "reuse the existing LDD eligibility check". This
  * trait is the eta-derivation + signature-shape boundary between the per-shard slot-leader call site and the underlying primitive —
  * nothing more.
  *
  * '''Why `slotGap` is in the signature.''' The design doc §5.3 reference snippet passes `slotGap` to `checkEligibility`, but the slice 7
  * task's `isLeader` sketch omits it. LDD's threshold is gap-conditioned (the snowplow ramp drives `f(δ)`); omitting `slotGap` would change
  * the protocol. We accept it explicitly so each call site can pass `currentSlot - lastShardTipSlot` (the shard-chain analogue of the gl0
  * slotGap), and document the deviation here rather than silently fixing it inside the wrapper.
  */
trait ShardSlotLeader[F[_]] {

  /** Compute `shardEta` for `(shardId, gl0Eta)`. Hash routes through `Hasher[F]`; result is the 32 raw SHA-256 digest bytes of the
    * canonical JSON encoding of [[ShardSlotLeader.ShardEtaInput]] — matching the byte shape `EligibilityChecker.checkEligibility` requires
    * for its `eta` argument (`vrfProofForSlot` asserts `eta.length == 32`).
    *
    * `gl0Eta` MUST be 32 bytes (gl0 epoch randomness); enforced at the call boundary.
    */
  def computeShardEta(shardId: ShardId, gl0Eta: Array[Byte])(implicit hasher: Hasher[F]): F[Array[Byte]]

  /** Per-slot leader eligibility check. Returns `Some(proof, vrfOutput)` if this operator wins the slot lottery for `(shardEta, slot,
    * σ_in_committee)`, `None` otherwise.
    *
    * Delegates threshold arithmetic to [[EligibilityChecker.checkEligibility]] verbatim — same LDD snowplow `1 - (1 - f(δ))^σ`, same
    * `Ratio` arithmetic, same byte-equivalent behavior across JVMs.
    */
  def isLeader(
    vrfSk: Array[Byte],
    shardEta: Array[Byte],
    slot: Slot,
    slotGap: Long,
    sigmaInCommittee: Ratio,
    lddConfig: LddConfig
  ): F[Option[(Array[Byte], Array[Byte])]]

  /** Verifier path. Given a claimed leader's VRF proof, confirms both (a) the proof cryptographically verifies under `vrfVk` for the
    * canonical `(shardEta, slot)` message AND (b) the VRF output falls below the LDD threshold for the gap-conditioned `(σ_in_committee,
    * lddConfig)`. Delegates to [[EligibilityChecker.verifyEligibility]] verbatim.
    */
  def verifyLeader(
    vrfVk: Array[Byte],
    shardEta: Array[Byte],
    slot: Slot,
    slotGap: Long,
    sigmaInCommittee: Ratio,
    lddConfig: LddConfig,
    proof: Array[Byte]
  ): F[Boolean]

  /** Shuffled-staircase duty order (owner, 2026-06-12 — replaces the per-slot LDD lottery for shard-checkpoint PRODUCTION; design §5.7 rev
    * 2). Returns the committee deterministically ordered for `(shardEta, shardOrdinal)`: rank r proposes during slot window `[parentSlot +
    * 1 + r·δ, parentSlot + 1 + (r+1)·δ)`, wrapping modulo the committee size so liveness needs only ONE live member.
    *
    * The ordering is a hash-sort: each member's position is `Hasher[F](StaircaseRankInput(tag, shardEta, shardOrdinal, peerId))`, sorted
    * lexicographically. Pure function of `(eta, ordinal, membership)` — every committee member and every verifier derives the identical
    * order, and the eta (frozen at the prior period's 2/3-mark) makes it unbiasable by the participants. Run-13/14 evidence for WHY a
    * lottery cannot work here: at per-slot draws a small committee forks at genesis (everyone is instantly eligible at unbounded gap) and
    * siblings under any quorum lag — duty assignment is the only shape with a UNIQUE producer per window.
    */
  def dutyOrder(
    committee: List[PeerId],
    shardEta: Array[Byte],
    shardOrdinal: ShardOrdinal
  )(implicit hasher: Hasher[F]): F[List[PeerId]]

  /** VRF membership proof over `(shardEta, slot)` — the SAME primitive the attestation emitter uses (`vrfProofForSlot`). Carried on the
    * produced envelope's `CommitteeMemberSignature.vrfProof`. NOT a lottery-win claim under the staircase — it proves the producer
    * evaluated the VRF at this slot under this eta (structural check at Slice 9; Slice 13 elevates verification).
    */
  def membershipProof(vrfSk: Array[Byte], shardEta: Array[Byte], slot: Slot): F[Array[Byte]]
}

object ShardSlotLeader {

  /** Domain-separation tag carried in-band on the encoded `ShardEtaInput`. Must NOT collide with any other VRF or eta-derivation tag in the
    * codebase — keeps per-shard leader VRF eta independent of `CommitteeSortition`'s `"committee"` tag and of gl0's leader-VRF (which has
    * no tagged eta input, just `eta || slot`). Mirrors the `CommitteeSortition.CommitteeVrfInput.tag` pattern.
    */
  private val DomainTag: String = "shard-eta"

  /** Expected gl0 eta length in bytes — matches `EligibilityChecker.vrfProofForSlot` and `CommitteeSortition.message` invariants. We
    * `require` this at the boundary so a wrong-shape gl0 eta fails fast rather than silently shifting the shard VRF domain.
    */
  val EtaLength: Int = 32

  /** Canonical hash input for the per-shard eta derivation. The `tag` field provides domain-separation; `shardId` and `gl0Eta` are the two
    * pieces of per-shard identity. Encoded via the standard `Hasher[F]` surface so the resulting bytes are byte-equivalent across the
    * JVM-side and any cross-implementation verifier (e.g. the Go sidecar).
    *
    * '''Encoding shape.''' `gl0Eta` is the 32-byte epoch randomness rendered as a 64-char hex string (`Hex`), matching
    * `CommitteeSortition.CommitteeVrfInput`'s encoding of its `eta` field. `shardId` encodes as a non-negative integer via its derived
    * `Encoder[ShardId]` (slice 1: plain JSON number).
    */
  final case class ShardEtaInput(
    tag: String,
    shardId: ShardId,
    gl0Eta: Hex
  )

  object ShardEtaInput {
    implicit val encoder: Encoder[ShardEtaInput] = deriveEncoder
  }

  /** Standalone `(shardId, gl0Eta) => shardEta` derivation — the SINGLE byte-definition of the per-shard leader-VRF eta, shared by the
    * [[ShardSlotLeader]] instance method [[ShardSlotLeader.computeShardEta]] AND by any consensus-critical verifier that needs the eta
    * WITHOUT an [[EligibilityChecker]] in scope (e.g.
    * [[io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointGl0AcceptanceManager]]'s real
    * committee-VRF membership verify, wired from
    * [[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.acceptanceDeps]]). Co-locating the derivation here
    * — instead of duplicating the `ShardEtaInput` hashing at the verifier — guarantees the producer's
    * `ShardCheckpointAttestationEmitter.shardEtaFor` and the verifier resolve byte-identical eta bytes (the determinism invariant: a
    * divergent shardEta forks the committee-VRF verify).
    *
    * `gl0Eta` MUST be 32 bytes (gl0 epoch randomness); enforced here so a wrong-shape eta fails fast rather than silently shifting the
    * shard VRF domain. Returns the 32 raw SHA-256 digest bytes of the canonical JSON encoding of [[ShardEtaInput]] (decode of the hex hash,
    * matching the byte shape `EligibilityChecker.vrfProofForSlot` requires).
    */
  def computeShardEta[F[_]: Sync](shardId: ShardId, gl0Eta: Array[Byte])(implicit hasher: Hasher[F]): F[Array[Byte]] =
    Sync[F].delay {
      require(gl0Eta.length == EtaLength, s"gl0Eta must be $EtaLength bytes, got ${gl0Eta.length}")
      ShardEtaInput(DomainTag, shardId, Hex.fromBytes(gl0Eta))
    }.flatMap { input =>
      hasher.hash(input).map(h => Hex(h.value).toBytes)
    }

  /** Domain-separation tag for the staircase rank hash — distinct from [[DomainTag]] and `CommitteeSortition`'s `"committee"`. */
  private val StaircaseTag: String = "shard-staircase-rank"

  /** Canonical hash input for a member's staircase rank at one shard ordinal. `shardEta` as 64-char hex (same convention as
    * [[ShardEtaInput]]); rank = lexicographic order of the resulting hashes. Frozen-shape consensus contract (same discipline as
    * [[io.constellationnetwork.schema.sharding.ShardCheckpointSigPreimage]]).
    */
  final case class StaircaseRankInput(
    tag: String,
    shardEta: Hex,
    shardOrdinal: ShardOrdinal,
    peerId: PeerId
  )

  object StaircaseRankInput {
    implicit val encoder: Encoder[StaircaseRankInput] = deriveEncoder
  }

  /** Build a [[ShardSlotLeader]] backed by the supplied [[EligibilityChecker]]. Callers MUST pass the same `EligibilityChecker[F]` instance
    * the rest of the consensus path is using (so the per-shard slot-leader threshold arithmetic is bit-for-bit identical with the gl0 path
    * — same `Log1p`/`Exp` precision and same Bifrost continued-fraction interpreters).
    *
    * Eta-derivation goes through `Hasher[F]`; VRF + threshold delegate to `eligibilityChecker` directly.
    */
  def make[F[_]: Sync](eligibilityChecker: EligibilityChecker[F]): ShardSlotLeader[F] = new ShardSlotLeader[F] {

    def computeShardEta(shardId: ShardId, gl0Eta: Array[Byte])(implicit hasher: Hasher[F]): F[Array[Byte]] =
      // Delegates to the standalone `ShardSlotLeader.computeShardEta` so the producer (this instance method, via the attestation
      // emitter) and the consensus-critical committee-VRF verifier (which calls the static helper directly, without an
      // EligibilityChecker in scope) derive byte-identical eta bytes from the SAME single definition. `Sync[F].delay` lifts the eager
      // length `require` into the F context so a wrong-shape eta is recoverable via `.attempt` rather than escaping the suspension.
      ShardSlotLeader.computeShardEta[F](shardId, gl0Eta)

    def isLeader(
      vrfSk: Array[Byte],
      shardEta: Array[Byte],
      slot: Slot,
      slotGap: Long,
      sigmaInCommittee: Ratio,
      lddConfig: LddConfig
    ): F[Option[(Array[Byte], Array[Byte])]] =
      eligibilityChecker.checkEligibility(vrfSk, slot, slotGap, shardEta, sigmaInCommittee, lddConfig)

    def verifyLeader(
      vrfVk: Array[Byte],
      shardEta: Array[Byte],
      slot: Slot,
      slotGap: Long,
      sigmaInCommittee: Ratio,
      lddConfig: LddConfig,
      proof: Array[Byte]
    ): F[Boolean] =
      eligibilityChecker.verifyEligibility(vrfVk, slot, slotGap, shardEta, sigmaInCommittee, lddConfig, proof)

    def dutyOrder(
      committee: List[PeerId],
      shardEta: Array[Byte],
      shardOrdinal: ShardOrdinal
    )(implicit hasher: Hasher[F]): F[List[PeerId]] =
      committee.traverse { p =>
        hasher.hash(StaircaseRankInput(StaircaseTag, Hex.fromBytes(shardEta), shardOrdinal, p)).map(h => (h.value, p))
      }
        // Lexicographic sort on the canonical hex hash; ties impossible (distinct peerIds hash distinctly modulo SHA-256
        // collisions). Deterministic across JVMs: same Hasher, same String ordering.
        .map(_.sortBy(_._1).map(_._2))

    def membershipProof(vrfSk: Array[Byte], shardEta: Array[Byte], slot: Slot): F[Array[Byte]] =
      Sync[F].delay(eligibilityChecker.vrfProofForSlot(vrfSk, slot, shardEta))
  }
}
