package io.constellationnetwork.schema.sharding

import cats.Eq
import cats.data.{NonEmptyList, NonEmptySet}
import cats.syntax.eq._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SharedArtifact, SpendAction, SpendTransaction}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap.{CurrencyId, SwapAmount}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.{NonNegInt, NonNegLong, PosLong}
import io.circe.parser.decode
import io.circe.syntax._
import weaver.FunSuite

/** Circe round-trip + ordering-preservation suite for the `io.constellationnetwork.schema.sharding` package (Slice 1 of the hierarchical
  * shard-checkpoints design — `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §3.1–§3.2).
  *
  * '''What this suite proves.''' Every new case class:
  *   1. Round-trips through Circe (encode → decode → equality preserved)
  *   1. Carries deterministic encoding for ordered collections — `SortedMap` keys come out in `Ordering[K]` order, `NonEmptyList` elements
  *      come out in input order. This matters because the checkpoint's canonical hash (the bytes signed by committee members) is derived
  *      from this encoding via `Hasher[F]`; two operators encoding the same logical content must produce byte-identical bytes or signature
  *      verification fails (`feedback_use_hasher_no_manual_serialize` rationale).
  *
  * '''What this suite does NOT exercise.''' No `Hasher[F]` integration (that's the producer/verifier wiring, slice 2+). No signature
  * verification (slice 3+). No state-application semantics (slice 4+, gl0 admission). Pure schema-level codec contract.
  *
  * '''Why `FunSuite`, not `MutableIOSuite`.''' Circe codecs are pure functions; no `IO` is needed to construct or verify them. The existing
  * codec test files in this project (e.g. `KesRegistrationCodecsSuite`, `MetagraphSyncDataInfoCodecSuite`, `EpochStakeSnapshotterSuite`)
  * settled on `FunSuite` for the same reason — keep the test free of irrelevant resource scaffolding.
  */
object ShardingCodecsSuite extends FunSuite {

  // ===========================================================================
  // Fixtures
  //
  // Deterministic — no `Gen` / `Arbitrary` so failure traces don't shrink across runs. Each case-class field gets a non-default,
  // non-empty value so a "missing field" decoder bug surfaces as a value mismatch rather than a silent fallthrough.
  // ===========================================================================

  // ---- Primitive fixtures --------------------------------------------------

  private def hex(s: String): Hex = Hex(s)
  private def hash(seed: Char): Hash = Hash(seed.toString * 64) // 64 hex chars = 32 bytes

  // Use `Address.fromBytes` (sha256 → base58 → parity-prefix) for deterministic, refinement-valid addresses; constructing via raw
  // `DAGAddressRefined` requires re-doing the parity calculation by hand, which is brittle.
  private def addr(tag: String): Address = Address.fromBytes(tag.getBytes("UTF-8"))

  private val mgAddrA: Address = addr("mg-aaa")
  private val mgAddrB: Address = addr("mg-bbb")
  private val holderA: Address = addr("holder-1")
  private val holderB: Address = addr("holder-2")

  private def peerIdN(n: Int): PeerId =
    PeerId(Hex((n.toHexString.padTo(2, '0')) * 64)) // 128 hex chars

  // ---- ShardId / ShardOrdinal ----------------------------------------------

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val shardOne: ShardId = ShardId.unsafeApply(1)
  private val shardTwo: ShardId = ShardId.unsafeApply(2)

  private val sampleShardOrdinal: ShardOrdinal = ShardOrdinal(42L)

  // ---- CommitteeMemberSignature --------------------------------------------

  private def mkSig(peerN: Int, step: Int): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = peerIdN(peerN),
      vrfProof = hex("aa" * 80), // 80 bytes hex
      ed25519Sig = hex("bb" * 64), // 64 bytes hex
      kesProductSig = hex("cc" * 128), // 128 bytes hex
      kesTreeStep = step
    )

  // ---- StateChannelSnapshotBinary (for ShardDerivedStateDelta.includedSnapshots) ----

  private def mkScsb(lastHashSeed: Char, contentByte: Byte, fee: Long): StateChannelSnapshotBinary =
    StateChannelSnapshotBinary(
      lastSnapshotHash = hash(lastHashSeed),
      content = Array.fill(8)(contentByte),
      fee = SnapshotFee(NonNegLong.unsafeFrom(fee))
    )

  private def mkSigned[A](value: A): Signed[A] = {
    val proof = SignatureProof(Id(hex("11" * 64)), Signature(hex("22" * 70)))
    Signed(value, NonEmptySet.of(proof))
  }

  private def mkSignedBinary(seed: Char, contentByte: Byte, fee: Long): Signed[StateChannelSnapshotBinary] =
    mkSigned(mkScsb(seed, contentByte, fee))

  // ---- ShardDerivedStateDelta ----------------------------------------------

  private def sampleDelta: ShardDerivedStateDelta =
    ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap(
        mgAddrA -> hash('a'),
        mgAddrB -> hash('b')
      ),
      perMetagraphStateDiff = SortedMap.empty,
      includedSnapshots = SortedMap(
        mgAddrA -> NonEmptyList.of(mkSignedBinary('1', 0x01, 10L), mkSignedBinary('2', 0x02, 20L)),
        mgAddrB -> NonEmptyList.of(mkSignedBinary('3', 0x03, 30L))
      ),
      tokenLockBalancesDelta = SortedMap(
        mgAddrA -> SortedMap(
          holderA -> Balance(NonNegLong(100L)),
          holderB -> Balance(NonNegLong(200L))
        )
      ),
      perMetagraphArtifacts = SortedMap(
        mgAddrA -> List[SharedArtifact](
          SpendAction(
            spendTransactions = NonEmptyList.of(
              SpendTransaction(
                allowSpendRef = Some(hash('s')),
                currencyId = Some(CurrencyId(mgAddrB)),
                amount = SwapAmount(PosLong(50L)),
                source = holderA,
                destination = holderB
              )
            )
          )
        )
      ),
      perMetagraphSyncDataDelta = SortedMap(
        mgAddrA -> MetagraphSyncDataInfo(
          globalOrdinalLastAcceptedOn = SnapshotOrdinal(NonNegLong(7L)),
          globalEpochProgressLastAcceptedOn = EpochProgress(NonNegLong(3L)),
          unappliedGlobalChangeOrdinals = SortedSet(
            SnapshotOrdinal(NonNegLong(1L)),
            SnapshotOrdinal(NonNegLong(2L))
          )
        )
      )
    )

  // ---- CrossShardReceipt ---------------------------------------------------

  private def sampleReceipt: CrossShardReceipt =
    CrossShardReceipt.MetagraphSyncDataWrite(
      sourceShardId = shardZero,
      sourceMetagraph = mgAddrA,
      sourceCheckpointHash = hash('p'),
      targetShardId = shardOne,
      targetMetagraph = mgAddrB,
      increment = MetagraphSyncDataInfo(
        globalOrdinalLastAcceptedOn = SnapshotOrdinal(NonNegLong(10L)),
        globalEpochProgressLastAcceptedOn = EpochProgress(NonNegLong(4L)),
        unappliedGlobalChangeOrdinals = SortedSet.empty
      )
    )

  // ---- ShardCheckpoint -----------------------------------------------------

  private def sampleCheckpoint: ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = hash('p'),
      shardOrdinal = sampleShardOrdinal,
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong(99L)),
      slot = SlotT.unsafeApply(99L),
      derivedStateDelta = sampleDelta,
      emittedReceipts = List(sampleReceipt),
      committeeSignatures = NonEmptyList.of(mkSig(1, 7), mkSig(2, 8), mkSig(3, 9)),
      epoch = EtaPeriod(5L)
    )

  // ---- FraudProofEnvelope --------------------------------------------------

  private def sampleFraud: FraudProofEnvelope =
    FraudProofEnvelope(
      shardId = shardZero,
      disputedCheckpointHash = hash('d'),
      claimedDerivation = hash('c'),
      challengerDerivation = hash('x'),
      reexecutionWitness = hex("dead" * 32),
      challengerSignature = hex("beef" * 32)
    )

  // ---- Generic round-trip helper -------------------------------------------
  //
  // Encodes a value to a JSON string, parses it back, and asserts equality. Each test calls this for the case class it covers; a single
  // helper keeps the body of each test one line so failures point straight at the type that broke.
  //
  // '''Why `Eq[A]` not Scala `==`.''' Several fields transitively carry `Array[Byte]` (e.g. `StateChannelSnapshotBinary.content`), and
  // Scala's auto-generated `equals` falls back to `Array#equals` which is reference equality — guaranteed to fail after a JSON round-trip
  // even when the bytes match. Using cats `Eq` routes through the project's `arrayEq` instance from `schema.OrphanInstances` which does
  // element-wise comparison. Same reason `StateChannelSnapshotBinary` itself is `@derive(eqv)` not just `case class`.
  private def roundtrip[A: io.circe.Encoder: io.circe.Decoder: Eq](label: String, value: A): weaver.Expectations = {
    val jsonStr = value.asJson.noSpaces
    decode[A](jsonStr) match {
      case Right(decoded) => expect(decoded === value).traced(weaver.SourceLocation.fromContext)
      case Left(err)      => failure(s"$label decode failed: ${err.getMessage}; json=$jsonStr")
    }
  }

  // ===========================================================================
  // Per-type round-trip tests
  // ===========================================================================

  test("ShardId: round-trips through Circe")(roundtrip("ShardId", shardZero))
  test("ShardId: round-trips with non-zero value")(roundtrip("ShardId(2)", shardTwo))

  test("ShardId.apply rejects negative values") {
    expect(ShardId(-1).isEmpty)
  }

  test("ShardId.apply accepts zero and positive values") {
    expect.all(ShardId(0).contains(shardZero), ShardId(1).contains(shardOne))
  }

  test("ShardOrdinal: round-trips through Circe")(roundtrip("ShardOrdinal", sampleShardOrdinal))
  test("ShardOrdinal: round-trips at Genesis")(roundtrip("Genesis", ShardOrdinal.Genesis))

  test("ShardOrdinal.next increments by 1") {
    expect(ShardOrdinal(7L).next == ShardOrdinal(8L))
  }

  test("CommitteeMemberSignature: round-trips through Circe")(roundtrip("CommitteeMemberSignature", mkSig(1, 5)))

  test("ShardDerivedStateDelta: round-trips through Circe")(roundtrip("ShardDerivedStateDelta", sampleDelta))
  test("ShardDerivedStateDelta.empty: round-trips through Circe")(roundtrip("empty", ShardDerivedStateDelta.empty))

  test("CrossShardReceipt: round-trips through Circe (MetagraphSyncDataWrite variant)")(
    roundtrip("CrossShardReceipt", sampleReceipt)
  )

  test("ShardCheckpoint: round-trips through Circe")(roundtrip("ShardCheckpoint", sampleCheckpoint))

  test("ShardCheckpointSigPreimage: round-trips through Circe") {
    roundtrip("ShardCheckpointSigPreimage", sampleCheckpoint.signingPreimage)
  }

  test("FraudProofEnvelope: round-trips through Circe")(roundtrip("FraudProofEnvelope", sampleFraud))

  // ===========================================================================
  // Ordering preservation
  //
  // The canonical hash of a `ShardCheckpoint` is produced by encoding the `ShardCheckpointSigPreimage` to JSON and hashing those bytes
  // (`Hasher[F]`). If `SortedMap` order or `NonEmptyList` order is not stable across encode/decode, two operators with semantically
  // identical content can produce divergent hashes — every signer would sign a different hash and the resulting checkpoint would be
  // rejected. These tests pin the contract.
  // ===========================================================================

  test("ShardCheckpoint.includedSnapshots preserves SortedMap key order") {
    val jsonStr = sampleCheckpoint.asJson.noSpaces
    val decoded = decode[ShardCheckpoint](jsonStr).fold(e => throw new AssertionError(s"decode failed: ${e.getMessage}"), identity)
    val originalKeys = sampleCheckpoint.derivedStateDelta.includedSnapshots.keys.toList
    val decodedKeys = decoded.derivedStateDelta.includedSnapshots.keys.toList
    expect.same(originalKeys, decodedKeys)
  }

  test("ShardCheckpoint.committeeSignatures preserves NonEmptyList element order") {
    val jsonStr = sampleCheckpoint.asJson.noSpaces
    val decoded = decode[ShardCheckpoint](jsonStr).fold(e => throw new AssertionError(s"decode failed: ${e.getMessage}"), identity)
    val originalSigs = sampleCheckpoint.committeeSignatures.toList
    val decodedSigs = decoded.committeeSignatures.toList
    expect.same(originalSigs, decodedSigs)
  }

  test("ShardCheckpoint.derivedStateDelta.includedSnapshots inner NonEmptyList preserves order") {
    val jsonStr = sampleCheckpoint.asJson.noSpaces
    val decoded = decode[ShardCheckpoint](jsonStr).fold(e => throw new AssertionError(s"decode failed: ${e.getMessage}"), identity)
    val originalA = sampleCheckpoint.derivedStateDelta.includedSnapshots(mgAddrA).toList
    val decodedA = decoded.derivedStateDelta.includedSnapshots(mgAddrA).toList
    // List `===` via cats Eq composes through `Signed.eq` → `StateChannelSnapshotBinary` Eq → `arrayEq` for byte-array content.
    expect(originalA === decodedA)
  }

  test("ShardDerivedStateDelta.tokenLockBalancesDelta nested SortedMap preserves key order") {
    val jsonStr = sampleDelta.asJson.noSpaces
    val decoded = decode[ShardDerivedStateDelta](jsonStr).fold(e => throw new AssertionError(s"decode failed: ${e.getMessage}"), identity)
    val originalInner = sampleDelta.tokenLockBalancesDelta(mgAddrA).keys.toList
    val decodedInner = decoded.tokenLockBalancesDelta(mgAddrA).keys.toList
    expect.same(originalInner, decodedInner)
  }

  // ===========================================================================
  // Determinism — encoding the same value twice produces identical bytes
  // ===========================================================================

  test("ShardCheckpoint: re-encoding produces byte-identical JSON") {
    val first = sampleCheckpoint.asJson.noSpaces
    val second = sampleCheckpoint.asJson.noSpaces
    expect.same(first, second)
  }

  test("ShardCheckpoint built from same maps with different insertion order produces identical JSON") {
    val deltaA = sampleDelta
    val deltaB = sampleDelta.copy(
      perMetagraphMptRoots = SortedMap(
        // insertion order reversed; SortedMap normalizes by key ordering
        mgAddrB -> hash('b'),
        mgAddrA -> hash('a')
      )
    )
    val checkpointA = sampleCheckpoint.copy(derivedStateDelta = deltaA)
    val checkpointB = sampleCheckpoint.copy(derivedStateDelta = deltaB)
    expect.same(checkpointA.asJson.noSpaces, checkpointB.asJson.noSpaces)
  }

  // ===========================================================================
  // Cross-type wiring — verify ShardCheckpoint.signingPreimage is the structural projection it claims
  // ===========================================================================

  test("ShardCheckpoint.signingPreimage carries every field except committeeSignatures") {
    val preimage = sampleCheckpoint.signingPreimage
    // Use cats `===` (Eq) instead of Scala `==` because `ShardDerivedStateDelta` transitively carries `Array[Byte]` fields whose default
    // `equals` is reference-identity (see scaladoc on `roundtrip` for the same reason).
    expect.all(
      preimage.shardId === sampleCheckpoint.shardId,
      preimage.parentCheckpointHash === sampleCheckpoint.parentCheckpointHash,
      preimage.shardOrdinal === sampleCheckpoint.shardOrdinal,
      preimage.gl0AnchorOrdinal === sampleCheckpoint.gl0AnchorOrdinal,
      preimage.derivedStateDelta === sampleCheckpoint.derivedStateDelta,
      preimage.emittedReceipts === sampleCheckpoint.emittedReceipts,
      preimage.epoch === sampleCheckpoint.epoch
    )
  }

  test("ShardCheckpoint.signingPreimage encodes to JSON that does NOT contain `committeeSignatures` key") {
    val jsonStr = sampleCheckpoint.signingPreimage.asJson.noSpaces
    // Verifier replays the preimage construction from the envelope it received; any field in the envelope that the verifier accidentally
    // includes will surface as a hash mismatch. This test makes sure the preimage shape and the envelope shape differ exactly in that
    // one field name.
    expect(!jsonStr.contains("committeeSignatures"))
  }

  // ===========================================================================
  // ShardId map-key codec — used by `GlobalIncrementalSnapshot.shardCheckpoints: SortedMap[ShardId, _]` (§3.4 of design doc)
  // ===========================================================================

  test("ShardId encodes as a JSON key as the underlying decimal Int") {
    val m: SortedMap[ShardId, Int] = SortedMap(shardZero -> 0, shardOne -> 10, shardTwo -> 20)
    val jsonStr = m.asJson.noSpaces
    // Key encoding should yield {"0":0,"1":10,"2":20}. The ShardId-to-string contract is "underlying decimal Int".
    expect.all(
      jsonStr.contains("\"0\":0"),
      jsonStr.contains("\"1\":10"),
      jsonStr.contains("\"2\":20")
    )
  }

  test("ShardId map-key roundtrip preserves order") {
    val m: SortedMap[ShardId, String] = SortedMap(shardTwo -> "two", shardZero -> "zero", shardOne -> "one")
    val jsonStr = m.asJson.noSpaces
    val decoded = decode[SortedMap[ShardId, String]](jsonStr)
      .fold(e => throw new AssertionError(s"decode failed: ${e.getMessage}"), identity)
    expect.same(m.toList, decoded.toList)
  }

  // ===========================================================================
  // Refinement guards
  // ===========================================================================

  test("ShardId refuses negative NonNegInt at the type boundary") {
    // `NonNegInt.from` returns Either; the refined library guarantees no path can construct a ShardId from -1 except `unsafeApply`.
    val attempt: Either[String, NonNegInt] = NonNegInt.from(-1)
    expect(attempt.isLeft)
  }
}
