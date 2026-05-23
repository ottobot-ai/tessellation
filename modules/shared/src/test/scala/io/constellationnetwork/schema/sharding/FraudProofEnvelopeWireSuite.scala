package io.constellationnetwork.schema.sharding

import cats.syntax.eq._

import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import io.circe.parser.{decode, parse}
import io.circe.syntax._
import weaver.FunSuite

/** Wire-format reservation suite for [[FraudProofEnvelope]] — slice 18 of the hierarchical shard-checkpoints design
  * (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §3.2 + §11.3).
  *
  * '''What this suite proves and why it exists.''' Slice 1 landed [[FraudProofEnvelope]] and a baseline round-trip test in
  * [[ShardingCodecsSuite]]. The dispute handler is v2; the schema is in v1 so that snapshot bytes and slashing-tx bytes already encode the
  * envelope before the handler ships (§11.3). This suite locks the wire-format contract beyond the baseline round-trip:
  *
  *   1. '''Required-field discipline.''' Removing any field from the JSON makes decode fail. If a future Circe-derived encoder silently
  *      defaulted a missing field (e.g. via an inadvertent `Option` lift), v2's dispute handler would accept incomplete envelopes from
  *      malformed challengers and either crash or — worse — apply a partial proof. Pinning the decoder's strictness now means v2 can rely
  *      on the parsed `FraudProofEnvelope` being structurally complete.
  *   1. '''Deterministic encoding.''' The challenger's `challengerSignature` is computed over the JSON-encoded preimage of the envelope's
  *      other fields. If two encodings of the same envelope produced different bytes (e.g. because the encoder happened to randomise field
  *      order), every challenger would sign a unique byte sequence and verifiers couldn't reproduce the signed bytes. Pinning byte
  *      equality of two successive encodes makes this assumption an enforced contract instead of folklore.
  *   1. '''Empty + large `reexecutionWitness`.''' v1 carries `Array.empty` witness bytes (§11.3 — challenger construction lives in v2);
  *      v2 will carry per-derivation witnesses that can be sizeable. The `Hex`-backed wire shape needs to handle both ends without
  *      surprise — a zero-length string should encode and decode cleanly (no edge-case crash), and a witness on the order of 10 KB should
  *      round-trip identically (no implicit truncation, no buffer-limit collision).
  *
  * '''What this suite does NOT exercise.''' The basic happy-path round-trip is already covered by
  * `ShardingCodecsSuite.test("FraudProofEnvelope: round-trips through Circe")` — see the "Optional addendum" guidance in the slice 18
  * brief. This file adds the missing-field, determinism, and witness-extremes cases without duplicating the baseline.
  *
  * '''Why `FunSuite`.''' Same rationale as [[ShardingCodecsSuite]]: Circe codecs are pure functions; no `IO` scaffolding is needed.
  */
object FraudProofEnvelopeWireSuite extends FunSuite {

  // ===========================================================================
  // Fixtures — deterministic, distinct field values
  //
  // Distinct values per field surface a "field swap" bug as a value mismatch rather than silent success. Mirrors the
  // `ShardingCodecsSuite` style so reviewers can read the two suites together without context-switch friction.
  // ===========================================================================

  private def hex(s: String): Hex = Hex(s)
  private def hash(seed: Char): Hash = Hash(seed.toString * 64) // 64 hex chars = 32 bytes

  private val sampleEnvelope: FraudProofEnvelope =
    FraudProofEnvelope(
      shardId = ShardId.unsafeApply(3),
      disputedCheckpointHash = hash('d'),
      claimedDerivation = hash('c'),
      challengerDerivation = hash('x'),
      reexecutionWitness = hex("dead" * 32),
      challengerSignature = hex("beef" * 32)
    )

  // The set of required JSON keys for [[FraudProofEnvelope]]. Pinned here (rather than read from the encoder's output) so that a typo or
  // field rename in [[FraudProofEnvelope]] surfaces as a clear test failure naming the missing key — without this list the missing-field
  // tests would silently skip the field.
  private val requiredFields: List[String] = List(
    "shardId",
    "disputedCheckpointHash",
    "claimedDerivation",
    "challengerDerivation",
    "reexecutionWitness",
    "challengerSignature"
  )

  // ===========================================================================
  // Required-field discipline — every field must be present for decode to succeed
  // ===========================================================================

  // Encode the envelope, drop one field at a time from the JSON object, and assert that the partial object fails to decode. Using a single
  // loop wrapped in one `test` keeps the failure trace pointing at the specific field that was tolerated.
  test("FraudProofEnvelope: decoder rejects JSON missing any required field") {
    val baseJson = sampleEnvelope.asJson
    val baseObj = baseJson.asObject.getOrElse(throw new AssertionError(s"sample envelope did not encode as JSON object; got $baseJson"))

    // Sanity: the encoder produces exactly the fields we expect. If a field is renamed and `requiredFields` is not updated, this catches
    // the drift in one place — and surfaces as a clearer failure than "decoder unexpectedly succeeded".
    val encodedKeys = baseObj.keys.toSet
    val sanity =
      expect(encodedKeys == requiredFields.toSet)
        .traced(weaver.SourceLocation.fromContext)

    val perFieldChecks = requiredFields.map { field =>
      val pruned = baseObj.remove(field).asJson.noSpaces
      val result = decode[FraudProofEnvelope](pruned)
      // A missing required field surfaces as a circe `DecodingFailure`; we don't pin the exact message (would couple the test to circe's
      // formatting) but we do require `Left`.
      expect(result.isLeft).traced(weaver.SourceLocation.fromContext)
    }

    perFieldChecks.foldLeft(sanity)(_ and _)
  }

  // ===========================================================================
  // Encoding stability — same value, two encodes, byte-identical bytes
  //
  // Slice 1's `ShardCheckpoint: re-encoding produces byte-identical JSON` test covers `ShardCheckpoint`. We assert the same property
  // independently for `FraudProofEnvelope` because the dispute handler in v2 will recompute the bytes signed by `challengerSignature` from
  // a re-encode; non-determinism here would silently break signature verification at the v2 boundary.
  // ===========================================================================

  test("FraudProofEnvelope: re-encoding produces byte-identical JSON") {
    val first = sampleEnvelope.asJson.noSpaces
    val second = sampleEnvelope.asJson.noSpaces
    expect.same(first, second)
  }

  test("FraudProofEnvelope: encoding stability holds across noSpaces and spaces2") {
    // Same logical bytes, two pretty-printer modes; serialise twice in each mode and assert byte equality within mode. Catches a
    // hypothetical encoder bug that introduces nondeterminism only under one printer (e.g. via implicit ordering surprises).
    val n1 = sampleEnvelope.asJson.noSpaces
    val n2 = sampleEnvelope.asJson.noSpaces
    val p1 = sampleEnvelope.asJson.spaces2
    val p2 = sampleEnvelope.asJson.spaces2
    expect.all(n1 == n2, p1 == p2)
  }

  // ===========================================================================
  // Empty `reexecutionWitness` — v1 chain state carries empty witnesses (§11.3)
  // ===========================================================================

  test("FraudProofEnvelope: round-trips with empty reexecutionWitness") {
    val empty = sampleEnvelope.copy(reexecutionWitness = Hex(""))
    val jsonStr = empty.asJson.noSpaces
    decode[FraudProofEnvelope](jsonStr) match {
      case Right(decoded) =>
        // Cats `===` (`Eq[FraudProofEnvelope]` from `derevo.cats.eqv`) is the correct comparison for any envelope; the `Hex` field's
        // underlying `String` `equals` would also work here, but using `===` keeps the test consistent with the rest of the suite and
        // future-proofs against any field gaining `Array[Byte]` substructure.
        expect.all(
          decoded === empty,
          decoded.reexecutionWitness.value.isEmpty,
          decoded.reexecutionWitness.toBytes.isEmpty
        )
      case Left(err) => failure(s"empty-witness decode failed: ${err.getMessage}; json=$jsonStr")
    }
  }

  test("FraudProofEnvelope: empty-witness JSON carries reexecutionWitness as an empty string") {
    val empty = sampleEnvelope.copy(reexecutionWitness = Hex(""))
    val jsonStr = empty.asJson.noSpaces
    // Pin the wire shape: `Hex("")` encodes to `""`, not omitted, not `null`. This matters because the v2 dispute handler will likely
    // distinguish "no witness submitted" (empty bytes) from "no field" (missing) — pinning the empty-string form now keeps the v2 contract
    // unambiguous.
    expect(jsonStr.contains("\"reexecutionWitness\":\"\""))
  }

  // ===========================================================================
  // Large `reexecutionWitness` — v2 witnesses can carry sizeable proof bytes (§11.3)
  //
  // ~10 KB of witness bytes — 10240 bytes = 20480 hex chars. Catches any inadvertent buffer-size limit baked into a downstream codec layer
  // (e.g. a fixed-size Hex arbitrary, a JSON parser truncation, etc.).
  // ===========================================================================

  test("FraudProofEnvelope: round-trips with ~10 KB reexecutionWitness") {
    val tenKbBytes = 10 * 1024
    val witnessHex = Hex.fromBytes(Array.fill[Byte](tenKbBytes)(0x5a.toByte))
    val large = sampleEnvelope.copy(reexecutionWitness = witnessHex)

    val jsonStr = large.asJson.noSpaces
    decode[FraudProofEnvelope](jsonStr) match {
      case Right(decoded) =>
        expect.all(
          decoded === large,
          decoded.reexecutionWitness.value.length == tenKbBytes * 2, // each byte → 2 hex chars
          decoded.reexecutionWitness.toBytes.length == tenKbBytes
        )
      case Left(err) => failure(s"large-witness decode failed: ${err.getMessage}; json snippet=${jsonStr.take(120)}…")
    }
  }

  test("FraudProofEnvelope: large-witness encoding is structurally well-formed JSON") {
    // Cheap belt-and-braces: re-parse the JSON via `io.circe.parser.parse` (not the type-directed `decode`) to confirm the encoder
    // produced syntactically valid JSON even at ~10 KB witness sizes. This separates "the witness round-tripped to bytes" from
    // "the surrounding envelope is still a valid JSON document".
    val tenKbBytes = 10 * 1024
    val witnessHex = Hex.fromBytes(Array.fill[Byte](tenKbBytes)(0xa5.toByte))
    val large = sampleEnvelope.copy(reexecutionWitness = witnessHex)
    val jsonStr = large.asJson.noSpaces
    expect(parse(jsonStr).isRight)
  }
}
