package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.address.{Address, DAGAddress, DAGAddressRefined}
import io.constellationnetwork.serde.ImmutableCodec

import eu.timepit.refined.refineV
import scodec.codecs.{ascii, uint8, variableSizeBytes}
import scodec.{Attempt, Codec, Err}

/** Canonical scodec codec for `Address` — the network's refined-string wrapper
  * carrying a non-trivial validator (`DAGAddressRefined`: length=40, `DAG`
  * prefix, digit-sum parity, base58-safe suffix, plus the Stardust Collective
  * special case at 51 chars).
  *
  * Wire format: 1-byte length prefix + ASCII payload.
  *   - Normal DAG address (40 chars): 1 + 40 = 41 bytes.
  *   - Stardust Collective (51 chars): 1 + 51 = 52 bytes.
  *
  * Why variable-length and not fixed-width 64-pad-NUL? Length-prefix is the
  * standard functional idiom (scodec's `variableSizeBytes`) and keeps decoded
  * bytes ≡ encoded bytes byte-for-byte — no padding convention to remember.
  * Random-access into a compound record that contains an Address works by
  * reading the length prefix and skipping, same as any length-prefixed
  * structure. For a Balance map (`Address → Balance`) the Address is the MPT
  * key, not a sub-field — random access within the leaf value (Balance) is
  * unaffected.
  *
  * Why not a generic `RefinedStringNewtype` shape derivation? `DAGAddressRefined`
  * is non-trivial: 40-char length, DAG prefix, digit-sum parity, base58 safety,
  * plus a whitelisted exception. A naive "newtype over refined string" helper
  * would bypass the validator on decode and let arbitrary 40-byte sequences
  * become `Address` values — a consensus-safety hole. Hand-coded here; if a
  * second consensus type with a similarly complex refiner appears, extract a
  * shape then.
  *
  * On decode, the raw bytes are parsed as ASCII then routed through
  * `refineV[DAGAddressRefined]`. A byte sequence that decodes to an ASCII
  * string but fails the refiner produces `SerdeError.ScodecFailure` — the
  * canonical path for "malformed chain bytes." No silent acceptance.
  *
  * Consensus contract: FROZEN. 1-byte length prefix, ASCII payload, validator
  * runs on every decode.
  *
  * Goldens:
  *   - `Address-scodec-v1.hex` — a representative 40-char DAG address.
  */
object AddressCodec {

  // 1 byte (uint8) prefix holds the ASCII payload length. Addresses are up to
  // 51 chars (Stardust); uint8 is more than enough (max value 255).
  private val rawCodec: Codec[String] = variableSizeBytes(uint8, ascii)

  implicit val codec: Codec[Address] =
    rawCodec.exmap(
      s =>
        refineV[DAGAddressRefined](s).fold(
          err => Attempt.failure(Err(s"Address decode: refinement failed: $err")),
          (refined: DAGAddress) => Attempt.successful(Address(refined))
        ),
      (a: Address) => Attempt.successful(a.value.value)
    )

  implicit val immutableCodec: ImmutableCodec[Address] = ImmutableCodec.fromScodecCodec(codec)

  // Helper for tests / compound codecs that want to unsafely build an Address
  // from a known-valid literal at construction time. NOT exposed publicly —
  // regular call sites go through `Address.fromBytes(...)` or the JSON decoder.
  private[serde] def unsafeFromLiteral(s: String): Address =
    refineV[DAGAddressRefined](s).fold(
      err => throw new IllegalArgumentException(s"Bad test literal: $err"),
      Address(_)
    )
}
