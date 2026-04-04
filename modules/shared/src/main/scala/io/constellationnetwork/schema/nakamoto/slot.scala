package io.constellationnetwork.schema.nakamoto

import cats.Order
import cats.kernel.{Next, PartialOrder, PartialPrevious}
import cats.syntax.all._

import io.constellationnetwork.ext.derevo.ordering
import io.constellationnetwork.schema._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import derevo.cats.{eqv, order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import eu.timepit.refined.api.Refined
import eu.timepit.refined.cats._
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.refineV
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.{Decoder, Encoder}
import io.estatico.newtype.macros.newtype

/** Nakamoto consensus types for Taktikos-style slot-based leader election.
  */
object slot {

  /** Slot number in Nakamoto consensus. 1 slot = 1 second.
    */
  @derive(order, ordering, show)
  case class Slot(value: NonNegLong) {
    def plus(addend: NonNegLong): Slot = Slot(value |+| addend)
  }

  object Slot {
    def apply(value: Long): Option[Slot] =
      NonNegLong.from(value).toOption.map(Slot(_))

    def unsafeApply(value: Long): Slot =
      Slot(Refined.unsafeApply(value))

    val MinValue: Slot = Slot(NonNegLong.MinValue)

    implicit val next: Next[Slot] = new Next[Slot] {
      def next(a: Slot): Slot = Slot(a.value |+| NonNegLong(1L))
      def partialOrder: PartialOrder[Slot] = Order[Slot]
    }

    implicit val partialPrevious: PartialPrevious[Slot] = new PartialPrevious[Slot] {
      def partialOrder: PartialOrder[Slot] = Order[Slot]

      def partialPrevious(a: Slot): Option[Slot] =
        refineV[NonNegative].apply[Long](a.value.value |+| -1).toOption.map(r => Slot(r))
    }

    implicit val encoder: Encoder[Slot] = Encoder[NonNegLong].contramap(_.value)
    implicit val decoder: Decoder[Slot] = Decoder[NonNegLong].map(Slot(_))
  }

  /** VRF proof bytes (80 bytes: Gamma || c || s) encoded as hex string.
    *
    * The proof demonstrates that the holder of a secret key computed a deterministic output for a given input, without revealing the
    * secret.
    */
  @derive(decoder, encoder, eqv, show)
  @newtype
  case class VrfProof(value: Hex) {
    def toBytes: Array[Byte] = value.toBytes
  }

  object VrfProof {
    val ExpectedLength: Int = 80

    def fromBytes(bytes: Array[Byte]): VrfProof =
      VrfProof(Hex.fromBytes(bytes))

    def fromHex(hex: String): VrfProof =
      VrfProof(Hex(hex))
  }

  /** VRF output hash (64 bytes, the beta value) encoded as hex string.
    *
    * This is the verifiable random output derived from the VRF proof. Used for slot leader election: hash < threshold → eligible to produce
    * block.
    */
  @derive(decoder, encoder, eqv, show)
  @newtype
  case class VrfOutput(value: Hex) {
    def toBytes: Array[Byte] = value.toBytes
  }

  object VrfOutput {
    val ExpectedLength: Int = 64

    def fromBytes(bytes: Array[Byte]): VrfOutput =
      VrfOutput(Hex.fromBytes(bytes))

    def fromHex(hex: String): VrfOutput =
      VrfOutput(Hex(hex))
  }

  /** VRF public key (Ed25519, 32 bytes) encoded as hex string.
    */
  @derive(decoder, encoder, eqv, show)
  @newtype
  case class VrfPublicKey(value: Hex) {
    def toBytes: Array[Byte] = value.toBytes
  }

  object VrfPublicKey {
    val ExpectedLength: Int = 32

    def fromBytes(bytes: Array[Byte]): VrfPublicKey =
      VrfPublicKey(Hex.fromBytes(bytes))
  }

  /** Certificate proving a validator was eligible to produce a snapshot in a given slot.
    *
    * All-or-nothing: either you have the full certificate or none. Present on GlobalIncrementalSnapshot post-activation (None
    * pre-activation).
    *
    * Verifiers check:
    *   1. slot is within clock tolerance (|mySlot - slot| ≤ skew) 2. VRF proof verifies against (eta || slot) with the given public key 3.
    *      VRF output normalized to [0,1) is below LDD threshold for the slot gap 4. Public key matches the producer's derived VRF key
    */
  @derive(decoder, encoder, eqv, show)
  case class SlotCertificate(
    slot: Slot,
    vrfProof: VrfProof,
    vrfPublicKey: VrfPublicKey,
    eta: Hash,
    activePoolSize: Int,
    activePoolHash: Hash
  )
}
