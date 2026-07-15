package io.constellationnetwork.schema.nakamoto

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.Signature

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Immutable period-zero KES+VRF identity committed by the canonical genesis state root.
  *
  * This is deliberately not a runtime `KesRegistrationCert`: genesis has no prior registration history to extend, and inserting a
  * fabricated runtime certificate would give the two artifact classes incompatible signature domains and parent semantics. The long-term
  * operator identity signs this complete record (except the signature itself), including its network and activation context. Possession of
  * this record proves key ownership only; it does not confer validator, committee, watchtower, or stake eligibility.
  */
@derive(encoder, decoder, eqv, show)
case class GenesisOperatorConsensusKey(
  networkMagic: String,
  activationOrdinal: Long,
  startingEpochProgress: Long,
  operatorPeerId: PeerId,
  operatorAddress: Address,
  kesMasterVerificationKey: Hex,
  kesMasterVerificationKeyStep: Int,
  kesPeriodOffset: Long,
  vrfPublicKey: VrfPublicKey,
  longTermSignature: Signature
)

object GenesisOperatorConsensusKey {
  private val SignatureDomain = "tessellation-nakamoto/gl0-genesis-operator-consensus-keys/v1"

  private def writeBytes(out: DataOutputStream, bytes: Array[Byte]): Unit = {
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private def writeUtf8(out: DataOutputStream, value: String): Unit =
    writeBytes(out, value.getBytes(StandardCharsets.UTF_8))

  /** Canonical preimage signed by the operator's established long-term identity. */
  def signaturePreimage(record: GenesisOperatorConsensusKey): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)

    writeUtf8(out, SignatureDomain)
    writeUtf8(out, record.networkMagic)
    out.writeLong(record.activationOrdinal)
    out.writeLong(record.startingEpochProgress)
    writeBytes(out, record.operatorPeerId.value.toBytes)
    writeUtf8(out, record.operatorAddress.value.value)
    writeBytes(out, record.kesMasterVerificationKey.toBytes)
    out.writeInt(record.kesMasterVerificationKeyStep)
    out.writeLong(record.kesPeriodOffset)
    writeBytes(out, record.vrfPublicKey.toBytes)
    out.flush()
    bytes.toByteArray
  }

  /** Constructor-time signing helper retained for the genesis generator and fixtures. */
  def signaturePreimage(
    networkMagic: String,
    activationOrdinal: Long,
    startingEpochProgress: Long,
    peerId: Array[Byte],
    address: String,
    kesMasterVerificationKey: Array[Byte],
    kesMasterVerificationKeyStep: Int,
    kesPeriodOffset: Long,
    vrfPublicKey: Array[Byte]
  ): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)

    writeUtf8(out, SignatureDomain)
    writeUtf8(out, networkMagic)
    out.writeLong(activationOrdinal)
    out.writeLong(startingEpochProgress)
    writeBytes(out, peerId)
    writeUtf8(out, address)
    writeBytes(out, kesMasterVerificationKey)
    out.writeInt(kesMasterVerificationKeyStep)
    out.writeLong(kesPeriodOffset)
    writeBytes(out, vrfPublicKey)
    out.flush()
    bytes.toByteArray
  }
}
