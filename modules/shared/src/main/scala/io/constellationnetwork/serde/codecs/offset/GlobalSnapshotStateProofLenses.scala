package io.constellationnetwork.serde.codecs.offset

import io.constellationnetwork.schema.GlobalSnapshotStateProof
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.HashCodec

import scodec.bits.ByteVector

/** Byte-offset lenses for `GlobalSnapshotStateProof` — the 19-field transitional state-proof. `mptRoot` is field 17; the trailing fields
  * (`historicalStakeSnapshots`, `smtRoot`) come AFTER it and do not affect any lens here (all lenses read at or before `mptRoot`).
  *
  * Wire layout recap:
  *   - bytes [0..31] lastStateChannelSnapshotHashesProof : Hash
  *   - bytes [32..63] lastTxRefsProof : Hash
  *   - bytes [64..95] balancesProof : Hash
  *   - byte [96] Option[CurrencySnapshotMptRoots] (field 4) discriminator (0x00 | non-zero)
  *   - if present: [97..160] body (2 × 32-byte hashes = 64 bytes)
  *   - then 12 × Option[Hash] entries (fields 5..16), each either 1 byte (absent) or 33 bytes (present flag + 32 hash), then `mptRoot`
  *     (field 17)
  *
  * The three required hashes have fixed offsets and get direct `Fixed` lenses. The `mptRoot` (field 17) requires a dynamic scan because
  * everything between it and offset 96 is variable-length depending on which prior optionals are present.
  *
  * This lens family is exactly what the MPT verification path will use: "given the bytes of a snapshot's stateProof on disk, read just the
  * mptRoot" — without decoding the preceding witness hashes or the currency-roots sub-record.
  */
object GlobalSnapshotStateProofLenses {

  // ---- Fixed-offset: the three required hashes ----------------------------

  val lastStateChannelSnapshotHashesProof: FieldLens[GlobalSnapshotStateProof, Hash] =
    FieldLens.fixed(offset = 0L, length = 32L, HashCodec.codec)

  val lastTxRefsProof: FieldLens[GlobalSnapshotStateProof, Hash] =
    FieldLens.fixed(offset = 32L, length = 32L, HashCodec.codec)

  val balancesProof: FieldLens[GlobalSnapshotStateProof, Hash] =
    FieldLens.fixed(offset = 64L, length = 32L, HashCodec.codec)

  // ---- Dynamic scan helpers -----------------------------------------------

  /** Advance a cursor past one `Option[Hash]` field: 1 byte if absent, 33 bytes if present. Returns the new cursor position, or `None` if
    * the byte stream is truncated.
    */
  private def skipOptionalHash(bytes: ByteVector, cursor: Long): Option[Long] =
    if (bytes.length <= cursor) None
    else if (bytes(cursor) == 0x00.toByte) Some(cursor + 1L)
    else if (bytes.length >= cursor + 33L) Some(cursor + 33L)
    else None

  /** Advance past the field-4 `Option[CurrencySnapshotMptRoots]` (1 byte if absent or 65 bytes if present: 1 discriminator + 2 × 32-byte
    * hashes).
    */
  private def skipOptionalCurrencyRoots(bytes: ByteVector, cursor: Long): Option[Long] =
    if (bytes.length <= cursor) None
    else if (bytes(cursor) == 0x00.toByte) Some(cursor + 1L)
    else if (bytes.length >= cursor + 65L) Some(cursor + 65L)
    else None

  /** Locate the `mptRoot` field (field 17). Scans past the field-4 currency-roots option and the 12 optional-Hash fields before it,
    * returning the byte slice occupied by the mptRoot's inner hash if present, or `None` otherwise.
    */
  private def locateMptRoot(bytes: ByteVector): Option[(Long, Long)] = {
    // After the 3 required hashes, cursor is at offset 96.
    val afterRequired = 96L

    // Step past the field-4 currency-roots option (first option).
    val afterCurrencyRoots = skipOptionalCurrencyRoots(bytes, afterRequired)

    // Then 12 more Option[Hash] fields (fields 5..16) before mptRoot (field 17).
    val afterTwelveHashes = (0 until 12).foldLeft(afterCurrencyRoots) { (cursor, _) =>
      cursor.flatMap(c => skipOptionalHash(bytes, c))
    }

    afterTwelveHashes.flatMap { cursor =>
      // Now at the mptRoot discriminator.
      if (bytes.length <= cursor) None
      else if (bytes(cursor) == 0x00.toByte) None // mptRoot absent
      else if (bytes.length >= cursor + 33L)
        // The 32-byte hash body is at cursor+1..cursor+33.
        Some((cursor + 1L, 32L))
      else None
    }
  }

  val mptRoot: FieldLens[GlobalSnapshotStateProof, Hash] =
    FieldLens.dynamic(HashCodec.codec)(locateMptRoot)

  /** Convenience: `Option[Hash]` variant — returns `None` if mptRoot is absent, `Some(hash)` if present, `Failure` if bytes are malformed.
    */
  def readMptRoot(bytes: ByteVector): scodec.Attempt[Option[Hash]] =
    mptRoot.asInstanceOf[FieldLens.Dynamic[GlobalSnapshotStateProof, Hash]].readOption(bytes)
}
