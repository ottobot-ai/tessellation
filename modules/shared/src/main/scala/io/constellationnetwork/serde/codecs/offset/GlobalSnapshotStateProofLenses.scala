package io.constellationnetwork.serde.codecs.offset

import io.constellationnetwork.schema.GlobalSnapshotStateProof
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.HashCodec

import scodec.bits.ByteVector

/** Byte-offset lenses for `GlobalSnapshotStateProof` — the 17-field transitional state-proof.
  *
  * Wire layout recap:
  *   - bytes [0..31] lastStateChannelSnapshotHashesProof : Hash
  *   - bytes [32..63] lastTxRefsProof : Hash
  *   - bytes [64..95] balancesProof : Hash
  *   - byte [96] Option[MerkleRoot] discriminator (0x00 | non-zero)
  *   - if present: [97..132] body (4 leafCount + 32 hash = 36 bytes)
  *   - then 13 × Option[Hash] entries, each either 1 byte (absent) or 33 bytes (present flag + 32 hash)
  *
  * The three required hashes have fixed offsets and get direct `Fixed` lenses. The `mptRoot` (final optional hash) requires a dynamic scan
  * because everything between it and offset 96 is variable-length depending on which prior optionals are present.
  *
  * This lens family is exactly what the MPT verification path will use: "given the bytes of a snapshot's stateProof on disk, read just the
  * mptRoot" — without decoding the 13 preceding witness hashes or the merkle-root sub-record.
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

  /** Advance past the `Option[MerkleRoot]` (first optional, 1 byte if absent or 37 bytes if present: 1 discriminator + 4 leafCount + 32
    * hash).
    */
  private def skipOptionalMerkleRoot(bytes: ByteVector, cursor: Long): Option[Long] =
    if (bytes.length <= cursor) None
    else if (bytes(cursor) == 0x00.toByte) Some(cursor + 1L)
    else if (bytes.length >= cursor + 37L) Some(cursor + 37L)
    else None

  /** Locate the `mptRoot` field (last optional Hash). Scans past the merkleRoot (field 4) and the 12 optional-Hash fields before it,
    * returning the byte slice occupied by the mptRoot's inner hash if present, or `None` otherwise.
    */
  private def locateMptRoot(bytes: ByteVector): Option[(Long, Long)] = {
    // After the 3 required hashes, cursor is at offset 96.
    val afterRequired = 96L

    // Step past the merkle-root option (field 4 of the full record, first option).
    val afterMerkleRoot = skipOptionalMerkleRoot(bytes, afterRequired)

    // Then 12 more Option[Hash] fields (fields 5..16) before mptRoot (field 17).
    val afterTwelveHashes = (0 until 12).foldLeft(afterMerkleRoot) { (cursor, _) =>
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
