package io.constellationnetwork.security.smt

import cats.effect.Sync
import cats.syntax.functor._

import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import io.circe.syntax.EncoderOps

/** The single, shared source of truth for SMT positions, node digests, and path bits — so the prover, the verifier, and the in-memory tree
  * all compute byte-identical hashes. ALL hashing routes through `Hasher[F].prefixedHash(commitment.asJson, prefix)` (Blake2b via the
  * typeclass + a Circe-encoded commitment), exactly as `MerklePatriciaNode`.
  *
  * The metakit-sdk TS verifier must reproduce these three functions to interop:
  *   - [[position]]: `position(key) = Hasher.hash(key.value)` (the 256-bit leaf slot; uniform because it is a hash).
  *   - [[leafDigest]]: `prefixedHash(SmtCommitment.Leaf(position, valueDigest), 0x00)`.
  *   - [[internalDigest]]: `prefixedHash(SmtCommitment.Internal(left, right), 0x01)`. Empty/default subtrees are NOT hashed — their digest
  *     is the fixed placeholder [[empty]] (`Hash.empty`).
  */
object SmtHashing {

  /** Digest of an empty (default) subtree — the all-zeros placeholder, never hashed (Diem/JMT convention). */
  val empty: Hash = Hash.empty

  /** The 256-bit position (slot) of a key: `Hasher.hash(key.value)`. Hashing the key uniformizes the slot distribution. The `Hash` is a
    * 64-char lowercase hex string (32 bytes); [[bits]] reads it big-endian, MSB-first.
    */
  def position[F[_]: Hasher](key: Hex): F[Hash] =
    Hasher[F].hash(key.value)

  /** Digest of a leaf binding the FULL position and value digest. Depth-independent (the full position is in the pre-image). */
  def leafDigest[F[_]: Sync: Hasher](position: Hash, valueDigest: Hash): F[Hash] =
    Hasher[F].prefixedHash(SmtCommitment.Leaf(position, valueDigest).asJson, SmtCommitment.LeafPrefix)

  /** Digest of an internal node binding its two child subtree digests in fixed `(left, right)` order. */
  def internalDigest[F[_]: Sync: Hasher](left: Hash, right: Hash): F[Hash] =
    Hasher[F].prefixedHash(SmtCommitment.Internal(left, right).asJson, SmtCommitment.InternalPrefix)

  /** Combine a child digest `cur` (on the path) with its `sibling`, given the path bit at this depth: bit `false` ⇒ path went LEFT (cur is
    * the left child), bit `true` ⇒ path went RIGHT. This is the per-level step both the tree builder and the verifier use.
    */
  def combine[F[_]: Sync: Hasher](bit: Boolean, cur: Hash, sibling: Hash): F[Hash] =
    if (bit) internalDigest[F](sibling, cur)
    else internalDigest[F](cur, sibling)

  /** Total number of position bits (256 — a SHA-256 hash). */
  val PositionBits: Int = 256

  /** Bit `index` (0 = most-significant bit of the first byte) of a 32-byte position hash, read big-endian. Returns `false` (LEFT) for
    * indices past the 256-bit space (defensive; callers never index past [[PositionBits]]).
    */
  def bit(position: Hash, index: Int): Boolean = {
    val bytes = positionBytes(position)
    val byteIdx = index / 8
    if (byteIdx < 0 || byteIdx >= bytes.length) false
    else {
      val bitInByte = 7 - (index % 8) // MSB-first within the byte
      ((bytes(byteIdx) >> bitInByte) & 1) == 1
    }
  }

  /** Whether two positions agree on the first `prefixLen` bits (used to sanity-check an OtherLeaf shares the queried key's prefix). */
  def sharePrefix(a: Hash, b: Hash, prefixLen: Int): Boolean =
    (0 until prefixLen).forall(i => bit(a, i) == bit(b, i))

  private def positionBytes(position: Hash): Array[Byte] = {
    // Hash.value is a hex string (64 chars for SHA-256). Decode to its 32 raw bytes; the hex string IS the canonical position encoding.
    val v = position.value
    Hex(v).toBytes
  }
}
