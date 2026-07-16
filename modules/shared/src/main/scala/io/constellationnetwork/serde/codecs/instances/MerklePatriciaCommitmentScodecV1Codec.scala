package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MerklePatriciaCommitment.{Branch, Extension, Leaf}
import io.constellationnetwork.security.mpt.{MerklePatriciaCommitment, Nibble}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}

import scodec._
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs.{bytes, uint16, uint8}

/** Dark ScodecV1 byte contract for an MPT node commitment.
  *
  * This codec is deliberately non-implicit and has no production consumer. Live MPT construction and verification continue to hash the
  * existing JSON commitment through `Hasher`; activating these bytes as an MPT hash preimage requires the coordinated consensus cutover.
  *
  * Wire layout:
  *   - leaf: `0x00 || path || dataDigest`
  *   - branch: `0x01 || uint8 childCount || (nibble || childDigest)*`, with entries strictly ascending by nibble
  *   - extension: `0x02 || path || childDigest`
  *   - path: `uint16 nibbleCount || ceil(nibbleCount / 2) packed bytes`; an odd path has a zero low-nibble pad
  *   - digest: 32 raw bytes
  */
object MerklePatriciaCommitmentScodecV1Codec {

  val MaxPathNibbles: Int = 0xffff
  val MaxBranchChildren: Int = 16

  private val LeafTag = 0
  private val BranchTag = 1
  private val ExtensionTag = 2

  private def isCanonicalHash(hash: Hash): Boolean =
    hash.value.length == HashCodec.HashByteLength * 2 && hash.value.forall { char =>
      (char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')
    }

  private val canonicalHashCodec: Codec[Hash] =
    hashCodec.exmap(
      hash =>
        if (isCanonicalHash(hash)) Attempt.successful(hash)
        else Attempt.failure(Err("MPT commitment hash must be 64-character lowercase hexadecimal")),
      hash =>
        if (isCanonicalHash(hash)) Attempt.successful(hash)
        else Attempt.failure(Err("MPT commitment hash must be 64-character lowercase hexadecimal"))
    )

  private val nibbleCodec: Codec[Nibble] =
    uint8.exmap(
      value =>
        Nibble
          .validated(value.toByte)
          .fold(_ => Attempt.failure(Err(s"MPT branch nibble must be in [0,15], got $value")), Attempt.successful),
      nibble => {
        val value = nibble.value.toInt
        if (value >= 0 && value <= 15) Attempt.successful(value)
        else Attempt.failure(Err(s"MPT branch nibble must be in [0,15], got $value"))
      }
    )

  private val pathCodec: Codec[Seq[Nibble]] = new Codec[Seq[Nibble]] {
    override val sizeBound: SizeBound = SizeBound.atLeast(16L)

    override def encode(path: Seq[Nibble]): Attempt[BitVector] = {
      val size = path.size
      val invalid = path.iterator.map(_.value.toInt).find(value => value < 0 || value > 15)

      if (size > MaxPathNibbles)
        Attempt.failure(Err(s"MPT path has $size nibbles, maximum is $MaxPathNibbles"))
      else
        invalid match {
          case Some(value) => Attempt.failure(Err(s"MPT path nibble must be in [0,15], got $value"))
          case None =>
            val packed = Array.fill[Byte]((size + 1) / 2)(0)
            path.iterator.zipWithIndex.foreach {
              case (nibble, index) =>
                val byteIndex = index / 2
                if ((index & 1) == 0) packed(byteIndex) = (nibble.value << 4).toByte
                else packed(byteIndex) = (packed(byteIndex) | nibble.value).toByte
            }

            uint16.encode(size).map(_ ++ ByteVector.view(packed).toBitVector)
        }
    }

    override def decode(bits: BitVector): Attempt[DecodeResult[Seq[Nibble]]] =
      uint16.decode(bits).flatMap {
        case DecodeResult(nibbleCount, afterCount) =>
          val byteCount = (nibbleCount + 1) / 2
          bytes(byteCount).decode(afterCount).flatMap {
            case DecodeResult(packed, remainder) =>
              if ((nibbleCount & 1) == 1 && (packed.last & 0x0f) != 0)
                Attempt.failure(Err("MPT path has non-zero low-nibble padding"))
              else
                Attempt.successful(DecodeResult(unpackPath(packed, nibbleCount), remainder))
          }
      }
  }

  private val entryCodec: Codec[(Nibble, Hash)] = nibbleCodec.pairedWith(canonicalHashCodec)

  private val leafCodec: Codec[Leaf] =
    pathCodec
      .pairedWith(canonicalHashCodec)
      .xmap[Leaf]({ case (remaining, dataDigest) => Leaf(remaining, dataDigest) }, leaf => leaf.remaining -> leaf.dataDigest)

  private val extensionCodec: Codec[Extension] =
    pathCodec
      .pairedWith(canonicalHashCodec)
      .xmap[Extension](
        { case (shared, childDigest) => Extension(shared, childDigest) },
        extension => extension.shared -> extension.childDigest
      )

  private val branchCodec: Codec[Branch] = new Codec[Branch] {
    override val sizeBound: SizeBound = SizeBound.atLeast(8L)

    override def encode(branch: Branch): Attempt[BitVector] = {
      val entries = branch.pathsDigest.toList.sortBy(_._1.value)
      val keysAreValid = entries.forall { case (nibble, _) => nibble.value >= 0 && nibble.value <= 15 }

      if (entries.size > MaxBranchChildren)
        Attempt.failure(Err(s"MPT branch has ${entries.size} children, maximum is $MaxBranchChildren"))
      else if (!keysAreValid)
        Attempt.failure(Err("MPT branch contains a nibble outside [0,15]"))
      else
        for {
          count <- uint8.encode(entries.size)
          body <- encodeEntries(entries)
        } yield count ++ body
    }

    override def decode(bits: BitVector): Attempt[DecodeResult[Branch]] =
      uint8.decode(bits).flatMap {
        case DecodeResult(count, _) if count > MaxBranchChildren =>
          Attempt.failure(Err(s"MPT branch child count $count exceeds maximum $MaxBranchChildren"))
        case DecodeResult(count, remainder) =>
          decodeEntries(count, remainder).flatMap {
            case DecodeResult(entries, afterEntries) =>
              if (strictlyIncreasing(entries))
                Attempt.successful(DecodeResult(Branch(entries.toMap), afterEntries))
              else Attempt.failure(Err("MPT branch keys must be strictly increasing"))
          }
      }
  }

  /** Explicit only: importing this object cannot install a consensus codec through implicit resolution. */
  val codec: Codec[MerklePatriciaCommitment] = new Codec[MerklePatriciaCommitment] {
    override val sizeBound: SizeBound = SizeBound.atLeast(8L)

    override def encode(commitment: MerklePatriciaCommitment): Attempt[BitVector] = {
      val (tag, body) = commitment match {
        case leaf: Leaf           => LeafTag -> leafCodec.encode(leaf)
        case branch: Branch       => BranchTag -> branchCodec.encode(branch)
        case extension: Extension => ExtensionTag -> extensionCodec.encode(extension)
      }

      for {
        tagBits <- uint8.encode(tag)
        bodyBits <- body
      } yield tagBits ++ bodyBits
    }

    override def decode(bits: BitVector): Attempt[DecodeResult[MerklePatriciaCommitment]] =
      uint8.decode(bits).flatMap {
        case DecodeResult(LeafTag, remainder) =>
          leafCodec.decode(remainder).map(_.map(value => value: MerklePatriciaCommitment))
        case DecodeResult(BranchTag, remainder) =>
          branchCodec.decode(remainder).map(_.map(value => value: MerklePatriciaCommitment))
        case DecodeResult(ExtensionTag, remainder) =>
          extensionCodec.decode(remainder).map(_.map(value => value: MerklePatriciaCommitment))
        case DecodeResult(other, _) =>
          Attempt.failure(Err(s"Unknown MPT commitment tag: $other"))
      }
  }

  /** Explicit only and complete on decode; it is not a runtime hashing/signing instance. */
  val immutableCodec: ImmutableCodec[MerklePatriciaCommitment] = ImmutableCodec.fromScodecCodec(codec)

  private def unpackPath(packed: ByteVector, nibbleCount: Int): Vector[Nibble] = {
    val result = Vector.newBuilder[Nibble]
    result.sizeHint(nibbleCount)
    val bytes = packed.toArray
    var index = 0

    while (index < nibbleCount) {
      val unsigned = bytes(index / 2) & 0xff
      val value = if ((index & 1) == 0) (unsigned >>> 4) & 0x0f else unsigned & 0x0f
      result += Nibble.unsafe(value.toByte)
      index += 1
    }

    result.result()
  }

  private def encodeEntries(entries: List[(Nibble, Hash)]): Attempt[BitVector] =
    entries.foldLeft(Attempt.successful(BitVector.empty)) {
      case (encoded, entry) =>
        for {
          prefix <- encoded
          next <- entryCodec.encode(entry)
        } yield prefix ++ next
    }

  private def decodeEntries(count: Int, bits: BitVector): Attempt[DecodeResult[List[(Nibble, Hash)]]] = {
    def loop(remaining: Int, current: BitVector, acc: List[(Nibble, Hash)]): Attempt[DecodeResult[List[(Nibble, Hash)]]] =
      if (remaining == 0) Attempt.successful(DecodeResult(acc.reverse, current))
      else
        entryCodec.decode(current).flatMap {
          case DecodeResult(entry, next) => loop(remaining - 1, next, entry :: acc)
        }

    loop(count, bits, Nil)
  }

  private def strictlyIncreasing(entries: List[(Nibble, Hash)]): Boolean =
    entries.zip(entries.drop(1)).forall {
      case ((left, _), (right, _)) => left.value < right.value
    }
}
