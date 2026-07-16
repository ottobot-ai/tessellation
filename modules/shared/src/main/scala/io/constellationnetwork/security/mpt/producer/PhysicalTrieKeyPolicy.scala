package io.constellationnetwork.security.mpt.producer

import io.constellationnetwork.security.hex.Hex

/** Immutable physical-key policy owned by one stateful MPT producer.
  *
  * [[Generic]] preserves the complete ROOT-011 terminal-collision validation for arbitrary-length keys. [[FixedWidth]] is an explicit,
  * construction-time capability for dedicated stores whose complete image and every later mutation use one exact key width. Distinct
  * canonical keys of equal width cannot be proper prefixes, so insertion validates the incoming batch without enumerating the retained key
  * set. Initial images and loaded images still receive complete validation.
  */
sealed trait PhysicalTrieKeyPolicy extends Serializable {
  def exactWidthBytes: Option[Int]

  private[producer] def validateComplete(keys: Iterable[Hex]): Either[PhysicalTrieKeyError, Unit]
  private[producer] def validateEach(keys: Iterable[Hex]): Either[PhysicalTrieKeyError, Unit]

  /** `currentKeys` is by-name so a fixed-width policy can prove it does not enumerate the complete retained set. */
  private[producer] def validateInsertion(
    currentKeys: => Set[Hex],
    incomingKeys: Iterable[Hex]
  ): Either[PhysicalTrieKeyError, Unit]
}

object PhysicalTrieKeyPolicy {

  case object Generic extends PhysicalTrieKeyPolicy {
    val exactWidthBytes: Option[Int] = None

    private[producer] def validateComplete(keys: Iterable[Hex]): Either[PhysicalTrieKeyError, Unit] =
      PhysicalTrieKeyValidator.validateKeys(keys)

    private[producer] def validateEach(keys: Iterable[Hex]): Either[PhysicalTrieKeyError, Unit] =
      PhysicalTrieKeyValidator.validateEachKey(keys)

    private[producer] def validateInsertion(
      currentKeys: => Set[Hex],
      incomingKeys: Iterable[Hex]
    ): Either[PhysicalTrieKeyError, Unit] =
      PhysicalTrieKeyValidator.validateInsertion(currentKeys, incomingKeys)
  }

  private final class FixedWidth(val widthBytes: Int) extends PhysicalTrieKeyPolicy {
    val exactWidthBytes: Option[Int] = Some(widthBytes)
    private val widthChars = widthBytes * 2

    private def validateWidth(key: Hex): Either[PhysicalTrieKeyError, Unit] =
      Either.cond(
        key.value.length == widthChars,
        (),
        UnexpectedPhysicalTrieKeyWidth(key, widthBytes, key.value.length / 2)
      )

    private def validateOne(key: Hex): Either[PhysicalTrieKeyError, Unit] =
      PhysicalTrieKeyValidator.validateKey(key).flatMap(_ => validateWidth(key))

    private[producer] def validateComplete(keys: Iterable[Hex]): Either[PhysicalTrieKeyError, Unit] = {
      val captured = keys.iterator.toVector
      captured
        .foldLeft[Either[PhysicalTrieKeyError, Unit]](Right(())) {
          case (Right(_), key)     => validateOne(key)
          case (left @ Left(_), _) => left
        }
        .flatMap(_ => PhysicalTrieKeyValidator.validateKeys(captured))
    }

    private[producer] def validateEach(keys: Iterable[Hex]): Either[PhysicalTrieKeyError, Unit] =
      keys.iterator.foldLeft[Either[PhysicalTrieKeyError, Unit]](Right(())) {
        case (Right(_), key)     => validateOne(key)
        case (left @ Left(_), _) => left
      }

    private[producer] def validateInsertion(
      currentKeys: => Set[Hex],
      incomingKeys: Iterable[Hex]
    ): Either[PhysicalTrieKeyError, Unit] =
      validateComplete(incomingKeys)

    override def equals(other: Any): Boolean = other match {
      case that: FixedWidth => widthBytes == that.widthBytes
      case _                => false
    }

    override def hashCode(): Int = widthBytes.hashCode()
    override def toString: String = s"FixedWidth($widthBytes bytes)"
  }

  final case class InvalidFixedWidth(widthBytes: Int)
      extends IllegalArgumentException(s"fixed-width physical MPT keys require 1..${Int.MaxValue / 2} bytes, got $widthBytes")

  def fixedWidth(widthBytes: Int): Either[InvalidFixedWidth, PhysicalTrieKeyPolicy] =
    Either.cond(widthBytes > 0 && widthBytes <= Int.MaxValue / 2, new FixedWidth(widthBytes), InvalidFixedWidth(widthBytes))
}
