package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.syntax.all._

import io.constellationnetwork.security.hex.Hex

sealed abstract class DurableNipopowRecoveryRequired(message: String) extends RuntimeException(message)

final case class MalformedDurableNipopowKey(partition: String, key: Hex, reason: String)
    extends DurableNipopowRecoveryRequired(
      s"$partition contains malformed durable key '${DurableNipopowKeyCodec.render(key)}': $reason; authenticated recovery required"
    )

final case class DuplicateDurableNipopowIdentity(partition: String, identity: String, keys: List[Hex])
    extends DurableNipopowRecoveryRequired(
      s"$partition contains duplicate logical identity '$identity' at physical keys " +
        s"${keys.map(DurableNipopowKeyCodec.render).mkString("[", ",", "]")}; authenticated recovery required"
    )

final case class MalformedDurableNipopowValue(partition: String, key: Hex, reason: String)
    extends DurableNipopowRecoveryRequired(
      s"$partition contains malformed durable value at key '${DurableNipopowKeyCodec.render(key)}': $reason; " +
        "authenticated recovery required"
    )

final case class DurableNipopowEnumerationChanged(partition: String)
    extends DurableNipopowRecoveryRequired(
      s"$partition changed between complete key validation and value enumeration; authenticated recovery required"
    )

private[nipopow] object DurableNipopowKeyCodec {

  private val MaxRenderedKeyChars = 128

  def render(key: Hex): String =
    Option(key).flatMap(keyValue => Option(keyValue.value)).fold("<null>") { value =>
      if (value.length <= MaxRenderedKeyChars) value
      else s"${value.take(MaxRenderedKeyChars)}...(${value.length} chars)"
    }

  def fixedWidthHex(partition: String, key: Hex, expectedChars: Int): Either[DurableNipopowRecoveryRequired, String] =
    Option(key).flatMap(keyValue => Option(keyValue.value)) match {
      case None => MalformedDurableNipopowKey(partition, key, "null key").asLeft
      case Some(value) if value.length != expectedChars =>
        MalformedDurableNipopowKey(partition, key, s"expected exactly $expectedChars hex characters, got ${value.length}").asLeft
      case Some(value) if !value.forall(isHexDigit) =>
        MalformedDurableNipopowKey(partition, key, "contains a non-hex character").asLeft
      case Some(value) => value.asRight
    }

  def boundedUnsigned(
    partition: String,
    key: Hex,
    component: String,
    value: String,
    maximum: BigInt
  ): Either[DurableNipopowRecoveryRequired, Long] = {
    val parsed = BigInt(value, 16)
    Either.cond(
      parsed <= maximum,
      parsed.longValue,
      MalformedDurableNipopowKey(partition, key, s"$component is out of range: 0x$value")
    )
  }

  def canonical(
    partition: String,
    physical: Hex,
    expected: Hex
  ): Either[DurableNipopowRecoveryRequired, Unit] = {
    val physicalValue = Option(physical).flatMap(value => Option(value.value))
    val expectedValue = Option(expected).flatMap(value => Option(value.value))

    Either.cond(
      physicalValue.isDefined && physicalValue == expectedValue,
      (),
      MalformedDurableNipopowKey(partition, physical, s"noncanonical encoding; expected '${expectedValue.getOrElse("<null>")}'")
    )
  }

  def decodeAll[A](
    partition: String,
    keys: Iterable[Hex],
    decodeCandidate: Hex => Either[DurableNipopowRecoveryRequired, A],
    canonicalKey: A => Hex,
    identity: A => String
  ): Either[DurableNipopowRecoveryRequired, List[(Hex, A)]] =
    keys.toList.sortBy(render).traverse { key =>
      decodeCandidate(key).map(key -> _)
    }.flatMap { decoded =>
      val duplicate = decoded
        .groupBy { case (_, logical) => identity(logical) }
        .toList
        .collect {
          case (logicalIdentity, occurrences) if occurrences.sizeCompare(1) > 0 =>
            DuplicateDurableNipopowIdentity(partition, logicalIdentity, occurrences.map(_._1).sortBy(render))
        }
        .sortBy(_.identity)
        .headOption

      duplicate
        .toLeft(())
        .flatMap(_ => decoded.traverse_ { case (physical, logical) => canonical(partition, physical, canonicalKey(logical)) })
        .as(decoded)
    }

  private def isHexDigit(char: Char): Boolean =
    (char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')
}
