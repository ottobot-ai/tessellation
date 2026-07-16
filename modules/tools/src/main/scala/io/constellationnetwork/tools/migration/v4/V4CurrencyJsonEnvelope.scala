package io.constellationnetwork.tools.migration.v4

import cats.data.NonEmptySet

import scala.collection.immutable.SortedSet

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.signature.signature.SignatureProof

import io.circe._
import io.circe.syntax._

private[v4] final case class V4CurrencyJsonEnvelope(
  value: Json,
  ordinal: SnapshotOrdinal,
  proofs: NonEmptySet[SignatureProof]
)

private[v4] object V4CurrencyJsonEnvelope {
  private val OuterKeys = Set("value", "proofs")

  private val RequiredCurrencyKeys = Set(
    "ordinal",
    "height",
    "subHeight",
    "lastSnapshotHash",
    "blocks",
    "rewards",
    "tips",
    "stateProof",
    "epochProgress",
    "version"
  )

  private val OptionalCurrencyKeys = Set(
    "dataApplication",
    "messages",
    "globalSnapshotSyncs",
    "feeTransactions",
    "artifacts",
    "allowSpendBlocks",
    "tokenLockBlocks",
    "globalSyncView"
  )

  private val CurrencyKeys = RequiredCurrencyKeys ++ OptionalCurrencyKeys

  private def objectOrFailure(json: Json, history: List[io.circe.CursorOp], label: String): Decoder.Result[JsonObject] =
    json.asObject.toRight(DecodingFailure(s"$label must be a JSON object", history))

  private def exactKeys(
    obj: JsonObject,
    required: Set[String],
    allowed: Set[String],
    history: List[io.circe.CursorOp],
    label: String
  ): Decoder.Result[Unit] = {
    val actual = obj.keys.toSet
    val missing = required -- actual
    val unknown = actual -- allowed

    Either.cond(
      missing.isEmpty && unknown.isEmpty,
      (),
      DecodingFailure(s"$label key mismatch: missing=${missing.toList.sorted} unknown=${unknown.toList.sorted}", history)
    )
  }

  private def rejectExplicitNulls(obj: JsonObject, history: List[io.circe.CursorOp]): Decoder.Result[Unit] = {
    val nullKeys = obj.toIterable.collect { case (key, value) if value.isNull => key }.toList.sorted

    Either.cond(
      nullKeys.isEmpty,
      (),
      DecodingFailure(s"currency value contains explicit null fields: $nullKeys", history)
    )
  }

  implicit val decoder: Decoder[V4CurrencyJsonEnvelope] = Decoder.instance { cursor =>
    for {
      outer <- objectOrFailure(cursor.value, cursor.history, "v4 signed envelope")
      _ <- exactKeys(outer, OuterKeys, OuterKeys, cursor.history, "v4 signed envelope")
      valueJson <- outer("value").toRight(DecodingFailure("v4 signed envelope is missing value", cursor.history))
      valueObject <- objectOrFailure(valueJson, cursor.history, "v4 currency value")
      _ <- exactKeys(valueObject, RequiredCurrencyKeys, CurrencyKeys, cursor.history, "v4 currency value")
      _ <- rejectExplicitNulls(valueObject, cursor.history)
      ordinalJson <- valueObject("ordinal").toRight(DecodingFailure("v4 currency value is missing ordinal", cursor.history))
      ordinal <- ordinalJson.as[SnapshotOrdinal]
      proofsJson <- outer("proofs").toRight(DecodingFailure("v4 signed envelope is missing proofs", cursor.history))
      proofVector <- proofsJson.as[Vector[SignatureProof]]
      proofSet = SortedSet.from(proofVector)
      _ <- Either.cond(
        proofVector.nonEmpty,
        (),
        DecodingFailure("v4 signed envelope proofs must be non-empty", cursor.history)
      )
      _ <- Either.cond(
        proofVector.size == proofSet.size,
        (),
        DecodingFailure("v4 signed envelope contains duplicate proofs", cursor.history)
      )
      _ <- Either.cond(
        proofVector == proofSet.toVector,
        (),
        DecodingFailure("v4 signed envelope proofs are not in canonical sorted-set order", cursor.history)
      )
    } yield V4CurrencyJsonEnvelope(valueJson, ordinal, NonEmptySet.fromSetUnsafe(proofSet))
  }

  implicit val encoder: Encoder.AsObject[V4CurrencyJsonEnvelope] = Encoder.AsObject.instance { envelope =>
    JsonObject(
      "value" -> envelope.value,
      "proofs" -> Json.fromValues(envelope.proofs.toSortedSet.toVector.map(_.asJson))
    )
  }
}
