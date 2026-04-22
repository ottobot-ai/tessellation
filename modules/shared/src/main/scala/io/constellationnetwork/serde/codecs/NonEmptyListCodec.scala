package io.constellationnetwork.serde.codecs

import cats.data.NonEmptyList

import scodec.codecs.{listOfN, uint16}
import scodec.{Attempt, Codec, Err}

/** Generic scodec codec factory for `NonEmptyList[A]`.
  *
  * Wire format: 2-byte length prefix (uint16) + elements in insertion order.
  * Unlike `NonEmptySet`, insertion order IS the contract for `NonEmptyList` —
  * we don't sort; we preserve the original sequence.
  *
  * A zero-length prefix on decode yields `SerdeError.ScodecFailure` — a
  * `NonEmptyList` cannot be empty.
  *
  * Not marked implicit — call sites invoke `nonEmptyList(...)` explicitly with
  * the inner codec, consistent with `NonEmptySetCodec.nonEmptySet`.
  */
object NonEmptyListCodec {

  def nonEmptyList[A](inner: Codec[A]): Codec[NonEmptyList[A]] =
    listOfN(uint16, inner).exmap(
      list =>
        NonEmptyList
          .fromList(list)
          .fold[Attempt[NonEmptyList[A]]](
            Attempt.failure(Err("NonEmptyList decode: empty collection"))
          )(Attempt.successful),
      (nel: NonEmptyList[A]) => Attempt.successful(nel.toList)
    )
}
