package io.constellationnetwork.serde.codecs

import scodec.Codec
import scodec.codecs.{listOfN, uint16}

/** Generic scodec codec factory for `List[A]`.
  *
  * Wire format: 2-byte length prefix (uint16) + elements in insertion order.
  *
  * Unlike `NonEmptyListCodec.nonEmptyList`, the empty case is valid — `List` has no non-emptiness
  * invariant. Unlike `SortedSetCodec.sortedSet`, elements are preserved in insertion order (the
  * contract a caller using `List` has chosen over `Set`).
  *
  * Not marked implicit — call sites invoke `list(...)` explicitly.
  *
  * Consensus contract: FROZEN. 2-byte count + insertion-order elements.
  */
object ListCodec {

  def list[A](inner: Codec[A]): Codec[List[A]] = listOfN(uint16, inner)
}
