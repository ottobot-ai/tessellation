package io.constellationnetwork.serde

import scala.io.Source
import scala.util.Using

import scodec.bits.ByteVector

/** Test helper: loads a golden hex file from `test/resources/golden/serde/` and returns the decoded bytes.
  *
  * Golden files are hand-authored, never auto-regenerated. See `test/resources/golden/serde/README.md` for the policy.
  */
object GoldenVectors {

  /** Load `golden/serde/<name>.hex` from classpath resources. Returns the decoded bytes. Throws on missing file or malformed hex — tests
    * that depend on a golden should fail loudly if the canary is missing.
    */
  def load(name: String): ByteVector = {
    val path = s"/golden/serde/$name.hex"
    val stream = Option(getClass.getResourceAsStream(path))
      .getOrElse(throw new IllegalStateException(s"Missing golden vector: $path"))
    val raw = Using.resource(Source.fromInputStream(stream))(_.mkString.trim)
    ByteVector
      .fromHexDescriptive(raw)
      .fold(err => throw new IllegalStateException(s"Golden '$name' has malformed hex: $err"), identity)
  }
}
