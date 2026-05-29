package io.constellationnetwork.security.smt

import io.constellationnetwork.security.hash.Hash

import io.circe._
import io.circe.syntax.EncoderOps

/** Domain-separated, Circe-encoded hash pre-images for the two materialized node kinds of the binary sparse Merkle tree.
  *
  * Hashing is routed through `Hasher[F].prefixedHash(commitment.asJson, prefix)` — EXACTLY as `MerklePatriciaCommitment` /
  * `MerklePatriciaNode` do — so all consensus-bytes hashing stays on the `Hasher[F]` + Circe case-class path (never hand-rolled Blake2b).
  * The one-byte domain-separation prefixes ([[SmtCommitment.LeafPrefix]] / [[SmtCommitment.InternalPrefix]]) keep a leaf pre-image from
  * ever colliding with an internal-node pre-image.
  *
  * The third logical node kind — an empty/default subtree — is NOT hashed; its digest is the fixed placeholder `Hash.empty`
  * ([[io.constellationnetwork.security.smt.node.SmtNode.Empty]]). This is the Diem/JMT empty-subtree convention.
  *
  *   - [[SmtCommitment.Leaf]] binds the FULL 256-bit leaf `position` (= `Hasher.hash(key)`) together with the value digest (=
  *     `Hasher.hashBytes(value)`). Binding the full position (not a depth-relative remainder) makes a leaf's digest independent of where it
  *     sits in the tree, which is what gives the structure its order-independence and lets a verifier recompute any leaf digest from
  *     `(position, valueDigest)` alone.
  *   - [[SmtCommitment.Internal]] binds the two child subtree digests in fixed `(left, right)` order.
  */
sealed trait SmtCommitment extends Product with Serializable

object SmtCommitment {

  /** Domain-separation prefix prepended (via `prefixedHash`) to a leaf pre-image. Distinct from [[InternalPrefix]] and from the MPT
    * prefixes (this is a separate primitive with its own namespace).
    */
  val LeafPrefix: Array[Byte] = Array(0: Byte)

  /** Domain-separation prefix prepended (via `prefixedHash`) to an internal-node pre-image. */
  val InternalPrefix: Array[Byte] = Array(1: Byte)

  /** Leaf pre-image: the full 256-bit position (the hashed key) and the value digest. */
  final case class Leaf(position: Hash, valueDigest: Hash) extends SmtCommitment

  /** Internal-node pre-image: the two child subtree digests, fixed `(left, right)` order. */
  final case class Internal(left: Hash, right: Hash) extends SmtCommitment

  object Leaf {

    implicit val leafCommitEncoder: Encoder[Leaf] =
      Encoder.instance { c =>
        Json.obj(
          "position" -> c.position.asJson,
          "valueDigest" -> c.valueDigest.asJson
        )
      }

    implicit val leafCommitDecoder: Decoder[Leaf] =
      Decoder.instance { hCursor =>
        for {
          position <- hCursor.downField("position").as[Hash]
          valueDigest <- hCursor.downField("valueDigest").as[Hash]
        } yield Leaf(position, valueDigest)
      }
  }

  object Internal {

    implicit val internalCommitEncoder: Encoder[Internal] =
      Encoder.instance { c =>
        Json.obj(
          "left" -> c.left.asJson,
          "right" -> c.right.asJson
        )
      }

    implicit val internalCommitDecoder: Decoder[Internal] =
      Decoder.instance { hCursor =>
        for {
          left <- hCursor.downField("left").as[Hash]
          right <- hCursor.downField("right").as[Hash]
        } yield Internal(left, right)
      }
  }

  implicit val smtCommitEncoder: Encoder[SmtCommitment] = Encoder.instance {
    case c: Leaf =>
      Json.obj("type" -> Json.fromString("Leaf"), "contents" -> c.asJson)
    case c: Internal =>
      Json.obj("type" -> Json.fromString("Internal"), "contents" -> c.asJson)
  }

  implicit val smtCommitDecoder: Decoder[SmtCommitment] = Decoder.instance { cursor =>
    cursor.downField("type").as[String].flatMap {
      case "Leaf"     => cursor.downField("contents").as[Leaf]
      case "Internal" => cursor.downField("contents").as[Internal]
      case other      => Left(DecodingFailure(s"Unknown SmtCommitment type: $other", cursor.history))
    }
  }
}
