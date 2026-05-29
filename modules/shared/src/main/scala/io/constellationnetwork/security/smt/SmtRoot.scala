package io.constellationnetwork.security.smt

import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Root commitment of a [[SparseMerkleTree]] — the digest of the root node.
  *
  * Wraps [[Hash]] exactly as `MerklePatriciaTrie`'s `MptRoot` does, so the two proof families read alike. The empty tree's root is
  * [[SmtRoot.empty]] (`Hash.empty`, the all-zeros default-subtree placeholder).
  */
@derive(decoder, encoder, eqv, show, order)
case class SmtRoot(value: Hash) extends AnyVal

object SmtRoot {

  /** Root of the empty tree: the all-zeros default-subtree placeholder (`Hash.empty`). Matches the SMT convention that a subtree with zero
    * leaves has the default hash.
    */
  val empty: SmtRoot = SmtRoot(Hash.empty)
}
