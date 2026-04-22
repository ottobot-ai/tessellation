package io.constellationnetwork.serde.codecs.instances

import cats.data.NonEmptyList

import io.constellationnetwork.merkletree.{MerkleTree, Proof, ProofEntry}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.EitherCodec.either
import io.constellationnetwork.serde.codecs.NonEmptyListCodec.nonEmptyList
import io.constellationnetwork.serde.codecs.Primitives._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codecs for the merkle-tree auxiliary types:
  *   - `ProofEntry` — `(target: Hash, sibling: Either[Hash, Hash])` (33 + 33 = 66 bytes).
  *   - `Proof` — `NonEmptyList[ProofEntry]`.
  *   - `MerkleTree` — `(leafCount: NonNegInt, nodes: NonEmptyList[Hash])`.
  *
  * `Proof` is the first codec to exercise the `EitherCodec` helper (in ProofEntry.sibling).
  */
object MerkleTreeCodecs {

  private val siblingCodec: Codec[Either[Hash, Hash]] = either(hashCodec, hashCodec)

  implicit val proofEntryCodec: Codec[ProofEntry] =
    (hashCodec :: siblingCodec)
      .xmap[ProofEntry](
        { case t :: s :: HNil => ProofEntry(t, s) },
        e => e.target :: e.sibling :: HNil
      )

  implicit val proofEntryImmutableCodec: ImmutableCodec[ProofEntry] =
    ImmutableCodec.fromScodecCodec(proofEntryCodec)

  private val entriesCodec: Codec[NonEmptyList[ProofEntry]] = nonEmptyList(proofEntryCodec)

  implicit val proofCodec: Codec[Proof] =
    entriesCodec.xmap[Proof](Proof(_), _.entries)

  implicit val proofImmutableCodec: ImmutableCodec[Proof] = ImmutableCodec.fromScodecCodec(proofCodec)

  private val nodesCodec: Codec[NonEmptyList[Hash]] = nonEmptyList(hashCodec)

  implicit val merkleTreeCodec: Codec[MerkleTree] =
    (nonNegIntCodec :: nodesCodec)
      .xmap[MerkleTree](
        { case lc :: ns :: HNil => MerkleTree(lc, ns) },
        m => m.leafCount :: m.nodes :: HNil
      )

  implicit val merkleTreeImmutableCodec: ImmutableCodec[MerkleTree] =
    ImmutableCodec.fromScodecCodec(merkleTreeCodec)
}
