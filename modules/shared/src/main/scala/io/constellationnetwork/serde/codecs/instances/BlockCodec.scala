package io.constellationnetwork.serde.codecs.instances

import cats.data.{NonEmptyList, NonEmptySet}

import io.constellationnetwork.schema.transaction.Transaction
import io.constellationnetwork.schema.{Block, BlockReference}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.BlockReferenceCodec.{codec => blockReferenceCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.signedCodec
import io.constellationnetwork.serde.codecs.instances.TransactionCodec.{codec => transactionCodec}
import io.constellationnetwork.serde.codecs.{NonEmptyListCodec, NonEmptySetCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `Block` — the L1 consensus unit.
  *
  * Wire layout:
  *   - parents : NonEmptyList[BlockReference] (uint16 count + 40 bytes each, insertion order)
  *   - transactions : NonEmptySet[Signed[Transaction]] (uint16 count + sorted elements)
  *
  * Determinism: parent order is a first-class part of the block's identity (preserved as written), but transactions are a set — the encode
  * path sorts them via `Order[Signed[Transaction]]` (derevo-derived from the case-class field order). Two nodes serializing the same
  * `Block` must produce bit-identical bytes; the `NonEmptySetCodec` sorted encode path guarantees that for the transactions, and
  * `NonEmptyList` preserves insertion order which the consensus protocol already treats as canonical.
  *
  * Composition: this is the first codec that exercises both collection helpers (`NonEmptyListCodec` + `NonEmptySetCodec`) AND the
  * parameterized `Signed[_]` wrapper, so it's the end-to-end test of the implicit chain.
  *
  * Consensus contract: FROZEN. Parents first, transactions second. Changing order, prefix width, or the transaction sort order breaks every
  * historical block's hash and therefore every signature covering it.
  */
object BlockCodec {

  private val parentsCodec: Codec[NonEmptyList[BlockReference]] =
    NonEmptyListCodec.nonEmptyList(blockReferenceCodec)

  private val signedTransactionCodec: Codec[Signed[Transaction]] = signedCodec(transactionCodec)

  private val transactionsCodec: Codec[NonEmptySet[Signed[Transaction]]] =
    NonEmptySetCodec.nonEmptySet(signedTransactionCodec)

  implicit val codec: Codec[Block] =
    (parentsCodec :: transactionsCodec)
      .xmap[Block](
        { case ps :: txs :: HNil => Block(ps, txs) },
        b => b.parent :: b.transactions :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[Block] = ImmutableCodec.fromScodecCodec(codec)
}
