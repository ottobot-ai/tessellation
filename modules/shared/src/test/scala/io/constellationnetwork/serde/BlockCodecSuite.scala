package io.constellationnetwork.serde

import cats.data.{NonEmptyList, NonEmptySet}

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.schema.{Block => SchemaBlock, BlockReference}
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.BlockCodec._
import io.constellationnetwork.serde.codecs.instances.BlockReferenceCodec.{codec => blockReferenceCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.signedCodec
import io.constellationnetwork.serde.codecs.instances.TransactionCodec.{codec => transactionCodec}
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import scodec.bits.ByteVector
import weaver.FunSuite

/** Round-trip + compositional byte-layout suite for `Block`.
  *
  * Block composes two collection codecs (`NonEmptyList[BlockReference]` + `NonEmptySet[Signed[Transaction]]`) and the parameterized
  * `Signed[_]` wrapper.
  *
  * Rather than a hand-authored hex golden (which would require manual arithmetic over a `Signed[Transaction]`'s proof bytes), we assert
  * that the encoded block equals the concatenation of the already-goldened primitive encodings:
  *
  * `encode(Block) == [uint16 parentCount] ++ encode(parents...) ++ [uint16 txCount] ++ encode(sortedTxs...)`
  *
  * That decomposes the structural contract onto codecs that already have strict byte goldens (`BlockReference-scodec-v1`,
  * `Transaction-scodec-v1`, and the `Signed[_]` wire layout exercised transitively).
  */
object BlockCodecSuite extends FunSuite {

  private val srcAddr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")
  private val dstAddr = AddressCodec.unsafeFromLiteral("DAG1UUPsDext9pvuoiyNTM72SX4t1xyod4Q1uXiM")

  private def parentA: BlockReference =
    BlockReference(Height(NonNegLong.unsafeFrom(10L)), ProofsHash("aa" * 32))

  private def parentB: BlockReference =
    BlockReference(Height(NonNegLong.unsafeFrom(11L)), ProofsHash("bb" * 32))

  private def tx: Transaction =
    Transaction(
      source = srcAddr,
      destination = dstAddr,
      amount = TransactionAmount(PosLong.unsafeFrom(100L)),
      fee = TransactionFee(NonNegLong.unsafeFrom(1L)),
      parent = TransactionReference(
        ordinal = TransactionOrdinal(NonNegLong.unsafeFrom(0L)),
        hash = Hash("0" * 64)
      ),
      salt = TransactionSalt(0x0102030405060708L)
    )

  private def proof: SignatureProof =
    SignatureProof(Id(Hex("cafecafe")), Signature(Hex("beefbeef")))

  private def signedTx: Signed[Transaction] =
    Signed(tx, NonEmptySet.of(proof))

  private def sample: SchemaBlock =
    SchemaBlock(
      parent = NonEmptyList.of(parentA, parentB),
      transactions = NonEmptySet.of(signedTx)
    )

  private def encodePrimitive[A](a: A, codec: scodec.Codec[A]): ByteVector =
    codec.encode(a).require.toByteVector

  test("Block round-trips through the typeclass layer") {
    val bytes = sample.immutableBytes
    expect(bytes.fromImmutableBytes[SchemaBlock] == Right(sample))
  }

  test("Block encoding begins with uint16 parent-count followed by parent bytes in insertion order") {
    val bytes = sample.immutableBytes
    val count = bytes.take(2)
    // Two parents = uint16 0x0002
    val parentBytesA = encodePrimitive(parentA, blockReferenceCodec)
    val parentBytesB = encodePrimitive(parentB, blockReferenceCodec)

    expect(count == ByteVector.fromValidHex("0002"))
      .and(expect(bytes.slice(2L, 2L + 40L) == parentBytesA))
      .and(expect(bytes.slice(2L + 40L, 2L + 80L) == parentBytesB))
  }

  test("Block transactions section begins right after parents with uint16 tx-count") {
    val bytes = sample.immutableBytes
    val afterParents = 2L + 80L // parent count + 2 × 40
    val txCount = bytes.slice(afterParents, afterParents + 2L)
    expect(txCount == ByteVector.fromValidHex("0001"))
  }

  test("Block transaction bytes equal Signed[Transaction] encoding of the only tx") {
    val bytes = sample.immutableBytes
    val afterParentsAndTxCount = 2L + 80L + 2L
    val encodedSigned = encodePrimitive(signedTx, signedCodec(transactionCodec))
    expect(bytes.drop(afterParentsAndTxCount) == encodedSigned)
  }

  test("Parent order is preserved (NonEmptyList insertion order is the contract)") {
    val swapped = sample.copy(parent = NonEmptyList.of(parentB, parentA))
    expect(sample.immutableBytes != swapped.immutableBytes)
  }

  test("Transaction set is sorted on encode (determinism — same set, same bytes)") {
    val tx2: Transaction = tx.copy(salt = TransactionSalt(0L))
    val signedTx2: Signed[Transaction] = Signed(tx2, NonEmptySet.of(proof))
    val a = sample.copy(transactions = NonEmptySet.of(signedTx, signedTx2))
    val b = sample.copy(transactions = NonEmptySet.of(signedTx2, signedTx))
    // Same mathematical set regardless of literal order → same bytes.
    expect(a.immutableBytes == b.immutableBytes)
  }
}
