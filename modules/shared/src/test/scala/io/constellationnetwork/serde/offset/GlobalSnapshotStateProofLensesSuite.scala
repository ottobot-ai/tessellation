package io.constellationnetwork.serde.offset

import io.constellationnetwork.merkletree.MerkleRoot
import io.constellationnetwork.schema.GlobalSnapshotStateProof
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotStateProofCodec
import io.constellationnetwork.serde.codecs.offset._

import eu.timepit.refined.types.numeric.NonNegInt
import weaver.FunSuite

/** Lens suite for `GlobalSnapshotStateProof` — exercises both the fixed-offset hashes at the
  * head of the record and the dynamic-offset `mptRoot` field at the tail.
  */
object GlobalSnapshotStateProofLensesSuite extends FunSuite {

  private def h(byte: String): Hash = Hash(byte * 32)

  private def merkleRoot = MerkleRoot(NonNegInt.unsafeFrom(7), h("dd"))

  private def allAbsent = GlobalSnapshotStateProof(
    h("aa"), h("bb"), h("cc"),
    None, None, None, None, None, None, None, None, None, None, None, None, None, None
  )

  private def allPresent = GlobalSnapshotStateProof(
    h("aa"), h("bb"), h("cc"),
    Some(merkleRoot),
    Some(h("01")), Some(h("02")), Some(h("03")), Some(h("04")),
    Some(h("05")), Some(h("06")), Some(h("07")), Some(h("08")),
    Some(h("09")), Some(h("0a")), Some(h("0b")), Some(h("0c")),
    Some(h("0d"))
  )

  test("Fixed lenses read the three required hashes from both all-absent and all-present layouts") {
    val bytesA = GlobalSnapshotStateProofCodec.codec.encode(allAbsent).require.toByteVector
    val bytesP = GlobalSnapshotStateProofCodec.codec.encode(allPresent).require.toByteVector

    expect(GlobalSnapshotStateProofLenses.lastStateChannelSnapshotHashesProof.read(bytesA).require == allAbsent.lastStateChannelSnapshotHashesProof) and
      expect(GlobalSnapshotStateProofLenses.lastTxRefsProof.read(bytesA).require == allAbsent.lastTxRefsProof) and
      expect(GlobalSnapshotStateProofLenses.balancesProof.read(bytesA).require == allAbsent.balancesProof) and
      expect(GlobalSnapshotStateProofLenses.lastStateChannelSnapshotHashesProof.read(bytesP).require == allPresent.lastStateChannelSnapshotHashesProof) and
      expect(GlobalSnapshotStateProofLenses.lastTxRefsProof.read(bytesP).require == allPresent.lastTxRefsProof) and
      expect(GlobalSnapshotStateProofLenses.balancesProof.read(bytesP).require == allPresent.balancesProof)
  }

  test("mptRoot dynamic lens returns None when mptRoot is absent (all-absent layout)") {
    val bytes = GlobalSnapshotStateProofCodec.codec.encode(allAbsent).require.toByteVector
    val result = GlobalSnapshotStateProofLenses.readMptRoot(bytes).require
    expect(result.isEmpty)
  }

  test("mptRoot dynamic lens returns Some(hash) when mptRoot is present (all-present layout)") {
    val bytes = GlobalSnapshotStateProofCodec.codec.encode(allPresent).require.toByteVector
    val result = GlobalSnapshotStateProofLenses.readMptRoot(bytes).require
    expect(result.contains(h("0d")))
  }

  test("mptRoot lens works when only mptRoot is set (everything else absent)") {
    val soleMpt = allAbsent.copy(mptRoot = Some(h("42")))
    val bytes = GlobalSnapshotStateProofCodec.codec.encode(soleMpt).require.toByteVector
    val result = GlobalSnapshotStateProofLenses.readMptRoot(bytes).require
    expect(result.contains(h("42")))
  }

  test("mptRoot lens works on a mixed-presence layout") {
    val mixed = allAbsent.copy(
      lastCurrencySnapshotsProof = Some(merkleRoot),
      activeAllowSpends = Some(h("11")),
      priceState = None,
      mptRoot = Some(h("99"))
    )
    val bytes = GlobalSnapshotStateProofCodec.codec.encode(mixed).require.toByteVector
    val result = GlobalSnapshotStateProofLenses.readMptRoot(bytes).require
    expect(result.contains(h("99")))
  }

  test("Lens reads agree with full-decode reads") {
    val bytes = GlobalSnapshotStateProofCodec.codec.encode(allPresent).require.toByteVector
    val fullDecoded = GlobalSnapshotStateProofCodec.codec.decode(bytes.bits).require.value
    val lensMpt = GlobalSnapshotStateProofLenses.readMptRoot(bytes).require
    expect(lensMpt == fullDecoded.mptRoot)
  }
}
