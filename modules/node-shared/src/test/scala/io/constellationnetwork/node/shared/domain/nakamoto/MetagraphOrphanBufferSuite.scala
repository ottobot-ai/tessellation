package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.auto._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.SimpleIOSuite

object MetagraphOrphanBufferSuite extends SimpleIOSuite {

  private val logger = Slf4jLogger.getLoggerFromName[IO]("MetagraphOrphanBufferSuite")

  // Two distinct DAG addresses with valid checksums; for the buffer's logic only Address equality matters.
  private val mgA = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWB")
  private val mgB = Address("DAG07tqNLYW8jHU9emXcRTT3CfgCUoumwcLghopd")

  private def mkHash(seed: String): Hash =
    Hash(seed.padTo(64, '0').take(64))

  private def mkBytes(n: Int): Array[Byte] = Array.tabulate(64)(i => ((i + n) % 256).toByte)

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def mkAtt(
    peer: String,
    binaryHash: Hash,
    parent: Hash = mkHash("p0"),
    mg: Address = mgA
  ): MetagraphCommitteeGate.IncomingAttestation =
    MetagraphCommitteeGate.IncomingAttestation(
      senderPeerId = pid(peer),
      senderVrfVk = mkBytes(1),
      metagraphAddress = mg,
      parentHash = parent,
      binaryHash = binaryHash,
      committeeVrfProof = mkBytes(2),
      longTermSignature = mkBytes(3),
      kesSignature = mkBytes(4),
      senderTreeStep = 0
    )

  // ─── #29 verify-on-attach: Phase-0 un-verified attestation buffer ───────────────────────────
  // `bufferAttestation` holds inbound attestations for binaries we cannot yet attach (orphan tine / not-yet-
  // arrived); `drainAttestations` releases them at the attach point for Phase-1 verification. No crypto in the
  // buffer — these tests assert the bookkeeping (order, isolation, idempotency, FIFO cap) only.

  test("bufferAttestation + drainAttestations — held un-verified, released in arrival order; buffer emptied") {
    val bin = mkHash("b1n1")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.bufferAttestation(mgA, bin, mkAtt("peer-1", bin))
      _ <- buf.bufferAttestation(mgA, bin, mkAtt("peer-2", bin))
      sizeBefore <- buf.attestationsBufferSize
      drained <- buf.drainAttestations(mgA, bin)
      sizeAfter <- buf.attestationsBufferSize
    } yield
      expect
        .eql(2, sizeBefore)
        .and(expect.eql(2, drained.length))
        .and(expect(drained.map(_.senderPeerId) == List(pid("peer-1"), pid("peer-2"))))
        .and(expect.eql(0, sizeAfter))
  }

  test("drainAttestations — absent (mg, binary) returns empty and leaves the buffer intact") {
    val bin = mkHash("b2n2")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.bufferAttestation(mgA, bin, mkAtt("peer-1", bin))
      drainedOtherBin <- buf.drainAttestations(mgA, mkHash("nope"))
      drainedOtherMg <- buf.drainAttestations(mgB, bin)
      sizeAfter <- buf.attestationsBufferSize
    } yield
      expect
        .eql(0, drainedOtherBin.length)
        .and(expect.eql(0, drainedOtherMg.length))
        .and(expect.eql(1, sizeAfter))
  }

  test("bufferAttestation — idempotent per senderPeerId (gossip re-delivery not re-buffered)") {
    val bin = mkHash("b3n3")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.bufferAttestation(mgA, bin, mkAtt("peer-1", bin))
      _ <- buf.bufferAttestation(mgA, bin, mkAtt("peer-1", bin)) // gossip re-delivery — same sender, dropped
      _ <- buf.bufferAttestation(mgA, bin, mkAtt("peer-2", bin)) // distinct sender — kept
      size <- buf.attestationsBufferSize
      drained <- buf.drainAttestations(mgA, bin)
    } yield
      expect
        .eql(2, size)
        .and(expect(drained.map(_.senderPeerId).toSet == Set(pid("peer-1"), pid("peer-2"))))
  }

  test("bufferAttestation — FIFO cap evicts the oldest attestation across binaries") {
    val cap = 3
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger, attestationsCap = cap)
      _ <- buf.bufferAttestation(mgA, mkHash("c1"), mkAtt("p1", mkHash("c1")))
      _ <- buf.bufferAttestation(mgA, mkHash("c2"), mkAtt("p2", mkHash("c2")))
      _ <- buf.bufferAttestation(mgA, mkHash("c3"), mkAtt("p3", mkHash("c3")))
      _ <- buf.bufferAttestation(mgA, mkHash("c4"), mkAtt("p4", mkHash("c4"))) // overflow → evict oldest (c1)
      size <- buf.attestationsBufferSize
      c1 <- buf.drainAttestations(mgA, mkHash("c1")) // oldest — evicted
      c4 <- buf.drainAttestations(mgA, mkHash("c4")) // newest — retained
    } yield
      expect
        .eql(cap, size)
        .and(expect.eql(0, c1.length))
        .and(expect.eql(1, c4.length))
  }

  test("record + drainChildren — orphans drained in chronological order") {
    val parent = mkHash("aaaa")
    val b1 = mkBytes(1)
    val b2 = mkBytes(2)
    val b3 = mkBytes(3)
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      s1 <- buf.record(mgA, parent, b1)
      s2 <- buf.record(mgA, parent, b2)
      s3 <- buf.record(mgA, parent, b3)
      drained <- buf.drainChildren(mgA, parent)
      sizeAfter <- buf.size
    } yield
      expect
        .eql(1, s1)
        .and(expect.eql(2, s2))
        .and(expect.eql(3, s3))
        .and(expect.eql(3, drained.length))
        .and(expect(java.util.Arrays.equals(drained(0), b1)))
        .and(expect(java.util.Arrays.equals(drained(1), b2)))
        .and(expect(java.util.Arrays.equals(drained(2), b3)))
        .and(expect.eql(0, sizeAfter))
  }

  test("drainChildren — wrong parent returns empty list") {
    val parent = mkHash("bbbb")
    val other = mkHash("cccc")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, parent, mkBytes(1))
      drained <- buf.drainChildren(mgA, other)
      sizeAfter <- buf.size
    } yield expect.eql(0, drained.length).and(expect.eql(1, sizeAfter))
  }

  test("drainChildren — wrong metagraph returns empty list") {
    val parent = mkHash("dddd")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, parent, mkBytes(1))
      drainedB <- buf.drainChildren(mgB, parent)
      drainedA <- buf.drainChildren(mgA, parent)
    } yield expect.eql(0, drainedB.length).and(expect.eql(1, drainedA.length))
  }

  // #213/#290 liveness: pending-parent-ordinal cache (keyed on the WIRE hash == `att.binaryHash`). Lets the
  // committee-attestation receiver recover an IN-FLIGHT (non-buffered) binary's parent ordinal — the case
  // `peekForWireHash` cannot serve, and the reason the committee threshold was previously reached zero times.
  test("pending-parent-ordinal — record then lookup by wire hash returns the parent ordinal") {
    val wireHash = mkHash("e1e1")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      before <- buf.lookupPendingParentOrdinal(mgA, wireHash)
      _ <- buf.recordPendingParentOrdinal(mgA, wireHash, 41L)
      after <- buf.lookupPendingParentOrdinal(mgA, wireHash)
    } yield expect(before.isEmpty).and(expect(after.contains(41L)))
  }

  test("pending-parent-ordinal — keyed by (metagraph, wireHash); no cross-talk") {
    val wireHash = mkHash("f2f2")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.recordPendingParentOrdinal(mgA, wireHash, 7L)
      sameKey <- buf.lookupPendingParentOrdinal(mgA, wireHash)
      otherMg <- buf.lookupPendingParentOrdinal(mgB, wireHash)
      otherHash <- buf.lookupPendingParentOrdinal(mgA, mkHash("9999"))
    } yield
      expect(sameKey.contains(7L))
        .and(expect(otherMg.isEmpty))
        .and(expect(otherHash.isEmpty))
  }

  test("pending-parent-ordinal — last write wins for the same wire hash") {
    val wireHash = mkHash("d3d3")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.recordPendingParentOrdinal(mgA, wireHash, 1L)
      _ <- buf.recordPendingParentOrdinal(mgA, wireHash, 2L)
      got <- buf.lookupPendingParentOrdinal(mgA, wireHash)
    } yield expect(got.contains(2L))
  }

  test("pending-parent-ordinal — FIFO eviction past the admissions cap drops the oldest") {
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger, admissionsCap = 2)
      _ <- buf.recordPendingParentOrdinal(mgA, mkHash("0001"), 10L)
      _ <- buf.recordPendingParentOrdinal(mgA, mkHash("0002"), 20L)
      _ <- buf.recordPendingParentOrdinal(mgA, mkHash("0003"), 30L)
      first <- buf.lookupPendingParentOrdinal(mgA, mkHash("0001"))
      second <- buf.lookupPendingParentOrdinal(mgA, mkHash("0002"))
      third <- buf.lookupPendingParentOrdinal(mgA, mkHash("0003"))
    } yield
      expect(first.isEmpty)
        .and(expect(second.contains(20L)))
        .and(expect(third.contains(30L)))
  }

  test("record is idempotent on identical (mg, parent, bytes) triple") {
    val parent = mkHash("eeee")
    val bytes = mkBytes(42)
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      s1 <- buf.record(mgA, parent, bytes)
      s2 <- buf.record(mgA, parent, bytes)
      s3 <- buf.record(mgA, parent, bytes)
    } yield expect.eql(1, s1).and(expect.eql(1, s2)).and(expect.eql(1, s3))
  }

  test("FIFO cap evicts oldest across all metagraphs") {
    val parent = mkHash("ffff")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger, cap = 2)
      _ <- buf.record(mgA, parent, mkBytes(1))
      _ <- buf.record(mgA, parent, mkBytes(2))
      _ <- buf.record(mgB, parent, mkBytes(3))
      // Now cap=2 → oldest (mgA-bytes(1)) evicted.
      // Buffer holds: (mgA, parent) → [bytes(2)], (mgB, parent) → [bytes(3)]
      drainedA <- buf.drainChildren(mgA, parent)
      drainedB <- buf.drainChildren(mgB, parent)
    } yield
      expect
        .eql(1, drainedA.length)
        .and(expect(java.util.Arrays.equals(drainedA.head, mkBytes(2))))
        .and(expect.eql(1, drainedB.length))
        .and(expect(java.util.Arrays.equals(drainedB.head, mkBytes(3))))
  }

  test("recordAdmission + lookupAdmittedOrd round-trip") {
    val valueHash = mkHash("1234")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.recordAdmission(mgA, valueHash, 5L)
      found <- buf.lookupAdmittedOrd(mgA, valueHash)
      missing <- buf.lookupAdmittedOrd(mgB, valueHash)
      otherHash <- IO.pure(mkHash("5678"))
      missing2 <- buf.lookupAdmittedOrd(mgA, otherHash)
      sz <- buf.admissionsSize
    } yield
      expect
        .eql(Some(5L), found)
        .and(expect.eql(None, missing))
        .and(expect.eql(None, missing2))
        .and(expect.eql(1, sz))
  }

  test("admission cap evicts oldest entries first") {
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger, admissionsCap = 2)
      h1 <- IO.pure(mkHash("h1"))
      h2 <- IO.pure(mkHash("h2"))
      h3 <- IO.pure(mkHash("h3"))
      _ <- buf.recordAdmission(mgA, h1, 1L)
      _ <- buf.recordAdmission(mgA, h2, 2L)
      _ <- buf.recordAdmission(mgA, h3, 3L)
      // h1 should be evicted
      lookup1 <- buf.lookupAdmittedOrd(mgA, h1)
      lookup2 <- buf.lookupAdmittedOrd(mgA, h2)
      lookup3 <- buf.lookupAdmittedOrd(mgA, h3)
      sz <- buf.admissionsSize
    } yield
      expect
        .eql(None, lookup1)
        .and(expect.eql(Some(2L), lookup2))
        .and(expect.eql(Some(3L), lookup3))
        .and(expect.eql(2, sz))
  }

  test("re-admission updates the cached ordinal") {
    val h = mkHash("abcd")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.recordAdmission(mgA, h, 1L)
      _ <- buf.recordAdmission(mgA, h, 7L)
      lookup <- buf.lookupAdmittedOrd(mgA, h)
    } yield expect.eql(Some(7L), lookup)
  }

  test("size counts only orphan entries, not admissions") {
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, mkHash("p1"), mkBytes(1))
      _ <- buf.recordAdmission(mgA, mkHash("v1"), 1L)
      orphans <- buf.size
      admissions <- buf.admissionsSize
    } yield expect.eql(1, orphans).and(expect.eql(1, admissions))
  }

  // ─── #259: listPendingParents + non-destructive peekForValueHash ───────────────────────────

  test("listPendingParents — distinct (mg, parentHash) keys currently buffered") {
    val p1 = mkHash("p1")
    val p2 = mkHash("p2")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, p1, mkBytes(1))
      _ <- buf.record(mgA, p1, mkBytes(2)) // same key, 2 entries → still ONE pending key
      _ <- buf.record(mgA, p2, mkBytes(3))
      _ <- buf.record(mgB, p1, mkBytes(4))
      pending <- buf.listPendingParents
    } yield
      expect
        .eql(3, pending.size)
        .and(expect(pending.toSet == Set((mgA, p1), (mgA, p2), (mgB, p1))))
  }

  test("listPendingParents — empty when nothing buffered (admissions don't count)") {
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.recordAdmission(mgA, mkHash("v1"), 1L)
      pending <- buf.listPendingParents
    } yield expect.eql(0, pending.size)
  }

  test("listPendingParents — a drained parent no longer appears") {
    val p1 = mkHash("p1")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, p1, mkBytes(1))
      before <- buf.listPendingParents
      _ <- buf.drainChildren(mgA, p1)
      after <- buf.listPendingParents
    } yield expect.eql(1, before.size).and(expect.eql(0, after.size))
  }

  test("peekForValueHash — finds the matching bytes NON-DESTRUCTIVELY") {
    val parent = mkHash("parent")
    val vh1 = mkHash("vh1")
    val vh2 = mkHash("vh2")
    val b1 = mkBytes(10)
    val b2 = mkBytes(20)
    // Fake decode: each byte array maps to a known value-hash. Real serve handler does
    // JsonSerializer.deserialize + Signed.toHashed.map(_.hash); here we stub it deterministically.
    def valueHashOf(bytes: Array[Byte]): IO[Option[Hash]] =
      IO.pure {
        if (java.util.Arrays.equals(bytes, b1)) Some(vh1)
        else if (java.util.Arrays.equals(bytes, b2)) Some(vh2)
        else None
      }
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, parent, b1)
      _ <- buf.record(mgA, parent, b2)
      sizeBefore <- buf.size
      found2 <- buf.peekForValueHash(mgA, vh2)(valueHashOf)
      found1 <- buf.peekForValueHash(mgA, vh1)(valueHashOf)
      sizeAfter <- buf.size
    } yield
      expect(found1.exists(java.util.Arrays.equals(_, b1)))
        .and(expect(found2.exists(java.util.Arrays.equals(_, b2))))
        // NON-DESTRUCTIVE: two peeks did not change the buffer size.
        .and(expect.eql(2, sizeBefore))
        .and(expect.eql(2, sizeAfter))
  }

  test("peekForValueHash — miss returns None and leaves the buffer unchanged") {
    val parent = mkHash("parent")
    val b1 = mkBytes(10)
    val wantedButAbsent = mkHash("absent")
    def valueHashOf(bytes: Array[Byte]): IO[Option[Hash]] =
      IO.pure(if (java.util.Arrays.equals(bytes, b1)) Some(mkHash("vh1")) else None)
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, parent, b1)
      result <- buf.peekForValueHash(mgA, wantedButAbsent)(valueHashOf)
      sizeAfter <- buf.size
    } yield expect(result.isEmpty).and(expect.eql(1, sizeAfter))
  }

  test("peekForValueHash — scopes to the requested metagraph (other mg's bytes not returned)") {
    val parent = mkHash("parent")
    val vh = mkHash("vh")
    val bA = mkBytes(10)
    val bB = mkBytes(20)
    // BOTH byte arrays would hash to the SAME value-hash; only mgA's must be returned for an mgA query.
    def valueHashOf(@annotation.unused bytes: Array[Byte]): IO[Option[Hash]] = IO.pure(Some(vh))
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, parent, bA)
      _ <- buf.record(mgB, parent, bB)
      foundA <- buf.peekForValueHash(mgA, vh)(valueHashOf)
      foundB <- buf.peekForValueHash(mgB, vh)(valueHashOf)
    } yield
      expect(foundA.exists(java.util.Arrays.equals(_, bA)))
        .and(expect(foundB.exists(java.util.Arrays.equals(_, bB))))
  }

  // ─── #213/#290: peekForWireHash — the receiver-side content lookup for handleMetagraphAttestation ──
  //
  // The attestation receiver looks up the ATTESTED binary by the WIRE digest the attestation carries
  // (`att.binaryHash` == `Hasher.hashBytes(wireBytes)`) so it can derive the parent ordinal from that
  // binary's content. Distinct buffered binaries (even under different parents) are disambiguated by
  // their wire digest, NON-DESTRUCTIVELY, and a miss returns None so the receiver FAILS CLOSED.

  test("peekForWireHash — finds the matching bytes by wire digest NON-DESTRUCTIVELY") {
    val parent = mkHash("parent")
    val wh1 = mkHash("wh1")
    val wh2 = mkHash("wh2")
    val b1 = mkBytes(11)
    val b2 = mkBytes(22)
    def wireHashOf(bytes: Array[Byte]): IO[Option[Hash]] =
      IO.pure {
        if (java.util.Arrays.equals(bytes, b1)) Some(wh1)
        else if (java.util.Arrays.equals(bytes, b2)) Some(wh2)
        else None
      }
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, parent, b1)
      _ <- buf.record(mgA, parent, b2)
      sizeBefore <- buf.size
      found2 <- buf.peekForWireHash(mgA, wh2)(wireHashOf)
      found1 <- buf.peekForWireHash(mgA, wh1)(wireHashOf)
      sizeAfter <- buf.size
    } yield
      expect(found1.exists(java.util.Arrays.equals(_, b1)))
        .and(expect(found2.exists(java.util.Arrays.equals(_, b2))))
        .and(expect.eql(2, sizeBefore))
        .and(expect.eql(2, sizeAfter))
  }

  test("peekForWireHash — miss returns None (receiver fails closed) and leaves the buffer unchanged") {
    val parent = mkHash("parent")
    val b1 = mkBytes(11)
    val wantedButAbsent = mkHash("absent-wire")
    def wireHashOf(bytes: Array[Byte]): IO[Option[Hash]] =
      IO.pure(if (java.util.Arrays.equals(bytes, b1)) Some(mkHash("wh1")) else None)
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, parent, b1)
      result <- buf.peekForWireHash(mgA, wantedButAbsent)(wireHashOf)
      sizeAfter <- buf.size
    } yield expect(result.isEmpty).and(expect.eql(1, sizeAfter))
  }

  test("peekForWireHash — scopes to the requested metagraph") {
    val parent = mkHash("parent")
    val wh = mkHash("wh")
    val bA = mkBytes(11)
    val bB = mkBytes(22)
    def wireHashOf(@annotation.unused bytes: Array[Byte]): IO[Option[Hash]] = IO.pure(Some(wh))
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      _ <- buf.record(mgA, parent, bA)
      _ <- buf.record(mgB, parent, bB)
      foundA <- buf.peekForWireHash(mgA, wh)(wireHashOf)
      foundB <- buf.peekForWireHash(mgB, wh)(wireHashOf)
    } yield
      expect(foundA.exists(java.util.Arrays.equals(_, bA)))
        .and(expect(foundB.exists(java.util.Arrays.equals(_, bB))))
  }
}
