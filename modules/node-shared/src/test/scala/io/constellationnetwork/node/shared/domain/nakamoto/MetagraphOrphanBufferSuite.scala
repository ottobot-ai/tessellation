package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

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
    } yield expect.eql(1, s1)
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
    } yield expect.eql(1, drainedA.length)
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
    } yield expect.eql(Some(5L), found)
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
    } yield expect.eql(None, lookup1)
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
}
