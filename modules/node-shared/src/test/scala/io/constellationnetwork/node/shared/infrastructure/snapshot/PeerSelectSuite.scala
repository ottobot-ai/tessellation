package io.constellationnetwork.node.shared.infrastructure.snapshot

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.applicative._

import weaver.SimpleIOSuite

object PeerSelectSuite extends SimpleIOSuite {

  test("hash selection is restricted to the unique largest ordinal cohort") {
    val ordinalObservations = NonEmptyList.of(
      "peer-a" -> 100L,
      "peer-b" -> 100L,
      "minority-peer" -> 99L
    )

    val result = for {
      (selectedOrdinal, ordinalCohort) <- PeerSelect.uniqueLargestCohort(ordinalObservations)
      hashObservations = ordinalCohort.map(peer => peer -> s"hash-at-$selectedOrdinal")
      (selectedHash, hashCohort) <- PeerSelect.uniqueLargestCohort(hashObservations)
    } yield (selectedOrdinal, selectedHash, hashCohort.toList)

    expect.same(Right((100L, "hash-at-100", List("peer-a", "peer-b"))), result).pure[IO]
  }

  test("equal largest cohorts reject instead of selecting by map iteration order") {
    val observations = NonEmptyList.of(
      "peer-a" -> 100L,
      "peer-b" -> 100L,
      "peer-c" -> 99L,
      "peer-d" -> 99L
    )

    expect.same(Left(PeerSelect.AmbiguousPeerCohort), PeerSelect.uniqueLargestCohort(observations)).pure[IO]
  }
}
