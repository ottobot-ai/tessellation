package io.constellationnetwork.dag.l0

import cats.effect.IO

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash

import io.circe.Encoder
import weaver.SimpleIOSuite

object FreshGenesisHasherBoundarySuite extends SimpleIOSuite {

  private def tagged(logic: io.constellationnetwork.security.HashLogic): Hasher[IO] =
    new Hasher[IO] {
      def hash[A: Encoder](data: A): IO[Hash] = IO.raiseError(new UnsupportedOperationException)
      def hashBytes(bytes: Array[Byte]): IO[Hash] = IO.raiseError(new UnsupportedOperationException)
      def compare[A: Encoder](data: A, expectedHash: Hash): IO[Boolean] =
        IO.raiseError(new UnsupportedOperationException)
      def getLogic(ordinal: SnapshotOrdinal): io.constellationnetwork.security.HashLogic = logic
      def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): IO[Hash] =
        IO.raiseError(new UnsupportedOperationException)
    }

  test("fresh genesis selects the genesis and first-live hash eras independently by ordinal") {
    val genesisHasher = tagged(KryoHash)
    val firstLiveHasher = tagged(JsonHash)
    val selector = new HasherSelector[IO] {
      def getCurrent: Hasher[IO] = firstLiveHasher
      def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[IO] =
        if (ordinal == SnapshotOrdinal.MinValue) genesisHasher else firstLiveHasher
    }

    for {
      genesisSelected <- FreshGenesisHasherBoundary.atGenesis(selector, SnapshotOrdinal.MinValue)(h => IO.pure(h eq genesisHasher))
      firstLiveSelected <- FreshGenesisHasherBoundary.atFirstLive(selector, SnapshotOrdinal.MinValue)(h => IO.pure(h eq firstLiveHasher))
    } yield expect(genesisSelected) && expect(firstLiveSelected)
  }
}
