package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.CombinedSnapshotCheckpointFileSystemStorage
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.storages.MptStateStorage

import fs2.io.file.Files
import weaver.MutableIOSuite

/** Signed-byte-store backfill SERVE side (2026-07-09) — the by-ordinal `mptEntriesAt` contract of [[FinalizedSnapshotReader]]:
  *
  *   1. WIRE ROUND-TRIP — a stored byte map at a finalized ordinal is served as JSON that `MptStateStorage.mptEntriesDecoder` (the exact
  *      decoder `SnapshotClient.getMptEntriesAt` uses) decodes back to the byte-identical map. This is the codec contract the
  *      `PinnedByteBackfill` puller's root-verify runs over.
  *   1. HOLE ⇒ `None` — an ordinal the signed store has no bytes for 404s, so the puller just tries the next peer (indistinguishable from
  *      "can't serve", fail-closed end to end).
  *   1. FINALITY GATE ⇒ `None` — an ordinal past the finalized watermark is NEVER served (the "only finalized data leaves the node"
  *      invariant, belt-and-braces with the route's own `isOrdinalServable` gate).
  *   1. NO BYTE STORE ⇒ `None` — non-global Nakamoto readers (constructed without an `MptStateStorage`) and the BFT reader keep their
  *      routes byte-identical (404).
  */
object FinalizedSnapshotReaderMptEntriesAtSuite extends MutableIOSuite {

  type Res = JsonSerializer[IO]

  override def sharedResource: Resource[IO, Res] = JsonSerializer.forAsync[IO].asResource

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal.unsafeApply(n)

  private val entries: Map[Hex, Array[Byte]] = Map(
    Hex("01aa") -> Array[Byte](1, 2, 3),
    Hex("02bb") -> Array[Byte](4, 5)
  )

  private def mkReader(
    dir: fs2.io.file.Path,
    finalized: Long,
    withByteStore: Boolean
  )(implicit js: JsonSerializer[IO]): IO[(FinalizedSnapshotReader[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo], MptStateStorage[IO])] =
    for {
      fileStorage <- CombinedSnapshotCheckpointFileSystemStorage.make[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo](dir / "checkpoints")
      byteStore <- MptStateStorage.make[IO](dir / "signed")
      gateRef <- Ref.of[IO, SnapshotOrdinal](ord(finalized))
      reader = FinalizedSnapshotReader.nakamoto[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
        FinalityGate.fromRef(gateRef),
        fileStorage,
        if (withByteStore) byteStore.some else None
      )
    } yield (reader, byteStore)

  test("WIRE ROUND-TRIP: served JSON at a finalized ordinal decodes (client codec) back to the byte-identical stored map") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReader(dir, finalized = 100L, withByteStore = true)
        (reader, byteStore) = rs
        _ <- byteStore.writeState(ord(42L), entries)
        respOpt <- reader.mptEntriesAt(ord(42L))
        body <- respOpt.traverse(_.bodyText.compile.string)
        decoded = body.map(s => io.circe.parser.decode(s)(MptStateStorage.mptEntriesDecoder))
      } yield
        expect(respOpt.isDefined) &&
          expect(decoded.exists(_.isRight)) &&
          expect(
            decoded.exists(_.exists { m =>
              m.keySet == entries.keySet && entries.forall { case (k, v) => m.get(k).exists(_.sameElements(v)) }
            })
          )
    }
  }

  test("HOLE: no bytes at the requested (finalized) ordinal ⇒ None (puller tries the next peer)") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReader(dir, finalized = 100L, withByteStore = true)
        (reader, _) = rs
        respOpt <- reader.mptEntriesAt(ord(42L))
      } yield expect(respOpt.isEmpty)
    }
  }

  test("FINALITY GATE: an ordinal past the finalized watermark is never served, even when bytes exist") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReader(dir, finalized = 10L, withByteStore = true)
        (reader, byteStore) = rs
        _ <- byteStore.writeState(ord(42L), entries) // present on disk but ABOVE finalized=10
        respOpt <- reader.mptEntriesAt(ord(42L))
      } yield expect(respOpt.isEmpty)
    }
  }

  test("NO BYTE STORE: a reader constructed without an MptStateStorage (non-global layer) always answers None") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReader(dir, finalized = 100L, withByteStore = false)
        (reader, byteStore) = rs
        _ <- byteStore.writeState(ord(42L), entries) // bytes exist on disk, but the reader has no store wired
        respOpt <- reader.mptEntriesAt(ord(42L))
      } yield expect(respOpt.isEmpty)
    }
  }
}
