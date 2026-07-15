package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.util.concurrent.CountDownLatch

import cats.effect.syntax.all._
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.http.routes.SnapshotRoutesActiveEraSuite
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.CombinedSnapshotCheckpointFileSystemStorage
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.signature.Signed

import fs2.Stream
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

  private def mkReaderWithStorage(
    dir: fs2.io.file.Path,
    finalized: Long,
    withByteStore: Boolean,
    activeEraSnapshot: Boolean = true
  )(
    implicit js: JsonSerializer[IO]
  ): IO[
    (
      FinalizedSnapshotReader[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
      MptStateStorage[IO],
      CombinedSnapshotCheckpointFileSystemStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo]
    )
  ] =
    for {
      fileStorage <- CombinedSnapshotCheckpointFileSystemStorage.make[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
        dir / "checkpoints"
      )
      byteStore <- MptStateStorage.make[IO](dir / "signed")
      gateRef <- Ref.of[IO, SnapshotOrdinal](ord(finalized))
      reader = FinalizedSnapshotReader.nakamoto[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
        FinalityGate.fromRef(gateRef),
        fileStorage,
        _ => activeEraSnapshot.pure[IO],
        if (withByteStore) byteStore.some else None
      )
    } yield (reader, byteStore, fileStorage)

  private def mkReader(
    dir: fs2.io.file.Path,
    finalized: Long,
    withByteStore: Boolean,
    activeEraSnapshot: Boolean = true
  )(
    implicit js: JsonSerializer[IO]
  ): IO[(FinalizedSnapshotReader[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo], MptStateStorage[IO])] =
    mkReaderWithStorage(dir, finalized, withByteStore, activeEraSnapshot).map {
      case (reader, byteStore, _) =>
        (reader, byteStore)
    }

  private val checkpointInfo = GlobalSnapshotInfo.empty
  private val olderOrdinal = ord(11L)
  private val selectedOrdinal = SnapshotRoutesActiveEraSuite.ordinal

  private val olderSnapshot = SnapshotRoutesActiveEraSuite.signed(
    SnapshotRoutesActiveEraSuite.snapshot.copy(ordinal = olderOrdinal)
  )

  private val forbiddenSelectedSnapshot = SnapshotRoutesActiveEraSuite.signed(
    SnapshotRoutesActiveEraSuite.snapshot.copy(
      stateProof = SnapshotRoutesActiveEraSuite.proof.copy(smtRoot = Hash("ef" * 32).some)
    )
  )

  private def writeOlderCheckpoint(
    fileStorage: CombinedSnapshotCheckpointFileSystemStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo]
  ): IO[Unit] =
    fileStorage.tryWrite(olderOrdinal, olderSnapshot, checkpointInfo, Hash.empty)

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

  test("ACTIVE ERA GATE: signed bytes are not served when the exact snapshot ordinal has a forbidden active-era shape") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReader(dir, finalized = 100L, withByteStore = true, activeEraSnapshot = false)
        (reader, byteStore) = rs
        _ <- byteStore.writeState(ord(42L), entries)
        respOpt <- reader.mptEntriesAt(ord(42L))
      } yield expect(respOpt.isEmpty)
    }
  }

  test("EXACT FORBIDDEN CHECKPOINT: latest combined fails closed instead of falling back to an older valid checkpoint") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (reader, _, fileStorage) = rs
        _ <- writeOlderCheckpoint(fileStorage)
        _ <- fileStorage.tryWrite(selectedOrdinal, forbiddenSelectedSnapshot, checkpointInfo, Hash.empty)
        respOpt <- reader.latestCombinedResponse
      } yield expect(respOpt.isEmpty)
    }
  }

  test("VALID EXACT CHECKPOINT: latest combined streams the structurally validated two-element file") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (reader, _, fileStorage) = rs
        _ <- fileStorage.tryWrite(
          selectedOrdinal,
          SnapshotRoutesActiveEraSuite.signed(SnapshotRoutesActiveEraSuite.snapshot),
          checkpointInfo,
          Hash.empty
        )
        respOpt <- reader.latestCombinedResponse
        body <- respOpt.traverse(_.bodyText.compile.string)
        elements = body.flatMap(io.circe.parser.parse(_).toOption).flatMap(_.asArray)
      } yield expect(respOpt.isDefined) && expect(elements.exists(_.size === 2))
    }
  }

  test("SAME-HANDLE CAPTURE: a same-ordinal atomic replacement cannot change an already validated response body") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (reader, _, fileStorage) = rs
        valid = SnapshotRoutesActiveEraSuite.signed(SnapshotRoutesActiveEraSuite.snapshot)
        _ <- fileStorage.tryWrite(selectedOrdinal, valid, checkpointInfo, Hash.empty)
        captured <- reader.latestCombinedResponse
        _ <- fileStorage.tryWrite(selectedOrdinal, forbiddenSelectedSnapshot, checkpointInfo, Hash.empty)
        capturedBody <- captured.traverse(_.bodyText.compile.string)
        capturedSigned = capturedBody
          .flatMap(io.circe.parser.parse(_).toOption)
          .flatMap(_.asArray)
          .flatMap(_.headOption)
          .flatMap(_.as[Signed[GlobalIncrementalSnapshot]].toOption)
        replacement <- reader.latestCombinedResponse
      } yield
        expect(capturedSigned.exists(_.value.stateProof.smtRoot.isEmpty)) &&
          expect(replacement.isEmpty)
    }
  }

  test("MALFORMED EXACT CHECKPOINT: latest combined fails closed instead of falling back to an older valid checkpoint") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (reader, _, fileStorage) = rs
        _ <- writeOlderCheckpoint(fileStorage)
        malformed = Stream.emits("[{\"not\":\"a signed snapshot\"},".getBytes("UTF-8")).covary[IO]
        _ <- malformed.through(Files[IO].writeAll(dir / "checkpoints" / selectedOrdinal.value.value.toString)).compile.drain
        respOpt <- reader.latestCombinedResponse
      } yield expect(respOpt.isEmpty)
    }
  }

  test("MISSING SELECTED BYTES: a forbidden exact checkpoint cannot fall back to an older common checkpoint") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (reader, byteStore, fileStorage) = rs
        _ <- writeOlderCheckpoint(fileStorage)
        _ <- byteStore.writeState(olderOrdinal, entries)
        _ <- fileStorage.tryWrite(selectedOrdinal, forbiddenSelectedSnapshot, checkpointInfo, Hash.empty)
        respOpt <- reader.latestMptEntriesResponse
      } yield expect(respOpt.isEmpty)
    }
  }

  test("VALID MPT TRIPLE: transformed response is exactly snapshot, info, and the byte-identical entries map") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (reader, byteStore, fileStorage) = rs
        _ <- fileStorage.tryWrite(
          selectedOrdinal,
          SnapshotRoutesActiveEraSuite.signed(SnapshotRoutesActiveEraSuite.snapshot),
          checkpointInfo,
          Hash.empty
        )
        _ <- byteStore.writeState(selectedOrdinal, entries)
        respOpt <- reader.latestMptEntriesResponse
        body <- respOpt.traverse(_.bodyText.compile.string)
        elements = body.flatMap(io.circe.parser.parse(_).toOption).flatMap(_.asArray)
        decodedEntries = elements.flatMap(_.lift(2)).map(_.as(MptStateStorage.mptEntriesDecoder))
      } yield
        expect(respOpt.isDefined) &&
          expect(elements.exists(_.size === 3)) &&
          expect(decodedEntries.exists(_.exists { decoded =>
            decoded.keySet === entries.keySet && entries.forall { case (key, value) => decoded.get(key).exists(_.sameElements(value)) }
          }))
    }
  }

  test("VALIDATED RESPONSE RESOURCE: a throwing transform releases its handle and stream permit") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (_, _, fileStorage) = rs
        _ <- fileStorage.tryWrite(
          selectedOrdinal,
          SnapshotRoutesActiveEraSuite.signed(SnapshotRoutesActiveEraSuite.snapshot),
          checkpointInfo,
          Hash.empty
        )
        failed <- fileStorage.getAsValidatedHttpResponse(
          selectedOrdinal,
          _ => true.pure[IO],
          _ => throw new RuntimeException("transform failed")
        )
        held <- List
          .fill(5)(())
          .parTraverse(_ => fileStorage.getAsValidatedHttpResponse(selectedOrdinal, _ => true.pure[IO]))
          .timeout(3.seconds)
        _ <- held.flatten.traverse_(_.body.compile.drain)
      } yield expect(failed.isEmpty) && expect(held.forall(_.isDefined))
    }
  }

  test("VALIDATED RESPONSE RESOURCE: cancelling validation releases its handle and stream permit") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (_, _, fileStorage) = rs
        _ <- fileStorage.tryWrite(
          selectedOrdinal,
          SnapshotRoutesActiveEraSuite.signed(SnapshotRoutesActiveEraSuite.snapshot),
          checkpointInfo,
          Hash.empty
        )
        fiber <- fileStorage.getAsValidatedHttpResponse(selectedOrdinal, _ => IO.never[Boolean]).start
        _ <- IO.sleep(100.millis)
        _ <- fiber.cancel
        held <- List
          .fill(5)(())
          .parTraverse(_ => fileStorage.getAsValidatedHttpResponse(selectedOrdinal, _ => true.pure[IO]))
          .timeout(3.seconds)
        _ <- held.flatten.traverse_(_.body.compile.drain)
      } yield expect(held.forall(_.isDefined))
    }
  }

  test("VALIDATED RESPONSE RESOURCE: a request blocked on the stream-permit cap remains cancelable") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (_, _, fileStorage) = rs
        _ <- fileStorage.tryWrite(
          selectedOrdinal,
          SnapshotRoutesActiveEraSuite.signed(SnapshotRoutesActiveEraSuite.snapshot),
          checkpointInfo,
          Hash.empty
        )
        held <- List.fill(5)(()).traverse(_ => fileStorage.getAsValidatedHttpResponse(selectedOrdinal, _ => true.pure[IO]))
        blocked <- fileStorage.getAsValidatedHttpResponse(selectedOrdinal, _ => true.pure[IO]).start
        _ <- IO.sleep(100.millis)
        _ <- blocked.cancel.timeout(3.seconds)
        _ <- held.flatten.traverse_(_.body.compile.drain)
      } yield expect(held.forall(_.isDefined))
    }
  }

  test("VALIDATED RESPONSE RESOURCE: cancellation after validation cannot leak before response handoff") { implicit js =>
    Files[IO].tempDirectory.use { dir =>
      val transformEntered = new CountDownLatch(1)
      val allowTransformReturn = new CountDownLatch(1)

      for {
        rs <- mkReaderWithStorage(dir, finalized = selectedOrdinal.value.value, withByteStore = true)
        (_, _, fileStorage) = rs
        _ <- fileStorage.tryWrite(
          selectedOrdinal,
          SnapshotRoutesActiveEraSuite.signed(SnapshotRoutesActiveEraSuite.snapshot),
          checkpointInfo,
          Hash.empty
        )
        fiber <- fileStorage
          .getAsValidatedHttpResponse(
            selectedOrdinal,
            _ => true.pure[IO],
            bytes => {
              transformEntered.countDown()
              allowTransformReturn.await()
              bytes
            }
          )
          .start
        _ <- IO.blocking(transformEntered.await()).timeout(3.seconds)
        cancellation <- fiber.cancel.start
        _ <- IO.sleep(100.millis)
        _ <- IO(allowTransformReturn.countDown())
        _ <- cancellation.join
        held <- List
          .fill(5)(())
          .parTraverse(_ => fileStorage.getAsValidatedHttpResponse(selectedOrdinal, _ => true.pure[IO]))
          .timeout(3.seconds)
        _ <- held.flatten.traverse_(_.body.compile.drain)
      } yield expect(held.forall(_.isDefined))
    }
  }
}
