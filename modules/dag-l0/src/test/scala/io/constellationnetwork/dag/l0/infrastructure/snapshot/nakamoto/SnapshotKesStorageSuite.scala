package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.file.{Files, Path}

import cats.effect.{IO, Resource}

import io.constellationnetwork.security.hash.Hash

import weaver.SimpleIOSuite

object SnapshotKesStorageSuite extends SimpleIOSuite {

  private def temporaryDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("snapshot-kes-storage-suite"))) { root =>
      IO.blocking {
        val paths = Files.walk(root)
        try paths.sorted(java.util.Comparator.reverseOrder()).forEach(path => { val _ = Files.deleteIfExists(path) })
        finally paths.close()
      }
    }

  test("KES evidence is durable, idempotent, and hash-conflict safe") {
    temporaryDirectory.use { dataDir =>
      val hash = Hash("a" * 64)
      val signature = Array[Byte](1, 2, 3, 4)
      for {
        absent <- SnapshotKesStorage.get[IO](dataDir, hash)
        _ <- SnapshotKesStorage.put[IO](dataDir, hash, signature)
        _ <- SnapshotKesStorage.put[IO](dataDir, hash, signature.clone())
        stored <- SnapshotKesStorage.get[IO](dataDir, hash)
        conflict <- SnapshotKesStorage.put[IO](dataDir, hash, Array[Byte](9, 9)).attempt
        malformed <- SnapshotKesStorage.get[IO](dataDir, Hash("../escape"))
      } yield expect.all(
        absent.isEmpty,
        stored.exists(java.util.Arrays.equals(_, signature)),
        conflict.isLeft,
        malformed.isEmpty
      )
    }
  }
}
