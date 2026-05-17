package io.constellationnetwork.security.kes

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path}

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.jdk.CollectionConverters._

import weaver.SimpleIOSuite

/** Tests for [[SecureStore.disk]] — disk-backed implementation with JVM-restart survival and best-effort secure erase.
  *
  * The [[ReadOnceEnforcementSuite]] documents what "secure erase" means under our threat model (best-effort overwrite, not cryptographic
  * unrecoverability); the same caveats apply to the disk variant — and additionally we cannot promise displacement of underlying physical
  * sectors on copy-on-write or log-structured filesystems. The tests here verify the LOGICAL contract: file is gone, perms are 0600,
  * round-trip is byte-identical, second read returns None.
  */
object SecureStoreDiskSuite extends SimpleIOSuite {

  /** Recursively delete a directory tree. Best-effort: ignores files that disappear mid-walk. */
  private def deleteRecursive(p: Path): Unit =
    if (Files.exists(p)) {
      if (Files.isDirectory(p)) {
        val stream = Files.list(p)
        try stream.iterator().asScala.foreach(deleteRecursive)
        finally stream.close()
      }
      try Files.delete(p)
      catch { case _: java.nio.file.NoSuchFileException => () }
    }

  /** Allocate a fresh temp directory for one test and clean it on release. */
  private def tempDir: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("kes-disk-test")))(d => IO.blocking(deleteRecursive(d)))

  test("write + consume round-trips bytes byte-identically") {
    tempDir.use { dir =>
      val bytes = (0 until 256).map(_.toByte).toArray
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.write("k1", bytes)
        result <- store.consume("k1")
      } yield expect(result.exists(_.sameElements(bytes)))
    }
  }

  test("consume returns Some on first call, None on second (read-once)") {
    tempDir.use { dir =>
      val payload = Array.fill[Byte](64)(7)
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.write("k", payload)
        first <- store.consume("k")
        second <- store.consume("k")
      } yield
        expect.all(
          first.exists(_.sameElements(payload)),
          second.isEmpty
        )
    }
  }

  test("write of an existing entry overwrites — new bytes are visible on next read") {
    tempDir.use { dir =>
      val firstPayload = Array.fill[Byte](32)(0x11.toByte)
      val secondPayload = Array.fill[Byte](32)(0x22.toByte)
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.write("k", firstPayload)
        _ <- store.write("k", secondPayload)
        read <- store.consume("k")
      } yield expect(read.exists(_.sameElements(secondPayload)))
    }
  }

  test("write of an existing entry with a different length is still atomic") {
    // Edge case: prior secure-overwrite expects the SAME-length scrub, and the new file may be a different length. The atomic move replaces
    // it regardless, but we want to confirm the visible state is the NEW bytes (and only the new bytes).
    tempDir.use { dir =>
      val firstPayload = Array.fill[Byte](128)(0xaa.toByte)
      val secondPayload = Array.fill[Byte](64)(0xbb.toByte)
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.write("k", firstPayload)
        _ <- store.write("k", secondPayload)
        read <- store.consume("k")
      } yield
        expect.all(
          read.exists(_.length == secondPayload.length),
          read.exists(_.sameElements(secondPayload))
        )
    }
  }

  test("list returns names in sorted order; .tmp leftovers and hidden files are filtered") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.write("zeta", Array.fill[Byte](4)(1))
        _ <- store.write("alpha", Array.fill[Byte](4)(1))
        _ <- store.write("mu", Array.fill[Byte](4)(1))
        // Simulate a crash-time leftover .tmp file — list() must NOT report it.
        _ <- IO.blocking(Files.write(dir.resolve("leftover.tmp"), Array.empty[Byte]))
        // Simulate an unrelated hidden file in the dir — must also be filtered.
        _ <- IO.blocking(Files.write(dir.resolve(".hidden"), Array.empty[Byte]))
        names <- store.list
      } yield expect.same(List("alpha", "mu", "zeta"), names.toList)
    }
  }

  test("JVM-restart survival: a new SecureStore.disk over the same dir sees existing entries") {
    tempDir.use { dir =>
      val payload = Array.fill[Byte](48)(0x42.toByte)
      for {
        store1 <- SecureStore.disk[IO](dir)
        _ <- store1.write("persisted", payload)
        // Simulate JVM restart: discard `store1`, create a brand-new store instance over the same dir.
        store2 <- SecureStore.disk[IO](dir)
        listed <- store2.list
        read <- store2.consume("persisted")
      } yield
        expect.all(
          listed.toList == List("persisted"),
          read.exists(_.sameElements(payload))
        )
    }
  }

  test("consume secure-erase: after consume the file is gone from the directory") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.write("k", Array.fill[Byte](32)(0x55.toByte))
        existedBefore <- IO.blocking(Files.exists(dir.resolve("k")))
        _ <- store.consume("k")
        existedAfter <- IO.blocking(Files.exists(dir.resolve("k")))
      } yield
        expect.all(
          existedBefore,
          !existedAfter
        )
    }
  }

  test("erase secure-erase: after erase the file is gone from the directory") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.write("k", Array.fill[Byte](32)(0x77.toByte))
        _ <- store.erase("k")
        existed <- IO.blocking(Files.exists(dir.resolve("k")))
        listed <- store.list
      } yield expect.all(!existed, listed.isEmpty)
    }
  }

  test("erase on a missing entry is a no-op") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.erase("nope") // must not throw
      } yield success
    }
  }

  test("consume on a missing entry returns None") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        result <- store.consume("nope")
      } yield expect(result.isEmpty)
    }
  }

  test("file permissions are 0600 on POSIX (no-op on non-POSIX FSes)") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.write("perm-check", Array.fill[Byte](16)(0x33.toByte))
        check <- IO.blocking {
          val supportsPosix = dir.getFileSystem.supportedFileAttributeViews().contains("posix")
          if (!supportsPosix) None
          else Some(PosixFilePermissions.toString(Files.getPosixFilePermissions(dir.resolve("perm-check"))))
        }
      } yield
        check match {
          case Some(perms) => expect.same("rw-------", perms)
          case None        => success // platform doesn't expose POSIX perms — the chmod is documented as a no-op
        }
    }
  }

  test("write rejects path-separator names with IllegalArgumentException") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        attempt <- store.write("../escape", Array.empty[Byte]).attempt
      } yield matches(attempt) { case Left(_: IllegalArgumentException) => success }
    }
  }

  test("write rejects names with embedded path separators with IllegalArgumentException") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        attempt <- store.write("foo/bar", Array.empty[Byte]).attempt
      } yield matches(attempt) { case Left(_: IllegalArgumentException) => success }
    }
  }

  test("write rejects .tmp-suffix names so callers can't collide with the rename intermediate") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        attempt <- store.write("evil.tmp", Array.empty[Byte]).attempt
      } yield matches(attempt) { case Left(_: IllegalArgumentException) => success }
    }
  }

  test("write of empty bytes round-trips empty bytes") {
    tempDir.use { dir =>
      for {
        store <- SecureStore.disk[IO](dir)
        _ <- store.write("empty", Array.empty[Byte])
        read <- store.consume("empty")
      } yield expect(read.exists(_.length == 0))
    }
  }
}
