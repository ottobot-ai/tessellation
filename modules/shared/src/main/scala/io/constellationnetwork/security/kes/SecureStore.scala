package io.constellationnetwork.security.kes

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file._
import java.nio.file.attribute.PosixFilePermissions

import cats.data.Chain
import cats.effect.{Async, Ref, Sync}
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.jdk.CollectionConverters._

/** Read-once persistent store for KES secret-key material.
  *
  * "Read-once" means [[consume]] returns the bytes AND erases the persisted representation atomically. After [[consume]] returns `Some(_)`,
  * subsequent calls to [[consume]] (or [[list]]) for the same `name` MUST observe the entry as absent.
  *
  * Ported from Bifrost's `co.topl.algebras.SecureStore`. The Bifrost version takes a `Persistable[A]` typeclass and does codec/decode
  * inside the store; here we keep it simple — the store is bytes-in/bytes-out, and codec wiring is the caller's responsibility. (A future
  * enhancement would be to add a `Persistable[A]` typeclass to mirror Bifrost exactly, but `Array[Byte]` is sufficient for §1.2.)
  *
  * Forward-security note: the [[consume]] erase must be best-effort secure overwrite, not just an "unlink". On JVM heap-only stores we can
  * `SecureRandom.nextBytes` the cached bytes; on disk-backed stores the underlying file system may not guarantee physical erasure. Callers
  * operating on persistent storage should overwrite the file's contents before unlinking.
  */
trait SecureStore[F[_]] {

  /** Write `data` under `name`, replacing any existing entry. */
  def write(name: String, data: Array[Byte]): F[Unit]

  /** Read a single value by name, then erase the persisted representation. Returns `Some(bytes)` if present, `None` otherwise.
    *
    * The bytes returned are a defensive copy; the caller owns them. The store's persistent state for `name` is gone after this call
    * returns.
    */
  def consume(name: String): F[Option[Array[Byte]]]

  /** List the names of all entries currently in the store. */
  def list: F[Chain[String]]

  /** Best-effort secure delete: overwrite the entry's bytes, then remove the entry. Distinct from [[consume]] only in that it does not
    * return the bytes.
    */
  def erase(name: String): F[Unit]
}

object SecureStore {

  /** In-memory implementation suitable for tests and development. Stores `name -> bytes` in a `cats.effect.Ref`.
    *
    * On [[consume]] / [[erase]], the stored bytes are overwritten with [[java.security.SecureRandom]] before the entry is removed from the
    * map.
    *
    * '''Caveat:''' the JVM does not give us strong guarantees that the overwritten array bytes will not survive in a garbage-collected heap
    * dump. The pattern here matches Bifrost's approach and is the best we can do without leaving JVM-managed memory.
    */
  def inMemory[F[_]: Sync]: F[SecureStore[F]] =
    Ref.of[F, Map[String, Array[Byte]]](Map.empty).map(new InMemoryImpl[F](_))

  private final class InMemoryImpl[F[_]: Sync](store: Ref[F, Map[String, Array[Byte]]]) extends SecureStore[F] {
    private val rnd = new java.security.SecureRandom()

    override def write(name: String, data: Array[Byte]): F[Unit] =
      // Defensive copy so callers can scrub their bytes without affecting the store.
      Sync[F].delay(data.clone()).flatMap { copy =>
        store.update { m =>
          // Overwrite any prior entry's bytes before replacing them.
          m.get(name).foreach(rnd.nextBytes)
          m.updated(name, copy)
        }
      }

    override def consume(name: String): F[Option[Array[Byte]]] =
      store.modify { m =>
        m.get(name) match {
          case Some(bytes) =>
            // Defensive copy for the caller.
            val out = bytes.clone()
            // Then overwrite the store's copy.
            rnd.nextBytes(bytes)
            (m.removed(name), Some(out))
          case None => (m, None)
        }
      }

    override def list: F[Chain[String]] =
      store.get.map(m => Chain.fromSeq(m.keys.toList.sorted))

    override def erase(name: String): F[Unit] =
      store.update { m =>
        m.get(name).foreach(rnd.nextBytes)
        m.removed(name)
      }
  }

  /** Disk-backed implementation that survives JVM restart.
    *
    * Layout: one file per `name` directly under `dir`. The store creates `dir` (and parents) if absent. Names containing path separators
    * are rejected (`IllegalArgumentException`) so a malicious or buggy caller can't escape the directory.
    *
    * '''Atomicity''' is provided by `Files.move(.., REPLACE_EXISTING, ATOMIC_MOVE)`: writes go to `dir/<name>.tmp`, are fsynced (file +
    * parent directory), and then renamed into place. Readers either see the prior bytes in full or the new bytes in full — never a
    * half-written file.
    *
    * '''Secure erase''' is best-effort: before unlink (in [[consume]] and [[erase]]) and before atomic-replace (in [[write]] when an entry
    * already exists), the existing file is opened RW, overwritten with [[java.security.SecureRandom]] bytes of the same length, and
    * fsynced. On copy-on-write or log-structured filesystems (btrfs, ZFS, ext4 with data=journal) the overwrite may not displace the
    * underlying physical sectors — the bytes can survive in old extents or the journal. The same caveat applies to SSDs (FTL remapping) and
    * to any snapshot/backup that captured the prior contents. This is the same forward-security limitation noted on the in-memory impl.
    *
    * '''File permissions''' are set to `0600` (owner read/write only) on POSIX filesystems. On non-POSIX filesystems (Windows, some
    * network-attached storage) the permission set is a no-op — `PosixFilePermissions` is silently inapplicable. Callers requiring stronger
    * filesystem-level isolation on those platforms should use an OS-level mechanism (DPAPI, ACLs).
    *
    * '''Concurrency''': each operation runs on the blocking thread pool via [[cats.effect.Async.blocking]]. The atomic move provides
    * single-file consistency across concurrent writers, but two concurrent writes to the same name may race on the prior-bytes
    * secure-overwrite (one writer scrubs, the second writer's tmp wins the rename — both bytes streams are accounted for). No shared lock
    * is taken because callers in §1.2 do not write concurrently to the same key name.
    */
  def disk[F[_]: Async](dir: Path): F[SecureStore[F]] =
    Async[F].blocking {
      Files.createDirectories(dir)
      new DiskImpl[F](dir)
    }

  private final class DiskImpl[F[_]: Async](dir: Path) extends SecureStore[F] {
    private val rnd = new java.security.SecureRandom()
    private val TmpSuffix = ".tmp"

    /** Reject names containing path separators or relative-path components so callers can't escape `dir`. */
    private def validate(name: String): Unit = {
      val invalid =
        name.isEmpty ||
          name == "." ||
          name == ".." ||
          name.contains('/') ||
          name.contains('\\') ||
          name.contains('\u0000') ||
          name.startsWith(".") || // also blocks ".tmp"-prefixed sentinels from leaking into list()
          name.endsWith(TmpSuffix) // reserved for the atomic-rename intermediate
      if (invalid) throw new IllegalArgumentException(s"Invalid SecureStore entry name: '$name'")
    }

    /** Open the parent directory (read-only) so we can fsync it after a metadata change (create, rename, unlink). On non-POSIX FSes this is
      * a no-op via the same `Channel.force` call.
      */
    private def fsyncDir(): Unit = {
      val ch = FileChannel.open(dir, StandardOpenOption.READ)
      try ch.force(true)
      finally ch.close()
    }

    /** Best-effort secure overwrite: open file RW, write SecureRandom bytes of the same length, fsync. Caller must handle subsequent
      * delete/rename.
      */
    private def secureOverwrite(file: Path): Unit = {
      val ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)
      try {
        val size = ch.size()
        if (size > 0L) {
          // Chunk to bound peak heap on large files (KES secret keys are O(KB), but be defensive).
          val ChunkSize = 16 * 1024
          val buf = new Array[Byte](math.min(size, ChunkSize.toLong).toInt)
          var remaining = size
          ch.position(0L)
          while (remaining > 0L) {
            val n = math.min(remaining, buf.length.toLong).toInt
            rnd.nextBytes(buf)
            // If `n < buf.length` we only write the first `n` bytes.
            val bb = if (n == buf.length) ByteBuffer.wrap(buf) else ByteBuffer.wrap(buf, 0, n)
            while (bb.hasRemaining) ch.write(bb)
            remaining -= n
          }
          ch.force(true)
        }
      } finally ch.close()
    }

    /** Apply 0600 perms if the filesystem supports POSIX permissions; otherwise no-op. */
    private def chmod0600(file: Path): Unit =
      try {
        val perms = PosixFilePermissions.fromString("rw-------")
        Files.setPosixFilePermissions(file, perms)
        ()
      } catch {
        case _: UnsupportedOperationException => ()
      }

    override def write(name: String, data: Array[Byte]): F[Unit] =
      Async[F].blocking {
        validate(name)
        val target = dir.resolve(name)
        val tmp = dir.resolve(name + TmpSuffix)

        // If a prior entry exists, scrub it BEFORE the atomic rename so the old bytes are gone even though the rename will atomically
        // replace the file.
        if (Files.exists(target)) {
          secureOverwrite(target)
        }

        // Write tmp with O_CREAT | O_TRUNC | O_WRITE, fsync, chmod 0600, then atomic-rename into place + fsync parent dir.
        val ch = FileChannel.open(
          tmp,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE
        )
        try {
          val bb = ByteBuffer.wrap(data)
          while (bb.hasRemaining) ch.write(bb)
          ch.force(true)
        } finally ch.close()

        chmod0600(tmp)

        // ATOMIC_MOVE on POSIX maps to rename(2); on Windows it maps to MoveFileEx with MOVEFILE_REPLACE_EXISTING (best-effort atomic). If
        // ATOMIC_MOVE is unsupported we fall back to REPLACE_EXISTING.
        try {
          Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
          ()
        } catch {
          case _: java.nio.file.AtomicMoveNotSupportedException =>
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
            ()
        }

        fsyncDir()
      }

    override def consume(name: String): F[Option[Array[Byte]]] =
      Async[F].blocking {
        validate(name)
        val target = dir.resolve(name)
        if (!Files.exists(target)) None
        else {
          val bytes = Files.readAllBytes(target)
          // Defensive copy for the caller — Files.readAllBytes already returns a fresh array, but we expose this as a contract.
          val out = bytes.clone()
          // Scrub bytes on disk before unlink.
          secureOverwrite(target)
          Files.delete(target)
          fsyncDir()
          // Also scrub the intermediate `bytes` we don't return.
          rnd.nextBytes(bytes)
          Some(out)
        }
      }.recover {
        // Race: file disappeared between exists() and readAllBytes().
        case _: NoSuchFileException => None
      }

    override def list: F[Chain[String]] =
      Async[F].blocking {
        val stream = Files.list(dir)
        try {
          val it = stream.iterator().asScala
          val names = it.flatMap { p =>
            val n = p.getFileName.toString
            if (n.endsWith(TmpSuffix) || n.startsWith(".")) Iterator.empty
            else Iterator.single(n)
          }.toList.sorted
          Chain.fromSeq(names)
        } finally stream.close()
      }

    override def erase(name: String): F[Unit] =
      Async[F].blocking {
        validate(name)
        val target = dir.resolve(name)
        if (Files.exists(target)) {
          secureOverwrite(target)
          Files.delete(target)
          fsyncDir()
        }
      }.recover {
        case _: NoSuchFileException => ()
      }
  }
}
