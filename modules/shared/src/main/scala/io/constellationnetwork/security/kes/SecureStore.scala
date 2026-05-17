package io.constellationnetwork.security.kes

import cats.data.Chain
import cats.effect.{Ref, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._

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
}
