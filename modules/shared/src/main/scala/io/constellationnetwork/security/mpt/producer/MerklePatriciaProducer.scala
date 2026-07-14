package io.constellationnetwork.security.mpt.producer

import cats.Parallel
import cats.effect.Async
import cats.syntax.functor._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.MerklePatriciaSingleInclusionProver
import io.constellationnetwork.security.mpt.{MerklePatriciaTrie, MptRoot}

import fs2.Stream
import io.circe.{Encoder, Json}

trait MerklePatriciaProducer[F[_]] {
  def create[A: Encoder](data: Map[Hex, A]): F[MerklePatriciaTrie]

  def insert[A: Encoder](
    current: MerklePatriciaTrie,
    data: Map[Hex, A]
  ): F[Either[MerklePatriciaError, MerklePatriciaTrie]]

  def remove(
    current: MerklePatriciaTrie,
    keys: List[Hex]
  ): F[Either[MerklePatriciaError, MerklePatriciaTrie]]

  def getProver(trie: MerklePatriciaTrie): F[MerklePatriciaSingleInclusionProver[F]]
}

/** Captured snapshot of a producer's internal state. Call `restore` to roll back the producer to the state at the time this savepoint was
  * created. Used to undo mutations from failed artifact validation (e.g. stateProof divergence).
  */
trait ProducerSavepoint[F[_]] {
  def restore: F[Unit]
}

trait StatefulMerklePatriciaProducer[F[_]] {

  /** Defensively copied full state image. Callers cannot mutate producer state through returned byte arrays. */
  def entries: F[Map[Hex, Array[Byte]]]

  /** Defensively copied point read. */
  def entry(key: Hex): F[Option[Array[Byte]]]

  /** Defensively copied selected-key read. */
  def entriesForKeys(keys: Set[Hex]): F[Map[Hex, Array[Byte]]]

  /** Entry count without requiring callers to retain the state image. */
  def entryCount: F[Int]

  /** Defensively copied entries whose nibble path starts with `prefix`. The comparison deliberately treats upper- and lower-case hex as the
    * same trie path, while retaining each original physical key in the result. This prevents a noncanonical case alias from disappearing
    * before whole-image grammar validation can reject it. The in-memory and filesystem backends filter their internal `Ref` directly, so
    * this is a linear scan (~O(N) over total entries, no I/O) but copies only matched values. Designed for materializing per-field views
    * like `(networkNamespace=Hypergraph, fieldId=LastAllowSpendRefs, no contract)` without an external address set.
    *
    * Note: under the current `GlobalStateKey.toHex` encoding the user-namespace is a one-way hash of the address, so consumers cannot
    * recover addresses from the returned hex keys directly. Consumers that need `Map[Address, V]` either (a) decode the value when it
    * embeds `source: Address` (AllowSpend/TokenLock/etc.), or (b) maintain a sidecar address index — same pattern as the expiry-index
    * partitions.
    */
  def entriesWithPrefix(prefix: Hex): F[Map[Hex, Array[Byte]]]

  def build: F[Either[MerklePatriciaError, MerklePatriciaTrie]]

  /** Build the trie and cache the root hash for the given ordinal. This allows retrieval of historical root hashes via
    * `getRootHashForOrdinal`.
    */
  def buildForOrdinal(ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]]

  /** Retrieve a cached root hash for a specific ordinal. Returns None if the ordinal is not in the cache (either too old or never built).
    */
  def getRootHashForOrdinal(ordinal: SnapshotOrdinal): F[Option[MptRoot]]

  /** Get the current root hash without building. Returns None if trie hasn't been built yet. This is useful for quick checks without the
    * overhead of a full build.
    */
  def getCurrentRootHash: F[Option[MptRoot]]

  /** Get the last built ordinal. Returns None if no ordinal has been built yet.
    */
  def getLastBuiltOrdinal: F[Option[SnapshotOrdinal]]

  def insert[A: Encoder](data: Map[Hex, A]): F[Either[MerklePatriciaError, Unit]]
  def insertBytes(data: Map[Hex, Array[Byte]]): F[Either[MerklePatriciaError, Unit]]
  def update[A: Encoder](key: Hex, value: A): F[Either[MerklePatriciaError, Unit]]
  def remove(keys: List[Hex]): F[Either[MerklePatriciaError, Unit]]
  def clear: F[Unit]
  def getProver: F[MerklePatriciaSingleInclusionProver[F]]
  def buildHexMap(data: Map[GlobalStateKey, Json]): F[Map[Hex, Array[Byte]]]

  /** Capture a snapshot of all internal state (entries, trie, pending changes, caches). The returned savepoint can restore the producer to
    * this exact state.
    */
  def savepoint: F[ProducerSavepoint[F]]
}

object StatefulMerklePatriciaProducer {

  /** Trie-path prefix comparison. Physical key spelling remains untouched so a later complete grammar pass can reject uppercase,
    * odd-length, invalid, or aliased keys rather than silently normalizing them.
    */
  def hasNibblePrefix(key: Hex, prefix: Hex): Boolean = {
    val keyValue = key.value
    val prefixValue = prefix.value

    keyValue.length >= prefixValue.length && prefixValue.indices.forall { index =>
      val expected = Character.digit(prefixValue.charAt(index), 16)
      expected >= 0 && Character.digit(keyValue.charAt(index), 16) == expected
    }
  }
}

trait StatefulWithPersistenceMerklePatriciaProducer[F[_]] extends StatefulMerklePatriciaProducer[F] {
  def persist(ordinal: SnapshotOrdinal): F[Unit]
  def load(ordinal: SnapshotOrdinal): F[Boolean]
  def deleteAbove(ordinal: SnapshotOrdinal): F[Unit]
  def listStoredOrdinals: F[Stream[F, SnapshotOrdinal]]
  def applyCutoff(ordinal: SnapshotOrdinal): F[Unit]
}

object MerklePatriciaProducer {
  def apply[F[_]](implicit producer: MerklePatriciaProducer[F]): MerklePatriciaProducer[F] = producer

  def make[F[_]: Hasher: Async]: MerklePatriciaProducer[F] = stateless[F]

  def stateless[F[_]: Hasher: Async]: MerklePatriciaProducer[F] =
    new StatelessMerklePatriciaProducer[F]

  def parallel[F[_]: Hasher: Async: Parallel: JsonSerializer]: MerklePatriciaProducer[F] =
    new ParallelMerklePatriciaProducer[F]

  def inMemory[F[_]: Async: Hasher: Parallel: JsonSerializer](
    initial: Map[Hex, Array[Byte]] = Map.empty
  ): F[StatefulMerklePatriciaProducer[F]] =
    InMemoryMerklePatriciaProducer.make[F](initial).widen[StatefulMerklePatriciaProducer[F]]
}

sealed trait MerklePatriciaError extends Throwable
case class InvalidData(message: String) extends MerklePatriciaError
case class OperationError(message: String) extends MerklePatriciaError
