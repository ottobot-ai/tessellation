package io.constellationnetwork.security

import java.nio.charset.StandardCharsets

import cats.effect.kernel.Sync
import cats.syntax.all._

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencyIncrementalSnapshotV1}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.{CustomKryoSerializer, KryoSerializer}
import io.constellationnetwork.schema.transaction.Transaction
import io.constellationnetwork.schema.{GlobalSnapshotInfo, GlobalSnapshotInfoV2, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash

import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Printer}

sealed trait HashLogic
case object JsonHash extends HashLogic
case object KryoHash extends HashLogic

trait HashSelect {
  def select(ordinal: SnapshotOrdinal): HashLogic
}

trait Hasher[F[_]] {
  def hash[A: Encoder](data: A): F[Hash]
  def hashBytes(bytes: Array[Byte]): F[Hash]
  def compare[A: Encoder](data: A, expectedHash: Hash): F[Boolean]
  def getLogic(ordinal: SnapshotOrdinal): HashLogic
  def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): F[Hash]
}

trait HasherSelector[F[_]] {
  def forOrdinal[A](ordinal: SnapshotOrdinal)(fn: Hasher[F] => F[A]): F[A] =
    fn(getForOrdinal(ordinal))

  def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[F]

  def withCurrent[A](fn: Hasher[F] => A): A = fn(getCurrent)
  def getCurrent: Hasher[F]
}

object HasherSelector {
  def apply[F[_]: HasherSelector]: HasherSelector[F] = implicitly

  def alwaysCurrent[F[_]: HasherSelector]: HasherSelector[F] = forSyncAlwaysCurrent(HasherSelector[F].getCurrent)

  def forSync[F[_]](hasherJson: Hasher[F], hasherKryo: Hasher[F], hashSelect: HashSelect): HasherSelector[F] = new HasherSelector[F] {
    def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[F] =
      hashSelect.select(ordinal) match {
        case JsonHash => hasherJson
        case KryoHash => hasherKryo
      }

    def getCurrent = hasherJson
  }

  def forSyncAlwaysCurrent[F[_]](hasherJson: Hasher[F]) = new HasherSelector[F] {
    def getCurrent = hasherJson
    def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[F] = getCurrent
  }
}

object Hasher {

  def apply[F[_]: Hasher]: Hasher[F] = implicitly

  def forKryo[F[_]: Sync: KryoSerializer]: Hasher[F] = new Hasher[F] {
    def getLogic(ordinal: SnapshotOrdinal): HashLogic = KryoHash

    def compare[A: Encoder](data: A, expectedHash: Hash): F[Boolean] =
      hashKryo(data).map(_ === expectedHash)

    def hashKryo[A](data: A): F[Hash] = {
      def map[B](d: B) =
        d match {
          case g: GlobalSnapshotInfo          => GlobalSnapshotInfoV2.fromGlobalSnapshotInfo(g)
          case s: CurrencyIncrementalSnapshot => CurrencyIncrementalSnapshotV1.fromCurrencyIncrementalSnapshot(s)
          case a                              => a
        }

      data match {
        case transaction: Transaction =>
          CustomKryoSerializer.hash(transaction.toEncode)
        case _ =>
          KryoSerializer[F]
            .serialize(data match {
              case d: Encodable[_] => map(d.toEncode)
              case _               => map(data)
            })
            .map(Hash.fromBytes)
            .liftTo[F]
      }
    }

    def hash[A: Encoder](data: A): F[Hash] =
      hashKryo(data)

    def hashBytes(bytes: Array[Byte]): F[Hash] =
      Hash.fromBytesForSync[F](bytes)

    def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): F[Hash] =
      KryoSerializer[F]
        .serialize(data)
        .map(bytes => prefix ++ bytes)
        .liftTo[F]
        .flatMap(Hash.fromBytesForSync[F])
  }

  def forJson[F[_]: Sync: JsonSerializer]: Hasher[F] = new Hasher[F] {
    def getLogic(ordinal: SnapshotOrdinal): HashLogic = JsonHash

    def compare[A: Encoder](data: A, expectedHash: Hash): F[Boolean] =
      hashJson(data).map(_ === expectedHash)

    def hashJson[A: Encoder](data: A): F[Hash] =
      (data match {
        case d: Encodable[_] =>
          JsonSerializer[F].serialize(d.toEncode)(d.jsonEncoder)
        case _ =>
          JsonSerializer[F].serialize[A](data)
      }).flatMap(Hash.fromBytesForSync[F])

    def hash[A: Encoder](data: A): F[Hash] =
      hashJson(data)

    def hashBytes(bytes: Array[Byte]): F[Hash] =
      Hash.fromBytesForSync[F](bytes)

    def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): F[Hash] =
      JsonSerializer[F]
        .serialize(data)
        .map(bytes => prefix ++ bytes)
        .flatMap(Hash.fromBytesForSync[F])
  }

  /** Brotli-free, RFC 8785-canonical-JSON Hasher. Hashes `SHA-256(prefix ++ canonicalJSON(data))`, where the canonical encoding is Circe's
    * `Printer(dropNullValues = true, indent = "", sortKeys = true)` — the SAME printer `JsonSerializer.forAsync` feeds into Brotli, but
    * WITHOUT the Brotli compression step in the pre-image.
    *
    * Why this exists: a TypeScript light client needs to recompute a globally re-executed Merkle-Patricia-Trie node digest with stock Web
    * Crypto + an inline RFC 8785 canonicalizer and ZERO custom code. The default [[forJson]] / [[forKryo]] hashers fold Brotli (resp. Kryo)
    * into the pre-image, which no off-the-shelf JS verifier can reproduce. This hasher's `prefixedHash` is byte-identical to that
    * verifier's `SHA-256(prefixByte ++ canonicalize(commitmentJSON))` for the MPT commitment shapes (`Leaf{remaining,dataDigest}` /
    * `Branch{pathsDigest}` / `Extension{shared,childDigest}`), which `MerklePatriciaCommitment` already encodes — so building/proving a
    * trie under this hasher yields a root + inclusion proof whose byte contract is implementable in TypeScript. The Scala KAT verifies the
    * canonical proof path; release gating still requires an external-client parity vector.
    *
    * Use this ONLY for the light-client commitment trees (e.g. the per-address balance MPT served at `/currency/{address}/balance/proof`).
    * It is a deliberate sibling of [[forJson]] — they hash differently and MUST NOT be mixed within one tree. Consensus-bytes hashing stays
    * on [[forJson]] / [[forKryo]] (Brotli/Kryo) as selected by [[HasherSelector]].
    */
  def forCanonicalJson[F[_]: Sync]: Hasher[F] = new Hasher[F] {
    private val canonicalPrinter: Printer = Printer(dropNullValues = true, indent = "", sortKeys = true)

    def getLogic(ordinal: SnapshotOrdinal): HashLogic = JsonHash

    private def canonicalBytes[A: Encoder](data: A): Array[Byte] =
      canonicalPrinter.print(data.asJson).getBytes(StandardCharsets.UTF_8)

    def hashJson[A: Encoder](data: A): F[Hash] =
      Hash.fromBytesForSync[F](canonicalBytes(data))

    def hash[A: Encoder](data: A): F[Hash] =
      hashJson(data)

    def compare[A: Encoder](data: A, expectedHash: Hash): F[Boolean] =
      hashJson(data).map(_ === expectedHash)

    def hashBytes(bytes: Array[Byte]): F[Hash] =
      Hash.fromBytesForSync[F](bytes)

    def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): F[Hash] =
      Hash.fromBytesForSync[F](prefix ++ canonicalBytes(data))
  }
}
