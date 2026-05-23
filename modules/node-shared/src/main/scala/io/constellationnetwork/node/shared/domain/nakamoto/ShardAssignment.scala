package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Sync
import cats.syntax.functor._

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegInt

/** Static metagraph → shard assignment service (hierarchical-shard-checkpoints v1, §4.1 + §4.3).
  *
  * The mapping is deterministic and cluster-wide: every honest gl0 operator computes the same `ShardId` for the same metagraph address. No
  * coordination round is required. See `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §4.1 for the design rationale.
  *
  * '''Implementation.''' We route the address through the standard `Hasher[F]` typeclass — never hand-rolled Blake2b / byte-concat — so the
  * mapping is byte-equivalent across implementations and the Go sidecar (per `[[feedback-use-hasher-no-manual-serialize]]`). The 32-byte
  * SHA-256 output is then reinterpreted as an unsigned big-endian `BigInt`, reduced `mod numShards`, and the truncated `Int` wrapped in a
  * `ShardId`.
  *
  * '''Why decode the hex first rather than `h.value.getBytes("UTF-8")`.''' The design doc's reference code (§4.1) hashed the UTF-8 bytes of
  * the hex string — that's the SHA-256 of the hex of the SHA-256, and uniformity then depends on the hex-string distribution rather than
  * the digest distribution. Decoding the hex back to the 32 raw digest bytes gives us the same unbiased mod-uniform sample the digest
  * itself already provides, with no extra hashing pass. This is the convention asked for in the slice 3 task.
  *
  * '''Why `unsafeApply` on `ShardId`.''' The result of `(bi mod BigInt(numShards)).toInt` is guaranteed non-negative because `numShards >
  * 0` (`require`d at construction) and `BigInt(1, bytes)` yields a non-negative value (`signum = 1`). So the refinement check would be pure
  * overhead at this point — every call site has already mathematically established non-negativity.
  *
  * '''Late-registered metagraphs (§4.3).''' v1 does not re-balance when a metagraph registers mid-cluster: the deterministic mapping
  * applies at the new metagraph's registration ord and from then on. Load balance is therefore governed by the uniformity of the SHA-256
  * distribution — for `M ≪ N_metagraphs` the law of large numbers gives roughly balanced shards. Pathological collisions are a v2 concern
  * (consistent hashing with virtual nodes / per-epoch redistribution).
  */
trait ShardAssignment[F[_]] {

  /** Deterministic static assignment: `shardId = (Hasher.hash(addr).bytes.toUnsignedBigInt mod M).toInt`. Every honest node computes the
    * same `shardId` for the same metagraph address.
    */
  def shardIdFor(metagraphAddress: Address)(implicit hasher: Hasher[F]): F[ShardId]
}

object ShardAssignment {

  /** Construct a [[ShardAssignment]] over a fixed cluster-wide shard count.
    *
    * @param numShards
    *   the `M` of `shardId = hash(addr) mod M`. Must be strictly positive; callers pass `sharedConfig.nakamoto.sharding.numShards` from the
    *   Slice 2 HOCON config. `require`d at construction time so a misconfigured cluster fails fast at wiring rather than at the first
    *   `shardIdFor` call.
    */
  def make[F[_]: Sync](numShards: Int): ShardAssignment[F] = {
    require(numShards > 0, s"numShards must be positive, got $numShards")

    new ShardAssignment[F] {
      private val modulus: BigInt = BigInt(numShards)

      def shardIdFor(metagraphAddress: Address)(implicit hasher: Hasher[F]): F[ShardId] =
        hasher.hash(metagraphAddress).map { h =>
          // Hash.value is a 64-char hex string (32-byte SHA-256). Decode back to the raw digest bytes
          // and read them as an unsigned big-endian BigInt (signum=1 ⇒ guaranteed non-negative).
          val digestBytes: Array[Byte] = Hex(h.value).toBytes
          val bi: BigInt = BigInt(1, digestBytes)
          val shardIndex: Int = bi.mod(modulus).toInt
          // (bi mod modulus) ∈ [0, modulus) and modulus.toInt ≤ Int.MaxValue ⇒ shardIndex is a valid
          // non-negative Int; the NonNegInt refinement is already mathematically guaranteed.
          ShardId(NonNegInt.unsafeFrom(shardIndex))
        }
    }
  }
}
