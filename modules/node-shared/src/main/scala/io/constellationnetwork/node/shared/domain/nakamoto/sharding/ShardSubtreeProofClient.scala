package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.Applicative
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.sharding.ShardId

/** Cross-shard read client — Slice 11 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8.2/§8.6.
  *
  * Consumer-side of the cross-shard read protocol. Pairs with the prover-side
  * [[ShardSubtreeProofService]] from Slice 10 and its HTTP route `/shard/{shardId}/proof`. When
  * shard X validates a transaction that references state owned by shard Y (e.g. a `SpendAction`
  * whose source `AllowSpend` belongs to a metagraph in shard Y), shard X calls
  * `fetchAndVerify(Y, M_y, key)` to obtain a proof rooted at shard Y's last committee-signed
  * checkpoint's per-MG MPT root (which gl0 already carries on its last finalized snapshot).
  *
  * '''Return semantics''':
  *   - `Some((Some(value), proof))` — proven membership. The validator MUST treat `value` as
  *     authoritative for the proof's anchor checkpoint (subject to §8.3 read-after-write
  *     staleness: up to one gl0 snapshot stale, ~7s).
  *   - `Some((None, proof))` — proven non-membership. v1 has no proof-of-absence support
  *     (`MerklePatriciaInclusionVerifier` ships membership proofs only — see
  *     [[ShardSubtreeProofService]] scaladoc); reserved for v2. Implementations MAY emit this
  *     shape but consumers SHOULD treat it as "state not present at anchor" (same effect as a
  *     missing key in the local-process map).
  *   - `None` — proof unavailable. Causes: target shard offline, network failure, peer doesn't
  *     own the requested MG per local assignment, HTTP timeout. Validator MUST treat this as
  *     "cannot validate this round; reject for retry on next gl0 ord". This is the safe default
  *     — refusing to validate beats validating against stale or fabricated state.
  *
  * '''Why `Array[Byte]` for the value rather than a typed `A`''':
  *   - The validator's consumer (`SpendActionValidator`) reads multiple disparate types
  *     (`AllowSpend` for ActiveAllowSpends partition, `Balance` for the Balances partition). A
  *     typed client surface would force one trait method per type, which doesn't compose. The
  *     wire-byte shape is uniform (JSON-encoded `Hex` on the wire per [[ShardSubtreeProof]]'s
  *     value field) and the validator decodes per-call via its existing Circe decoders. This
  *     keeps the trait minimal and shard-protocol-agnostic.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`): fresh trait for the
  * cross-shard read consumer. No compat ceremony with any pre-Slice-10 path (there isn't one).
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`): no `sys.env.get` in this
  * trait or its implementations. Concrete implementations take typed dependencies via
  * constructor parameters; production wiring threads `SharedConfig.nakamoto.sharding.*` from
  * the slice-2 HOCON config.
  */
trait ShardSubtreeProofClient[F[_]] {

  /** Fetch a value + inclusion proof from another shard for a specific (metagraph, key) pair.
    *
    * '''Contract'''
    *   - `targetShardId` — the shard the caller has determined owns `metagraphAddress` per the
    *     cluster-wide deterministic [[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment]].
    *     The implementation MAY re-check ownership and return `None` on mismatch (defence
    *     against a stale assignment table at the caller).
    *   - `metagraphAddress` — the per-MG subtree the proof witnesses. The verifier path
    *     cross-checks this against the proof envelope's `metagraphAddress` field.
    *   - `key` — the structured [[GlobalStateKey]] the value is stored under in the per-MG
    *     subtree.
    *
    * Returns: see scaladoc on the trait for the three-way return semantics.
    */
  def fetchAndVerify(
    targetShardId: ShardId,
    metagraphAddress: Address,
    key: GlobalStateKey
  ): F[Option[(Option[Array[Byte]], ShardSubtreeProof)]]
}

object ShardSubtreeProofClient {

  /** No-op implementation that always returns `None`. Used:
    *   - As the default in tests that don't exercise the cross-shard read path (the
    *     same-shard fast path in [[io.constellationnetwork.node.shared.domain.swap.SpendActionValidator]]
    *     bypasses the client entirely).
    *   - As the production default in clusters running M = 1 (`numShards = 1`) where every
    *     MG hashes to shard 0 and no cross-shard fetch is ever needed. The validator's
    *     same-shard short-circuit guarantees this no-op is never invoked in that
    *     configuration; wiring still passes it to satisfy the constructor signature.
    *   - As the temporary default until production HTTP wiring lands (the trait is integrated
    *     in this slice; the network plumbing follows in a subsequent slice — see design doc
    *     §8.6 wire shape and Slice 14 sidecar gossip for parallel work).
    */
  def noop[F[_]: Applicative]: ShardSubtreeProofClient[F] = new ShardSubtreeProofClient[F] {
    def fetchAndVerify(
      targetShardId: ShardId,
      metagraphAddress: Address,
      key: GlobalStateKey
    ): F[Option[(Option[Array[Byte]], ShardSubtreeProof)]] =
      Applicative[F].pure(none[(Option[Array[Byte]], ShardSubtreeProof)])
  }

  /** Production HTTP implementation stub — TODO: wire to the Slice 10 HTTP route
    * `POST /shard/{shardId}/proof?metagraph={addr}` once the inter-gl0 peer discovery for
    * shard-owning peers is wired (Slice 14 territory). v1 of Slice 11 stops at the trait
    * integration in [[io.constellationnetwork.node.shared.domain.swap.SpendActionValidator]];
    * the on-wire fetch is the responsibility of the network-plumbing slice.
    *
    * The intended shape:
    *   - `peerSelector(shardId)` resolves a peer URL hosting that shard's committee proof
    *     route. Production wiring would consult [[io.constellationnetwork.node.shared.domain.cluster.services.Cluster]]
    *     filtered to peers whose advertised shard set includes `shardId`.
    *   - Issue `POST /shard/{shardId}/proof?metagraph={mgAddress}` with the [[GlobalStateKey]]
    *     as the JSON body (matching the Slice 10 route's request shape — see
    *     `ShardProofRoutes` scaladoc).
    *   - Decode the response `ShardSubtreeProof` via the standard Circe instance.
    *   - Extract `value` from `proof.value.map(_.toBytes)` for the consumer.
    *
    * The implementation also needs to (re)verify the proof end-to-end against the
    * checkpoint hash recorded in gl0's last finalized snapshot's `shardCheckpoints[shardId]`
    * before surfacing the value — that's defence against a misbehaving peer returning a
    * structurally-valid but semantically-mismatched proof. The verify step uses the same
    * primitives Slice 10 exposes via [[ShardSubtreeProofService.verifyProof]].
    *
    * Stubbed in this slice because (a) the trait integration in `SpendActionValidator` is
    * the load-bearing test of the design and (b) peer discovery for shard-owning peers is
    * a separate workstream. Returns `None` so any premature production wiring fails closed
    * (validator rejects the cross-shard SpendAction for retry).
    */
  def httpStub[F[_]: Async]: ShardSubtreeProofClient[F] = noop[F]
}
