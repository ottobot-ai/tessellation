package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.Applicative
import cats.effect.Async
import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.http.p2p.PeerResponse
import io.constellationnetwork.node.shared.http.p2p.middlewares.PeerAuthMiddleware
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.peer.{P2PContext, Peer}
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId}
import io.constellationnetwork.schema.swap.AllowSpend
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaInclusionProof
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedAllowSpendSetCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import io.circe.syntax._
import org.http4s.Method.POST
import org.http4s.client.Client
import org.http4s.{Request, Uri}
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Cross-shard read client — Slice 11 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8.2/§8.6.
  *
  * Consumer-side of the cross-shard read protocol. Pairs with the prover-side [[ShardSubtreeProofService]] from Slice 10 and its HTTP route
  * `/shard/{shardId}/proof`. When shard X validates a transaction that references state owned by shard Y (e.g. a `SpendAction` whose source
  * `AllowSpend` belongs to a metagraph in shard Y), shard X calls `fetchAndVerify(Y, M_y, key)` to obtain a proof rooted at shard Y's last
  * committee-signed checkpoint's per-MG MPT root (which gl0 already carries on its last finalized snapshot).
  *
  * '''Return semantics''':
  *   - `Some((Some(value), proof))` — proven membership. The validator MUST treat `value` as authoritative for the proof's anchor
  *     checkpoint (subject to §8.3 read-after-write staleness: up to one gl0 snapshot stale, ~7s).
  *   - `Some((None, proof))` — proven non-membership. v1 has no proof-of-absence support (`MerklePatriciaInclusionVerifier` ships
  *     membership proofs only — see [[ShardSubtreeProofService]] scaladoc); reserved for v2. Implementations MAY emit this shape but
  *     consumers SHOULD treat it as "state not present at anchor" (same effect as a missing key in the local-process map).
  *   - `None` — proof unavailable. Causes: target shard offline, network failure, peer doesn't own the requested MG per local assignment,
  *     HTTP timeout. Validator MUST treat this as "cannot validate this round; reject for retry on next gl0 ord". This is the safe default
  *     — refusing to validate beats validating against stale or fabricated state.
  *
  * '''Why `Array[Byte]` for the value rather than a typed `A`''':
  *   - The validator's consumer (`SpendActionValidator`) reads multiple disparate types (`AllowSpend` for ActiveAllowSpends partition,
  *     `Balance` for the Balances partition). A typed client surface would force one trait method per type, which doesn't compose. The
  *     wire-byte shape is uniform (JSON-encoded `Hex` on the wire per [[ShardSubtreeProof]]'s value field) and the validator decodes
  *     per-call via its existing Circe decoders. This keeps the trait minimal and shard-protocol-agnostic.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`): fresh trait for the cross-shard read consumer. No compat ceremony
  * with any pre-Slice-10 path (there isn't one).
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`): no `sys.env.get` in this trait or its implementations. Concrete
  * implementations take typed dependencies via constructor parameters; production wiring threads `SharedConfig.nakamoto.sharding.*` from
  * the slice-2 HOCON config.
  */
trait ShardSubtreeProofClient[F[_]] {

  /** Fetch a value + inclusion proof from another shard for a specific (metagraph, key) pair.
    *
    * '''Contract'''
    *   - `targetShardId` — the shard the caller has determined owns `metagraphAddress` per the cluster-wide deterministic
    *     [[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment]]. The implementation MAY re-check ownership and return
    *     `None` on mismatch (defence against a stale assignment table at the caller).
    *   - `metagraphAddress` — the per-MG subtree the proof witnesses. The verifier path cross-checks this against the proof envelope's
    *     `metagraphAddress` field.
    *   - `key` — the structured [[GlobalStateKey]] the value is stored under in the per-MG subtree.
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
    *   - As the default in tests that don't exercise the cross-shard read path (the same-shard fast path in
    *     [[io.constellationnetwork.node.shared.domain.swap.SpendActionValidator]] bypasses the client entirely).
    *   - As the production default in clusters running M = 1 (`numShards = 1`) where every MG hashes to shard 0 and no cross-shard fetch is
    *     ever needed. The validator's same-shard short-circuit guarantees this no-op is never invoked in that configuration; wiring still
    *     passes it to satisfy the constructor signature.
    *   - As the temporary default until production HTTP wiring lands (the trait is integrated in this slice; the network plumbing follows
    *     in a subsequent slice — see design doc §8.6 wire shape and Slice 14 sidecar gossip for parallel work).
    */
  def noop[F[_]: Applicative]: ShardSubtreeProofClient[F] = new ShardSubtreeProofClient[F] {
    def fetchAndVerify(
      targetShardId: ShardId,
      metagraphAddress: Address,
      key: GlobalStateKey
    ): F[Option[(Option[Array[Byte]], ShardSubtreeProof)]] =
      Applicative[F].pure(none[(Option[Array[Byte]], ShardSubtreeProof)])
  }

  /** GL0-LOCAL cross-shard read client — the DETERMINISTIC cross-shard read source for the gl0 acceptance path
    * (`GlobalSnapshotAcceptanceManager.accept`, W3c activation).
    *
    * '''Why a local read, not a peer fetch.''' gl0 is the GLOBAL mirror — it already holds the finalized state of EVERY shard's metagraphs
    * in its own MPT. The cross-shard value a `SpendActionValidator` needs (an `AllowSpend` set under `ActiveAllowSpends[targetMg][source]`,
    * or a `Balance` under `Balances[targetMg][currencyId]`) is therefore readable directly from gl0's own consensus-pinned state — no
    * committee round-trip required. The HTTP client ([[http]]) is the SHARD-COMMITTEE read path (one shard committee asking another's
    * prover); on the gl0 accept path a peer fetch would be NODE-LOCAL (network/peer-pick/cooldown) and its result feeds the consensus
    * `mptRoot`, which would FORK the cluster. This local reader eliminates that non-determinism: every gl0 node reads the SAME key off the
    * SAME consensus-pinned reader and gets the byte-identical value.
    *
    * '''The deterministic anchor.''' `reader` MUST be the consensus-pinned prior-finalized reader the accept is already extending —
    * i.e. the SAME `GlobalStateReader` (branch-aware, bound to the accept's `parentTip`) the per-manager prior-state reads use
    * (`materializeActiveAllowSpendsFromMpt`, the `Balances` partition read, the `ConsumedAllowSpends` spent-set read). Because the accept's
    * prior state is cluster-uniform (it is the finalized snapshot every node agreed on), every gl0 node's reader returns the byte-identical
    * value for the same key. NEVER pass a pending/best-tip/peer view here.
    *
    * '''Return semantics''' (per the trait contract):
    *   - membership ⇒ `Some((Some(jsonBytes), selfProof))`. `jsonBytes` are the Circe-JSON encoding of the typed value
    *     (`SortedSet[Signed[AllowSpend]]` for [[GlobalStateFieldId.ActiveAllowSpends]], `Balance` for [[GlobalStateFieldId.Balances]]) —
    *     the EXACT shape `SpendActionValidator.decodeCrossShardAllowSpends` / `decodeCrossShardBalance` decode (UTF-8 JSON). The value is
    *     read TYPED off the MPT and re-encoded as JSON so it round-trips through the validator's existing decoders verbatim.
    *   - absent key ⇒ `Some((None, selfProof))`. gl0 holds all shards' state, so an absent key is a PROVEN absence at the finalized anchor
    *     (the validator treats it as "AllowSpend not found" / `Balance.empty`, identical to the same-shard `getOrElse` default) — NOT
    *     "couldn't fetch". This is the key difference from the HTTP client, whose `None` means "unavailable, retry".
    *   - unsupported `key.fieldId` (anything other than the two the validator reads) ⇒ `None` (unavailable). Defensive: the validator only
    *     ever asks for `ActiveAllowSpends` / `Balances`, so this is unreachable in practice.
    *
    * '''The self proof is inert.''' The `SpendActionValidator` cross-shard paths consume ONLY the value bytes (the proof half of the tuple
    * is discarded — `case Some((Some(bytes), _))`); the validator re-decodes the bytes and re-applies the W3c effective-balance overlay
    * itself. There is no peer to mistrust on the gl0-local path (the value comes from gl0's OWN finalized state), so there is nothing to
    * verify — the returned [[ShardSubtreeProof]] is a deterministic placeholder carrying the requested `(metagraphAddress, key, value)` and
    * empty roots/witness. It exists only to satisfy the trait's tuple type.
    *
    * '''`numShards = 1`.''' Never constructed there — the gl0 accept path builds this client ONLY inside the `numShards > 1`
    * sharded-validator branch, and at `numShards = 1` the injected unsharded validator is used (cross-shard path unreachable) ⇒
    * byte-identical.
    *
    * @param reader
    *   the consensus-pinned (branch-aware, accept-`parentTip`) finalized-state reader of gl0's global mirror. The determinism anchor.
    */
  def gl0Local[F[_]: Sync](reader: GlobalStateReader[F]): ShardSubtreeProofClient[F] = new ShardSubtreeProofClient[F] {

    // Deterministic placeholder proof for the gl0-local path. The validator discards the proof half of the tuple (it consumes only
    // the value bytes and re-applies the overlay itself), so this self proof is never inspected — it carries the request identity +
    // value and empty roots/witness only to satisfy the `(Option[Array[Byte]], ShardSubtreeProof)` tuple type.
    private def selfProof(metagraphAddress: Address, key: GlobalStateKey, value: Option[Array[Byte]]): ShardSubtreeProof =
      ShardSubtreeProof(
        shardCheckpointHash = io.constellationnetwork.security.hash.Hash.empty,
        metagraphAddress = metagraphAddress,
        perMgMptRoot = io.constellationnetwork.security.hash.Hash.empty,
        key = key,
        value = value.map(Hex.fromBytes(_)),
        mptProof = MerklePatriciaInclusionProof(path = Hex(""), witness = List.empty)
      )

    def fetchAndVerify(
      targetShardId: ShardId,
      metagraphAddress: Address,
      key: GlobalStateKey
    ): F[Option[(Option[Array[Byte]], ShardSubtreeProof)]] = {

      // Encode the typed value as Circe JSON UTF-8 bytes — the EXACT shape the SpendActionValidator decodes
      // (`io.circe.parser.decode[V](new String(bytes, UTF-8))`).
      def jsonBytes[A: io.circe.Encoder](a: A): Array[Byte] =
        a.asJson.noSpaces.getBytes(java.nio.charset.StandardCharsets.UTF_8)

      def present(bytes: Array[Byte]): Option[(Option[Array[Byte]], ShardSubtreeProof)] =
        (bytes.some, selfProof(metagraphAddress, key, bytes.some)).some

      val absent: Option[(Option[Array[Byte]], ShardSubtreeProof)] =
        (none[Array[Byte]], selfProof(metagraphAddress, key, none[Array[Byte]])).some

      val unavailable: Option[(Option[Array[Byte]], ShardSubtreeProof)] =
        none[(Option[Array[Byte]], ShardSubtreeProof)]

      key.fieldId match {
        case GlobalStateFieldId.ActiveAllowSpends =>
          reader
            .get[SortedSet[Signed[AllowSpend]]](key)
            .map {
              case Some(set) if set.nonEmpty => present(jsonBytes(set))
              case _                         => absent
            }

        case GlobalStateFieldId.Balances =>
          reader
            .get[Balance](key)
            .map {
              case Some(balance) => present(jsonBytes(balance))
              case None          => absent
            }

        // The validator only ever asks for the two partitions above; anything else is unsupported ⇒ fail-closed unavailable.
        case _ => unavailable.pure[F]
      }
    }
  }

  /** Resolves gl0's last-finalized [[ShardCheckpoint]] for a given shard, lifted into a [[Signed]] envelope so it can drive
    * [[ShardSubtreeProofService.verifyProof]]. Production wiring reads the checkpoint off the latest-finalized
    * `GlobalIncrementalSnapshot.shardCheckpoints[shardId]` (the field gl0 carries on every finalized snapshot) and wraps it in the
    * enclosing snapshot's own signature proofs — those proofs are inert for `verifyProof` (which re-derives the canonical hash from the
    * checkpoint's `signingPreimage` and never inspects the envelope's `proofs`), they exist only to satisfy the `Signed` type. The
    * checkpoint VALUE is the trust anchor: a peer's proof is accepted iff it anchors at the SAME checkpoint gl0 finalized AND its per-MG
    * root matches what gl0 finalized.
    *
    * Returns `None` when the finalized snapshot has no checkpoint for `shardId` (bootstrap, paused shard, pre-sharding window). The client
    * maps that to "no trusted anchor ⇒ cannot verify ⇒ unavailable", same fail-closed default as a network miss.
    */
  type FinalizedShardCheckpointLookup[F[_]] = ShardId => F[Option[Signed[ShardCheckpoint]]]

  /** Production HTTP implementation of the cross-shard read client — Slice 11 transport plumbing of
    * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8.6.
    *
    * '''Flow''' (mirrors `ShardCheckpointFetcher`, the read-only shard-checkpoint pull, point-for-point):
    *   1. Resolve gl0's last-finalized checkpoint for `targetShardId` via `finalizedCheckpoint`. NO finalized anchor ⇒ return `None`
    *      (fail-closed: there is no trusted root to verify against). This read happens BEFORE any network call so an offline shard / a
    *      pre-sharding snapshot short-circuits without touching a peer. 2. Select a peer over `clusterStorage.getResponsivePeers` with the
    *      SAME time-rotated pick `ShardCheckpointFetcher` uses (the shard committee is a subset of responsive gl0 peers and the serve is
    *      read-only, so any responsive peer that holds the checkpoint is a valid prover — there is no advertised-shard-set discovery yet;
    *      that is the future hardening the trait scaladoc references). 3. `POST /shard/{shardId}/proof?metagraph={mgAddress}` against the
    *      peer's PUBLIC port with the [[GlobalStateKey]] as the JSON body (the route's request shape — see `ShardProofRoutes`). The public
    *      app is response-signed, so the response runs through `responseVerifierMiddleware` to authenticate the peer authored it —
    *      identical to the `ShardCheckpointFetcher` public-port pull. 4. Decode the response as [[ShardSubtreeProof]] (standard Circe
    *      instance). 5. (Re)verify the proof end-to-end against gl0's FINALIZED checkpoint via [[ShardSubtreeProofService.verifyProof]] —
    *      this is the defence against a misbehaving peer returning a structurally-valid but semantically-mismatched proof: `verifyProof`
    *      rejects unless the proof's `perMgMptRoot` equals the per-MG root gl0 finalized AND the proof's `shardCheckpointHash` equals the
    *      canonical hash of gl0's finalized checkpoint AND the MPT witness chain validates against that root. A failed verify ⇒ `None` (the
    *      `Tampered` / semantically-mismatched case; the validator surfaces it as `CrossShardProofUnavailable` — it rejects the SpendAction
    *      for retry, the conservative outcome).
    *
    * '''Return semantics''' (per the trait contract):
    *   - verify passes ⇒ `Some((proof.value.map(_.toBytes), proof))`. The value bytes ride alongside the proof on the wire (the serve side
    *     populates `ShardSubtreeProof.value`); the consumer decodes them per-call. A membership proof whose serve side did not populate the
    *     value surfaces as `Some((None, proof))` — proven-present-but-no-value, which the validator treats as "state not present" (the same
    *     effect as an absent key).
    *   - any failure (no finalized anchor, no responsive peer, HTTP error, decode failure, verify=false) ⇒ `None`. This collapses the
    *     `CrossShardProofUnavailable` and `Tampered` cases into the single fail-closed `None` the trait specifies — refusing to validate
    *     beats validating against stale or fabricated state.
    *
    * '''Best-effort + deduped.''' A per-`(shard, key)` cooldown (`pullDedupCooldownMs`) suppresses re-requesting the same proof inside the
    * window (mirrors `ShardCheckpointFetcher`'s `cooldownRef`). Within the cooldown the client returns `None` (the validator retries on the
    * next gl0 ord). Every network/verify failure logs at debug and returns `None`.
    *
    * '''Inert at `numShards = 1`.''' This constructor is only wired into the validator on the sharding-active path; at `numShards = 1` the
    * validator is built with [[noop]] and the same-shard fast path covers every read, so this client is never invoked. The serve route it
    * talks to can still be mounted and answer — it just has no caller until the cross-shard validator is wired.
    *
    * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`): no `sys.env.get` — `pullDedupCooldownMs` is threaded from typed
    * `SharedConfig.nakamoto.sharding.*` config at the wiring site.
    *
    * @param client
    *   the shared http4s client (same instance the other gl0→gl0 clients use).
    * @param clusterStorage
    *   peer source for the time-rotated responsive-peer pick.
    * @param proofService
    *   the prover/verifier primitive — only `verifyProof` is used here, against gl0's finalized checkpoint.
    * @param finalizedCheckpoint
    *   resolver for gl0's last-finalized checkpoint per shard (the trust anchor).
    * @param pullDedupCooldownMs
    *   per-`(shard, key)` request-suppression window (mirrors `ShardCheckpointFetcher.pullDedupCooldownMs`).
    */
  def http[F[_]: Async: SecurityProvider](
    client: Client[F],
    clusterStorage: ClusterStorage[F],
    proofService: ShardSubtreeProofService[F],
    finalizedCheckpoint: FinalizedShardCheckpointLookup[F],
    pullDedupCooldownMs: Long
  ): F[ShardSubtreeProofClient[F]] =
    Ref.of[F, Map[String, Long]](Map.empty).map { cooldownRef =>
      val logger = Slf4jLogger.getLoggerFromName[F]("ShardSubtreeProofClient")

      // JSON over the response-signed public GET/POST — the same `circeEntityCodec` path `ShardCheckpointFetcher` uses.
      import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}

      new ShardSubtreeProofClient[F] {
        def fetchAndVerify(
          targetShardId: ShardId,
          metagraphAddress: Address,
          key: GlobalStateKey
        ): F[Option[(Option[Array[Byte]], ShardSubtreeProof)]] = {
          val dedupKey = s"${targetShardId.value.value}-${metagraphAddress.value.value}-${key.toString}"
          val unavailable: Option[(Option[Array[Byte]], ShardSubtreeProof)] =
            none[(Option[Array[Byte]], ShardSubtreeProof)]

          // 1. Trust anchor first — gl0's last-finalized checkpoint for the target shard. No anchor ⇒ fail closed BEFORE any network call.
          finalizedCheckpoint(targetShardId).flatMap {
            case None =>
              logger
                .debug(
                  s"🧩 cross-shard proof: no finalized checkpoint for shard=${targetShardId.value.value} (no trust anchor) — unavailable"
                )
                .as(unavailable)

            case Some(anchorCheckpoint) =>
              // Cooldown gate — suppress re-requesting the same (shard, mg, key) inside the window.
              Async[F].realTime.map(_.toMillis).flatMap { now =>
                cooldownRef.modify { m =>
                  m.get(dedupKey) match {
                    case Some(t) if now - t < pullDedupCooldownMs => (m, true) // within cooldown — suppress
                    case _                                        => (m.updated(dedupKey, now), false)
                  }
                }.flatMap { suppressed =>
                  if (suppressed) unavailable.pure[F]
                  else
                    clusterStorage.getResponsivePeers.flatMap { peers =>
                      val peerList = peers.toList
                      if (peerList.isEmpty)
                        logger
                          .debug(s"🧩 cross-shard proof: no responsive peers (shard=${targetShardId.value.value}, key=$dedupKey)")
                          .as(unavailable)
                      else {
                        // 2. Time-rotated peer pick — identical to ShardCheckpointFetcher.
                        val peer: Peer = peerList((now % peerList.size.toLong).toInt)
                        // 3. POST against the peer's PUBLIC port. `openRoutes` is response-signed (verified below) but requires no request
                        //    token — the proof is anchored against gl0's own finalized root, so a wrong/forged proof fails `verifyProof`.
                        val ctx = P2PContext(peer.ip, peer.publicPort, peer.id)
                        val path = s"shard/${targetShardId.value.value}/proof"
                        val uri = (u: Uri) => u.addPath(path).withQueryParam("metagraph", metagraphAddress.value.value)
                        val verified = PeerAuthMiddleware.responseVerifierMiddleware[F](peer.id)(client)
                        PeerResponse[F, F, ShardSubtreeProof](uri, POST)(verified) { (req: Request[F], c: Client[F]) =>
                          c.expect[ShardSubtreeProof](req.withEntity(key))
                        }.run(ctx)
                          .flatMap { proof =>
                            // 4a. Envelope cross-check: the peer MUST have answered for exactly the (MG, key) we asked. Without this a peer
                            //     could return a structurally-valid proof for a DIFFERENT MG/key whose per-MG root also exists in gl0's
                            //     finalized checkpoint, and `verifyProof` (which keys its root lookup on the proof's OWN `metagraphAddress`)
                            //     would accept it — surfacing the wrong MG's value to the validator. Reject the substitution here.
                            if (proof.metagraphAddress =!= metagraphAddress || proof.key =!= key)
                              logger
                                .warn(
                                  s"🧩 cross-shard proof MG/key SUBSTITUTION: shard=${targetShardId.value.value} " +
                                    s"asked mg=${metagraphAddress.value.value.take(8)} got mg=${proof.metagraphAddress.value.value
                                        .take(8)} from=${peer.id.value.value.take(8)} — rejecting"
                                )
                                .as(unavailable)
                            else
                              // 4b. + 5. Re-verify END-TO-END against gl0's finalized checkpoint (NOT the proof's self-claimed checkpoint):
                              //     catches a peer that proxies a structurally-valid proof for a different/forged checkpoint or root.
                              proofService.verifyProof(anchorCheckpoint, proof).flatMap { ok =>
                                if (ok)
                                  logger
                                    .info(
                                      s"🧩 cross-shard proof OK: shard=${targetShardId.value.value} mg=${metagraphAddress.value.value
                                          .take(8)} from=${peer.id.value.value.take(8)} hasValue=${proof.value.isDefined}"
                                    )
                                    .as((proof.value.map(_.toBytes), proof).some)
                                else
                                  logger
                                    .warn(
                                      s"🧩 cross-shard proof TAMPERED/mismatch: shard=${targetShardId.value.value} mg=${metagraphAddress.value.value
                                          .take(8)} from=${peer.id.value.value.take(8)} — verifyProof=false, rejecting"
                                    )
                                    .as(unavailable)
                              }
                          }
                          .handleErrorWith { e =>
                            logger
                              .debug(s"🧩 cross-shard proof failed: shard=${targetShardId.value.value} key=$dedupKey: ${e.getMessage}")
                              .as(unavailable)
                          }
                      }
                    }
                }
              }
          }
        }
      }
    }
}
