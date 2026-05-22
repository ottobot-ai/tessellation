package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.Logger

/** Resolves the metagraph parent snapshot's gl0-side ordinal from the gl0 GSI, given the metagraph address + the incoming binary's
  * `parentHash`.
  *
  * '''Why this exists (#201 / #202).''' The committee gate's KES period and the receiver-side eta both derive from "the ordinal of the
  * metagraph parent snapshot." The previous shortcut used `nakamotoFinalizedOrdinalRef` (the gl0 finalized ordinal) as a proxy, which is
  * the wrong quantity — the gl0 finalized ordinal and the metagraph parent's ordinal are independent (the metagraph may have produced 0, 1,
  * or many snapshots in the window since the gl0 chain last finalized). Worse, the gl0 finalized ordinal is per-peer asymmetric: different
  * peers see different `finalizedOrdinalRef` values for the same parentHash, so the KES period they derive disagrees → KES verify fails →
  * the gate times out.
  *
  * '''How it resolves.''' Reads `lastStateChannelSnapshotHashes[mg]` from the gl0 global-state MPT via the overlay-aware reader (post-#118
  * pending reader, so it picks up the chain's pending writes under MultiBranch instead of lagging behind base by `foldIntoBase`). If that
  * hash equals the incoming binary's `parentHash`, then the binary's parent is the metagraph's current tip and the parent's ordinal is
  * `lastIncrementalCurrencySnapshots[mg].value.ordinal`. Otherwise — at this point v1 used to fail-closed — we now consult the
  * '''historical lookup''' (see below) to recover the ordinal of any non-tip ancestor that the gl0 chain store has finalized in the past.
  * Only after both tip-match and history-walk miss do we treat the lookup as unresolved (`None`).
  *
  * '''Historical walk (v2, post-#202 hardening).''' At 4-metagraph stress the gl0 GSI's `lastStateChannelSnapshotHashes` lags the
  * cluster-canonical view: under reorg flap or recovery reset (GSAM `Recovery reset at ordinal=N`) the per-metagraph tip can be stuck at an
  * ancestor of the binary's `parentHash`. The forensic at HEAD f9073e474 observed 6,654/6,654 attestation rejections + 9,772 buffered
  * orphans / 0 admissions at 4×mg cadence — every gate fail-closed because tip-match never recovered. The history walk lets the resolver
  * accept binaries whose parent is any RECENTLY-OBSERVED metagraph tip (within a bounded history window), not strictly the current tip. The
  * per-binary state-proof verification downstream still catches any divergence the resolver might have admitted.
  *
  * '''Fail-closed on `None`.''' The committee gate's sender path skips publishing and the receiver path drops the attestation when the
  * parent ordinal cannot be resolved. The alternative (the previous `getOrElse(0L)` shortcut) lets the gate proceed with the WRONG eta /
  * KES period, which guarantees verify failure on the receiving side and a silent throughput loss — failing the lookup is louder. The
  * downside is a brief pre-bootstrap window where the gl0 hasn't yet persisted the GSI entry for a known metagraph; for the v1 wire format
  * (single metagraph, no reorgs, no cross-metagraph dependencies), that window is "before the metagraph's genesis lands in gl0", which is
  * also when no peer can produce a committee attestation anyway. The committee path remains correct under all post-genesis sequences.
  */
object MetagraphParentOrdinalResolver {

  /** Pluggable history lookup: walks back through the metagraph's SC-binary observation history to find `(mg, scBinaryHash) →
    * metagraphOrdinal`. Concrete impl (in `dag-l0`) is chain-store-backed: it iterates the gl0 chain store's recent finalized snapshots,
    * reading each one's `GlobalSnapshotInfo.lastStateChannelSnapshotHashes[mg]`. The walk depth is bounded by a HOCON-configurable
    * `nakamoto.committee.parent-resolver-history-depth` (default 200) so pathological queries can't burn unbounded CPU.
    *
    * Returns `None` for true "unknown parent" (no recent observation of this hash) — the resolver then fail-closes. Multi-metagraph
    * isolation is enforced by keying on `(mg, hash)` — a metagraph X binary whose `parentHash` happens to equal a metagraph Y binary's hash
    * gets `None`, not Y's ordinal.
    */
  trait HistoryLookup[F[_]] {
    def lookup(metagraphAddress: Address, scBinaryHash: Hash): F[Option[Long]]
  }

  object HistoryLookup {

    /** No-op lookup — returns `None` for every query. Behaviour-equivalent to the pre-v2 resolver. Used by tests / wirings that don't have
      * a chain store in scope (followers, the resolver suite's no-history tests). Production gl0 always passes a chain-backed impl —
      * anything else throws away the safety the walk provides.
      */
    def noop[F[_]: Async]: HistoryLookup[F] = new HistoryLookup[F] {
      def lookup(metagraphAddress: Address, scBinaryHash: Hash): F[Option[Long]] =
        Async[F].pure(Option.empty[Long])
    }
  }

  /** Resolve the metagraph parent ordinal for `(metagraphAddress, parentHash)`.
    *
    *   1. Fast path: read the gl0 GSI tip. If `parentHash` matches, derive the ordinal from `LastIncrementalCurrencySnapshots` (or
    *      `LastCurrencySnapshots` in the genesis-only window) and return `Some(ord)`.
    *   1. History walk: if the tip didn't match (the binary's parent is an ancestor of the current tip, or a stale-GSI peer hasn't yet
    *      caught up), consult `historyLookup`. The chain-backed impl walks the gl0 chain store's recent snapshots for an
    *      `lastStateChannelSnapshotHashes[mg]` entry equal to `parentHash`.
    *   1. Fail closed: if neither tip-match nor history walk resolves, return `None` — caller's contract is fail-closed.
    *
    * '''Backwards compatibility.''' Callers that don't pass `historyLookup` get `HistoryLookup.noop`, i.e. the pre-v2 fail-closed
    * behaviour. The production wire (in `GlobalSnapshotConsensus.scala`'s `committeeParentOrdinalFor` closure) passes the chain-backed impl
    * explicitly.
    */
  def resolve[F[_]: Async: Logger](
    reader: GlobalStateReader[F],
    metagraphAddress: Address,
    parentHash: Hash
  ): F[Option[Long]] =
    resolve[F](reader, metagraphAddress, parentHash, HistoryLookup.noop[F])

  /** Resolve with a pluggable `historyLookup`. The production gl0 path always uses this overload; the no-history overload above is
    * preserved for test ergonomics and follower modules where the resolver is constructed without a chain store in scope.
    */
  def resolve[F[_]: Async: Logger](
    reader: GlobalStateReader[F],
    metagraphAddress: Address,
    parentHash: Hash,
    historyLookup: HistoryLookup[F]
  ): F[Option[Long]] =
    reader.getLastStateChannelSnapshotHash(metagraphAddress).flatMap {
      case Some(storedHash) if storedHash === parentHash =>
        // Tip matches. Try the Right side (incremental snapshots) first. After the first incremental binary is accepted, the metagraph
        // tip is recorded in `LastIncrementalCurrencySnapshots`; before that (genesis-only window between the genesis binary's
        // acceptance and the first incremental binary's acceptance), only `LastCurrencySnapshots` (Left side) is populated. The
        // genesis-only window is a real call site for production traffic: ml0 sends the genesis binary AND the first incremental
        // binary back-to-back at boot (see `Genesis.acceptSignedGenesis` — the genesis binary's hash becomes the parent of the first
        // incremental binary). Other gl0 peers receiving the first incremental binary's gossip must be able to resolve its parent
        // ordinal even though only the Left side is set on this peer; otherwise the gate fail-closes and the first incremental binary
        // (and every binary chained off it) never reaches majority — manifests at 4-metagraph scale where the chain-link rejection
        // strands all metagraphs at gl0.lastCurrencySnapshots ord=1 forever.
        reader.getLastIncrementalCurrencySnapshot(metagraphAddress).flatMap {
          case Some(signed) =>
            // `Signed[CurrencyIncrementalSnapshot].value.ordinal: SnapshotOrdinal` — `.value.value` unwraps the NonNegLong newtype.
            Async[F].pure(Some(signed.value.ordinal.value.value))
          case None =>
            // Right side empty — fall back to the Left side (genesis-only window). The genesis snapshot's ordinal is the parent
            // ordinal that ml0's first incremental binary chains off.
            reader.getLastCurrencySnapshot(metagraphAddress).flatMap {
              case Some(genesis) =>
                Async[F].pure(Some(genesis.value.ordinal.value.value))
              case None =>
                // Both partitions empty — but `LastStateChannelSnapshotHashes` was set. The acceptance manager writes all three
                // partitions in the same `processStateChannelEvents` pass, so this state shouldn't appear post-genesis under normal
                // operation. Log and fail closed.
                Logger[F]
                  .warn(
                    s"⚠️ parent-ordinal resolve: lastStateChannelSnapshotHashes hit but neither incremental nor genesis snapshot " +
                      s"present for mg=$metagraphAddress parent=${parentHash.value.take(12)}... — returning None (fail-closed)"
                  )
                  .as(Option.empty[Long])
            }
        }
      case Some(storedHash) =>
        // Tip mismatch — the gl0 GSI says the metagraph's current tip is `storedHash`, but the binary's `parentHash` is something else.
        // v1 would fail-closed here; v2 walks the metagraph's SC-binary observation history (bounded, configurable depth) looking for
        // an ancestor whose ordinal we can recover.
        //
        // This catches the 4-metagraph-stress failure mode where gl0 lags the cluster by ≥1 metagraph ordinal: the binary's parent is
        // a tip that gl0 finalized at some past global ordinal but has since moved past, or is a tip the cluster admitted via 2/3
        // peer attestation that this gl0 peer hasn't yet recorded locally. Either way, the per-binary state-proof verification
        // downstream catches any chain-divergence the resolver might have admitted; the resolver's only job is to give the gate the
        // right ordinal for eta + KES period derivation.
        historyLookup.lookup(metagraphAddress, parentHash).flatMap {
          case Some(ord) =>
            Logger[F]
              .debug(
                s"📜 parent-ordinal resolve: history walk recovered mg=$metagraphAddress " +
                  s"parent=${parentHash.value.take(12)}... gl0-tip=${storedHash.value.take(12)}... ord=$ord"
              )
              .as(Some(ord))
          case None =>
            Logger[F]
              .warn(
                s"⚠️ parent-ordinal resolve: parentHash mismatch for mg=$metagraphAddress " +
                  s"parent=${parentHash.value.take(12)}... gl0-tip=${storedHash.value.take(12)}... — history walk found no ancestor, returning None (fail-closed)"
              )
              .as(Option.empty[Long])
        }
      case None =>
        // No GSI entry yet for this metagraph — the metagraph's genesis snapshot hasn't been finalized into the gl0 chain yet on
        // this peer. Try history walk one more time (a fresh peer that just bootstrapped chain-only could have observation history
        // ahead of its own MPT writes); on miss, fail closed: the gate will reject the binary; the sender retries once gossip has
        // carried the GSI update.
        historyLookup.lookup(metagraphAddress, parentHash).flatMap {
          case Some(ord) =>
            Logger[F]
              .debug(
                s"📜 parent-ordinal resolve: pre-GSI history walk recovered mg=$metagraphAddress " +
                  s"parent=${parentHash.value.take(12)}... ord=$ord"
              )
              .as(Some(ord))
          case None =>
            Async[F].pure(Option.empty[Long])
        }
    }
}
