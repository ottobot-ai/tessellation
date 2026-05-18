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
  * the wrong quantity — the gl0 finalized ordinal and the metagraph parent's ordinal are independent (the metagraph may have produced 0,
  * 1, or many snapshots in the window since the gl0 chain last finalized). Worse, the gl0 finalized ordinal is per-peer asymmetric:
  * different peers see different `finalizedOrdinalRef` values for the same parentHash, so the KES period they derive disagrees → KES
  * verify fails → the gate times out.
  *
  * '''How it resolves.''' Reads `lastStateChannelSnapshotHashes[mg]` from the gl0 global-state MPT via the overlay-aware reader (post-#118
  * pending reader, so it picks up the chain's pending writes under MultiBranch instead of lagging behind base by `foldIntoBase`). If that
  * hash equals the incoming binary's `parentHash`, then the binary's parent is the metagraph's current tip and the parent's ordinal is
  * `lastIncrementalCurrencySnapshots[mg].value.ordinal`. Otherwise we treat the lookup as unresolved — `None`.
  *
  * '''Fail-closed on `None`.''' The committee gate's sender path skips publishing and the receiver path drops the attestation when the
  * parent ordinal cannot be resolved. The alternative (the previous `getOrElse(0L)` shortcut) lets the gate proceed with the WRONG eta /
  * KES period, which guarantees verify failure on the receiving side and a silent throughput loss — failing the lookup is louder. The
  * downside is a brief pre-bootstrap window where the gl0 hasn't yet persisted the GSI entry for a known metagraph; for the v1 wire format
  * (single metagraph, no reorgs, no cross-metagraph dependencies), that window is "before the metagraph's genesis lands in gl0", which is
  * also when no peer can produce a committee attestation anyway. The committee path remains correct under all post-genesis sequences.
  *
  * '''v1 limitations.''' Assumes no metagraph reorgs — `parentHash` is always either the current tip or an unknown ancestor. If we ever
  * grow a metagraph reorg pipeline, the gate will need to resolve "this specific parentHash's ordinal" via a metagraph-snapshot store
  * (chainStore-equivalent for currency snapshots), not just the current tip. For now, a single equality check is sufficient and matches the
  * v1 ChainSelection's no-metagraph-reorg invariant.
  */
object MetagraphParentOrdinalResolver {

  /** Resolve the metagraph parent ordinal for `(metagraphAddress, parentHash)` via the gl0 GSI. Returns `None` when the lookup cannot be
    * resolved against the metagraph's current tip — caller's contract is "fail closed."
    *
    * Mismatches between `parentHash` and the gl0-recorded `lastStateChannelSnapshotHashes[mg]` are logged at WARN since they indicate
    * either (a) the binary's parent is a deeper-than-current ancestor — the v1 invariant rules this out, so it would mean an adversary
    * crafted the parentHash, or (b) stale GSI state on this peer that's been overtaken by the sender. Either case is noisy enough to log
    * loudly but not fatal — gate proceeds with `None`, gate rejects the binary, the peer retries on the next gossip wave once the GSI
    * catches up (or never if (a)).
    */
  def resolve[F[_]: Async: Logger](
    reader: GlobalStateReader[F],
    metagraphAddress: Address,
    parentHash: Hash
  ): F[Option[Long]] =
    reader.getLastStateChannelSnapshotHash(metagraphAddress).flatMap {
      case Some(storedHash) if storedHash === parentHash =>
        reader.getLastIncrementalCurrencySnapshot(metagraphAddress).flatMap {
          case Some(signed) =>
            // `Signed[CurrencyIncrementalSnapshot].value.ordinal: SnapshotOrdinal` — `.value.value` unwraps the NonNegLong newtype.
            Async[F].pure(Some(signed.value.ordinal.value.value))
          case None =>
            // The state-channel hash partition is populated but the incremental-snapshot partition isn't yet. This is the genesis-only
            // case (`LastCurrencySnapshots` Left side, not Right) — for v1 no metagraph has been bootstrapped to this state, but if/when
            // one is, the genesis snapshot lives in the `LastCurrencySnapshots` partition with ordinal 0 by definition. Returning None
            // here keeps the fail-closed invariant; production traffic shouldn't hit this for any in-flight binary.
            Logger[F]
              .warn(
                s"⚠️ parent-ordinal resolve: lastStateChannelSnapshotHashes hit but no incremental snapshot for mg=$metagraphAddress " +
                  s"parent=${parentHash.value.take(12)}... — returning None (fail-closed)"
              )
              .as(Option.empty[Long])
        }
      case Some(storedHash) =>
        Logger[F]
          .warn(
            s"⚠️ parent-ordinal resolve: parentHash mismatch for mg=$metagraphAddress " +
              s"parent=${parentHash.value.take(12)}... gl0-tip=${storedHash.value.take(12)}... — returning None (fail-closed)"
          )
          .as(Option.empty[Long])
      case None =>
        // No GSI entry yet for this metagraph — pre-bootstrap window (the metagraph's genesis snapshot hasn't been finalized into the
        // gl0 chain yet on this peer). Fail closed: the gate will reject the binary; the sender will retry once gossip has carried the
        // GSI update.
        Async[F].pure(Option.empty[Long])
    }
}
