package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed

import org.typelevel.log4cats.Logger

/** Resolves the metagraph parent snapshot's gl0-side ordinal, given the metagraph address, the incoming binary's `parentHash`, and the
  * incoming binary's own decoded content.
  *
  * '''Why this exists (#201 / #202).''' The committee gate's KES period and the receiver-side eta both derive from "the ordinal of the
  * metagraph parent snapshot." The previous shortcut used `nakamotoFinalizedOrdinalRef` (the gl0 finalized ordinal) as a proxy, which is
  * the wrong quantity — the gl0 finalized ordinal and the metagraph parent's ordinal are independent (the metagraph may have produced 0, 1,
  * or many snapshots in the window since the gl0 chain last finalized). Worse, the gl0 finalized ordinal is per-peer asymmetric: different
  * peers see different `finalizedOrdinalRef` values for the same parentHash, so the KES period they derive disagrees → KES verify fails →
  * the gate times out.
  *
  * '''Why the ordinal is now derived from the incoming binary (the #213/#290 deadlock fix).''' The earlier GSI-read shortcut looked the
  * parent ordinal up from gl0's GSI currency partitions (`lastIncrementalCurrencySnapshots[mg]` / `lastCurrencySnapshots[mg]`). Those
  * partitions are written via `calculateLastCurrencySnapshots` with a `.filterNot(_.isEmpty)` filter
  * (`GlobalSnapshotStateChannelEventsProcessor.calculateLastCurrencySnapshots`), so they DROP any metagraph whose currency derivation
  * produced no state in the window — while `lastStateChannelSnapshotHashes[mg]` is still written for that mg by `assembleAcceptanceResult`.
  * At 8gl0+4mg the result was: the tip hash matched (`lastStateChannelSnapshotHashes[mg] == parentHash`) but BOTH currency partitions were
  * empty, so the resolver returned `None` → the committee gate fail-closed → the binary was never admitted → the tip froze → permanent
  * deadlock; gl0 surfaced only 1/4 metagraphs in `lastCurrencySnapshots` and admitted 0 metagraph incrementals.
  *
  * The fix (approach ii): the committee gate already holds the incoming binary, and the parent's metagraph ordinal is simply the incoming
  * binary's OWN currency-snapshot ordinal − 1. The incoming binary's `content` bytes deserialize to a `Signed[CurrencyIncrementalSnapshot]`
  * (or a full `Signed[CurrencySnapshot]` at genesis) whose `.value.ordinal` is this binary's ordinal `N`; the parent is `N − 1` (clamped at
  * 0, so a genesis-parented binary derives parent ordinal 0). We KEEP the parent-IDENTITY guard — `lastStateChannelSnapshotHashes[mg]` must
  * still equal `parentHash`, so gl0 only verifies binaries whose parent it recognizes as that mg's recorded tip — and ONLY change where the
  * ordinal NUMBER comes from: the incoming binary's own ordinal, not the (possibly-empty) currency partition.
  *
  * '''Determinism.''' The derived parent ordinal is a PURE function of the incoming binary's content bytes (`N − 1`), identical on every
  * node regardless of node-local GSI state. There is no per-peer `finalizedOrdinalRef`, no race against the GSI-write filter — every honest
  * node that decodes the same binary derives the same parent ordinal, so every node derives the same KES period + eta and the committee VRF
  * proof verifies cross-node. This is the property that the GSI-read path lacked.
  *
  * '''Self-correcting (safe to trust the binary's self-reported ordinal).''' A forged ordinal `N'` derives the wrong KES period / eta, so
  * the sender's attestation over that (period, eta) fails to verify on honest receivers (who derive their own (period, eta) from the same
  * binary and would only match a truthful `N`). A liar therefore cannot get its binary admitted; trusting the self-reported ordinal here is
  * safe because the downstream attestation verify is the actual gate. The parent-identity guard additionally pins the binary to the
  * metagraph's recorded tip.
  *
  * '''Fail-closed on `None`.''' The committee gate's sender path skips publishing and the receiver path drops the attestation when the
  * parent ordinal cannot be resolved — either the tip doesn't match (`parentHash` is not the mg's recorded tip) or the content failed to
  * decode (malformed binary). The legacy `resolve(reader, mg, parentHash)` overload is preserved for any caller that doesn't have the
  * decoded binary; new call sites use `resolveFromBinary`.
  */
object MetagraphParentOrdinalResolver {

  /** Extract this binary's own metagraph ordinal `N` from its `content` bytes, then return the PARENT ordinal `N − 1` (clamped at 0). Pure
    * function of `content` — see the determinism note in the object scaladoc. Tries the incremental decode first (the common case) and
    * falls back to the full/genesis decode (genesis binary, `N = 0` → parent 0). Returns `None` only when `content` decodes as neither
    * (malformed binary) — the caller fails closed.
    *
    * Uses the SAME `JsonSerializer` (JSON + Brotli) + currency codec path that `GlobalSnapshotStateChannelEventsProcessor.deserialize` uses
    * to decode SC-binary content, so this never hand-rolls serialization (project rule: route consensus bytes through the existing
    * codec/Hasher surface).
    *
    * '''Total on malformed input.''' `JsonSerializer.deserialize` returns `F[Either[Throwable, A]]`, but the underlying brotli frame decode
    * can *raise* in `F` (e.g. `NullPointerException` from `ByteBuffer.wrap` on a corrupt brotli header) rather than yielding a `Left`. We
    * `handleError` each decode to `Left` so a raised decode error is treated IDENTICALLY to a `Left` decode failure — a malformed binary
    * fails closed (`None`) instead of crashing the gate-processing fiber.
    */
  def parentOrdinalFromContent[F[_]: Async: JsonSerializer](
    content: Array[Byte]
  ): F[Option[Long]] = {
    def decodeOpt[A: io.circe.Decoder]: F[Option[A]] =
      JsonSerializer[F]
        .deserialize[A](content)
        .map(_.toOption)
        .handleError(_ => Option.empty[A]) // a raised brotli/JSON error == decode failure == fail-closed
    decodeOpt[Signed[CurrencyIncrementalSnapshot]].flatMap {
      case Some(signed) =>
        // `Signed[CurrencyIncrementalSnapshot].value.ordinal: SnapshotOrdinal`; `.value.value` unwraps the NonNegLong newtype.
        Async[F].pure(Some(math.max(0L, signed.value.ordinal.value.value - 1L)))
      case None =>
        // Not an incremental — try the full/genesis snapshot. A genesis snapshot's ordinal is 0, so the parent ordinal is also 0.
        decodeOpt[Signed[CurrencySnapshot]].map(_.map(genesis => math.max(0L, genesis.value.ordinal.value.value - 1L)))
    }
  }

  /** Resolve the metagraph parent ordinal for `(metagraphAddress, parentHash)` by deriving it from the INCOMING binary's own content
    * (ordinal − 1), while still enforcing the parent-IDENTITY guard against the gl0 GSI.
    *
    * Contract:
    *   - If `lastStateChannelSnapshotHashes[mg] != parentHash` (or absent), the binary's parent is not the mg's recorded tip on this peer →
    *     `None` (fail closed). This is unchanged from the legacy resolver — it is the identity guard.
    *   - Otherwise the parent ordinal is `(this binary's own ordinal) − 1`, derived from `binaryContent` via `parentOrdinalFromContent`
    *     (NOT read from gl0's currency partitions, which may have been dropped by the empty-state filter — the deadlock this fixes).
    *
    * This is the path the committee-gate call site uses. The legacy `resolve` overload below is retained for callers that only have
    * `(reader, mg, parentHash)` and not the decoded binary.
    */
  def resolveFromBinary[F[_]: Async: JsonSerializer: Logger](
    reader: GlobalStateReader[F],
    metagraphAddress: Address,
    parentHash: Hash,
    binaryContent: Array[Byte]
  ): F[Option[Long]] =
    reader.getLastStateChannelSnapshotHash(metagraphAddress).flatMap {
      case Some(storedHash) if storedHash === parentHash =>
        // Tip matches — the binary's parent is the mg's recorded tip. Derive the parent ordinal from the incoming binary's OWN ordinal
        // (deterministic across peers) instead of reading gl0's currency partition (which is dropped by `.filterNot(_.isEmpty)` whenever
        // the metagraph's currency derivation produced no state in the window → the permanent deadlock at 8gl0+4mg).
        parentOrdinalFromContent[F](binaryContent).flatMap {
          case some @ Some(_) => Async[F].pure(some)
          case None =>
            Logger[F]
              .warn(
                s"⚠️ parent-ordinal resolve: tip matched but incoming binary content for mg=$metagraphAddress " +
                  s"parent=${parentHash.value.take(12)}... decoded as neither incremental nor full currency snapshot — " +
                  s"returning None (fail-closed)"
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

  /** LEGACY overload — resolve the metagraph parent ordinal via the gl0 GSI currency partitions only (no incoming-binary content). Retained
    * for callers that don't have the decoded binary on hand.
    *
    * '''Known limitation (the reason `resolveFromBinary` exists).''' This path reads the ordinal from
    * `lastIncrementalCurrencySnapshots[mg]` / `lastCurrencySnapshots[mg]`, which `calculateLastCurrencySnapshots` DROPS (via
    * `.filterNot(_.isEmpty)`) whenever the metagraph's currency derivation produced no state — even though
    * `lastStateChannelSnapshotHashes[mg]` is still written. When that happens this returns `None` (tip matched, both partitions empty) and
    * any fail-closed caller deadlocks the metagraph. New consensus-path call sites MUST use `resolveFromBinary`, which derives the ordinal
    * from the incoming binary instead. Returns `None` when the lookup cannot be resolved against the metagraph's current tip — caller's
    * contract is "fail closed."
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
            reader.getLastCurrencySnapshot(metagraphAddress).flatMap {
              case Some(genesis) =>
                Async[F].pure(Some(genesis.value.ordinal.value.value))
              case None =>
                Logger[F]
                  .warn(
                    s"⚠️ parent-ordinal resolve (legacy GSI path): lastStateChannelSnapshotHashes hit but neither incremental nor " +
                      s"genesis snapshot present for mg=$metagraphAddress parent=${parentHash.value.take(12)}... — returning None " +
                      s"(fail-closed; prefer resolveFromBinary at consensus call sites)"
                  )
                  .as(Option.empty[Long])
            }
        }
      case Some(storedHash) =>
        Logger[F]
          .warn(
            s"⚠️ parent-ordinal resolve (legacy GSI path): parentHash mismatch for mg=$metagraphAddress " +
              s"parent=${parentHash.value.take(12)}... gl0-tip=${storedHash.value.take(12)}... — returning None (fail-closed)"
          )
          .as(Option.empty[Long])
      case None =>
        Async[F].pure(Option.empty[Long])
    }
}
