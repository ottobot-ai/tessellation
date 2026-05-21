package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSyncView
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** Targets the producer-side priority chain for the cl0 → gl0 sync ordinal.
  *
  * Mode-2 bug (see docs/nakamoto/MODE2-GLOBAL-SYNC-VIEW-RCA.md): the chain used to prefer
  * the prior CL0 snapshot's `globalSyncView.ordinal` over the local GL0 head, which created
  * a fixed point at the genesis-inherited `GlobalSyncView(ord=1, epochProgress=1)`.
  *
  * Fix: when no peer-sync quorum is available, take `max(prior_view, local_head)` instead of
  * `prior_view.orElse(local_head)`. This preserves the prior view's monotonic lower-bound
  * semantics while letting the producer escape the genesis seed once the local follower
  * advances.
  */
object CurrencySnapshotAcceptanceManagerSuite extends SimpleIOSuite {

  private def ord(value: Long): SnapshotOrdinal =
    SnapshotOrdinal(NonNegLong.unsafeFrom(value))

  private def viewAt(ordValue: Long, epochProgress: Long): GlobalSyncView =
    GlobalSyncView(ord(ordValue), Hash.empty, EpochProgress(NonNegLong.unsafeFrom(epochProgress)))

  pureTest(
    "Mode-2 regression: prior view stuck at ord=1, local gl0 head at 100, no peer-sync " +
      "quorum -> chosen ordinal is 100 (not 1)"
  ) {
    val chosen = CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot(
      forcedGlobalSyncView = None,
      maybeSnapshotOrdinalSync = None,
      maybeLastGlobalSyncView = Some(viewAt(1L, 1L)),
      fallbackOrdinal = ord(100L)
    )
    expect.eql(ord(100L), chosen)
  }

  pureTest(
    "Peer-sync quorum (path A) takes priority over both prior view (B) and local head (C), " +
      "even when its ordinal is lower than the local head"
  ) {
    val chosen = CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot(
      forcedGlobalSyncView = None,
      maybeSnapshotOrdinalSync = Some(ord(50L)),
      maybeLastGlobalSyncView = Some(viewAt(60L, 60L)),
      fallbackOrdinal = ord(100L)
    )
    expect.eql(ord(50L), chosen)
  }

  pureTest(
    "Peer-sync quorum (path A) takes priority even when above the local head (committee can be ahead)"
  ) {
    val chosen = CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot(
      forcedGlobalSyncView = None,
      maybeSnapshotOrdinalSync = Some(ord(120L)),
      maybeLastGlobalSyncView = Some(viewAt(60L, 60L)),
      fallbackOrdinal = ord(100L)
    )
    expect.eql(ord(120L), chosen)
  }

  pureTest(
    "Forced sync view (validator path) bypasses producer priority chain entirely"
  ) {
    val chosen = CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot(
      forcedGlobalSyncView = Some(viewAt(42L, 42L)),
      maybeSnapshotOrdinalSync = Some(ord(999L)),
      maybeLastGlobalSyncView = Some(viewAt(60L, 60L)),
      fallbackOrdinal = ord(100L)
    )
    expect.eql(ord(42L), chosen)
  }

  pureTest(
    "No peer-sync, no prior view -> falls back to local head"
  ) {
    val chosen = CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot(
      forcedGlobalSyncView = None,
      maybeSnapshotOrdinalSync = None,
      maybeLastGlobalSyncView = None,
      fallbackOrdinal = ord(100L)
    )
    expect.eql(ord(100L), chosen)
  }

  pureTest(
    "No peer-sync, prior view at MinValue (sentinel = no real prior) -> falls back to local head"
  ) {
    val chosen = CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot(
      forcedGlobalSyncView = None,
      maybeSnapshotOrdinalSync = None,
      maybeLastGlobalSyncView = Some(viewAt(SnapshotOrdinal.MinValue.value.value, 0L)),
      fallbackOrdinal = ord(100L)
    )
    expect.eql(ord(100L), chosen)
  }

  pureTest(
    "No peer-sync, prior view ahead of local head -> prior view wins (lower-bound preserved)"
  ) {
    val chosen = CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot(
      forcedGlobalSyncView = None,
      maybeSnapshotOrdinalSync = None,
      maybeLastGlobalSyncView = Some(viewAt(150L, 150L)),
      fallbackOrdinal = ord(100L)
    )
    expect.eql(ord(150L), chosen)
  }

  pureTest(
    "No peer-sync, prior view equal to local head -> either is fine (idempotent at equality)"
  ) {
    val chosen = CurrencySnapshotAcceptanceManager.selectOrdinalToFetchGlobalSnapshot(
      forcedGlobalSyncView = None,
      maybeSnapshotOrdinalSync = None,
      maybeLastGlobalSyncView = Some(viewAt(100L, 100L)),
      fallbackOrdinal = ord(100L)
    )
    expect.eql(ord(100L), chosen)
  }
}
