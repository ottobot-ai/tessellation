package io.constellationnetwork.schema.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.generators.addressGen
import io.constellationnetwork.security._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.addressSetImmutableCodec

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Regression for the fork-recovery global-`mptRoot` divergence storm.
  *
  * Root cause: the consensus global `mptRoot` was a full MPT build over EVERY stored byte, including the `SystemNamespace` sidecar
  * partitions (`ActiveAddressIndex`, expiry buckets). The `ActiveAddressIndex` partition is maintained '''append-only''' on the accept path
  * (`applyActiveAddressIndexDelta(..., removed = Set.empty)`), so its contents are a function of the per-ordinal delta '''history''', not
  * the current KV state — two honest nodes that processed different (but equivalent-final) ordinal streams, or a node that rebuilt via
  * `syncFromGlobalSnapshotInfo` (which seeds the index from current keysets), end up with different sidecar entry sets for IDENTICAL
  * user-field state. Because the sidecar has no per-field stateProof slot, the divergence surfaced as `stateProof[mptRoot]`-ONLY rejection
  * (every per-field root matched, only the rolled-up root differed) → tentative branch → reorg → re-exec → same divergence → storm.
  *
  * Fix: the global `mptRoot` excludes all `SystemNamespace` (`03…`) entries (`GlobalStateKey.nonSystemNamespaceEntries`), making it a pure
  * function of the user-field KV set. This suite pins that invariance: adding / varying sidecar entries must NOT change `mptRoot` (nor any
  * per-field proof), so two nodes that agree on user state agree on the root regardless of sidecar history.
  */
object GlobalMptRootSidecarInvarianceSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def bal(n: Long): Balance = Balance(NonNegLong.unsafeFrom(n))

  /** A small but non-trivial GSI: a handful of balances across distinct addresses. The exact field set is irrelevant — only that
    * `mptStateProofFromBytes` produces a non-empty `mptRoot` and per-field proofs over real user entries.
    */
  private def sampleGsi: GlobalSnapshotInfo = {
    val addrs = (0 until 6).map(_ => addressGen.sample.get).distinct.toList
    GlobalSnapshotInfo.empty.copy(
      balances = SortedMap.from(addrs.zipWithIndex.map { case (a, i) => a -> bal((i + 1) * 1000L) }),
      lastTxRefs = SortedMap.empty
    )
  }

  /** The hex-keyed user-field bytes the global root is built from. */
  private def userBytes(gsi: GlobalSnapshotInfo)(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[Map[Hex, Array[Byte]]] = {
    import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
    gsi.allStateEntriesAsBytes.flatMap { typed =>
      typed.toList.traverse { case (k, v) => GlobalStateKey.toHex[IO](k).map(_ -> v) }.map(_.toMap)
    }
  }

  /** Build one or more `ActiveAddressIndex` sidecar entries (`03…`) for a set of fieldIds — the append-only partition that drifts across
    * nodes. Values are arbitrary address sets; their content is what differs node-to-node in the wild.
    */
  private def sidecarEntries(
    perField: List[(GlobalStateFieldId, SortedSet[Address])]
  )(implicit h: Hasher[IO]): IO[Map[Hex, Array[Byte]]] =
    perField.traverse {
      case (fieldId, addrs) =>
        GlobalStateKey.activeAddressIndexKey[IO](fieldId).flatMap(GlobalStateKey.toHex[IO]).map { hex =>
          hex -> ImmutableCodec[SortedSet[Address]].immutableBytes(addrs).toArray
        }
    }.map(_.toMap)

  test("global mptRoot is invariant to the presence of SystemNamespace sidecar entries") { res =>
    implicit val (h, _, j) = res
    val gsi = sampleGsi
    for {
      base <- userBytes(gsi)
      sidecar <- sidecarEntries(
        List(
          GlobalStateFieldId.Balances -> gsi.balances.keySet.to(SortedSet),
          GlobalStateFieldId.LastTxRefs -> SortedSet(addressGen.sample.get)
        )
      )
      withSidecar = base ++ sidecar
      proofNoSidecar <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](gsi, base)
      proofWithSidecar <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](gsi, withSidecar)
    } yield
      expect.all(
        // Base bytes are all user-field entries (no 0x03 sidecars yet); the added entries ARE sidecars.
        base.keys.forall(k => !GlobalStateKey.isSystemNamespaceHex(k)),
        withSidecar.size > base.size,
        proofNoSidecar.mptRoot.isDefined,
        // The whole point: more sidecar bytes, SAME consensus root.
        proofNoSidecar.mptRoot == proofWithSidecar.mptRoot,
        // Per-field proofs are likewise unaffected (sidecars have no field slot).
        proofNoSidecar.balancesProof == proofWithSidecar.balancesProof,
        proofNoSidecar == proofWithSidecar
      )
  }

  test("global mptRoot is invariant to DIFFERING sidecar contents (append-only ActiveAddressIndex drift)") { res =>
    implicit val (h, _, j) = res
    val gsi = sampleGsi
    for {
      base <- userBytes(gsi)
      // Node A's append-only index: the current balance keyset.
      sidecarA <- sidecarEntries(List(GlobalStateFieldId.Balances -> gsi.balances.keySet.to(SortedSet)))
      // Node B's append-only index: the same keyset PLUS a stale address whose balance left the field
      // (the exact monotonic-drift scenario the bug exploits).
      staleAddr = addressGen.sample.get
      sidecarB <- sidecarEntries(
        List(GlobalStateFieldId.Balances -> (gsi.balances.keySet.to(SortedSet) + staleAddr))
      )
      rootA <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](gsi, base ++ sidecarA).map(_.mptRoot)
      rootB <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](gsi, base ++ sidecarB).map(_.mptRoot)
      // And a node that has NO sidecar at all (fresh GSI rebuild edge).
      rootNone <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](gsi, base).map(_.mptRoot)
    } yield
      expect.same(rootA, rootB) &&
        expect.same(rootA, rootNone) &&
        expect(rootA.isDefined)
  }

  test("nonSystemNamespaceEntries drops exactly the 03-prefixed entries") { res =>
    implicit val (h, _, j) = res
    val gsi = sampleGsi
    for {
      base <- userBytes(gsi)
      sidecar <- sidecarEntries(List(GlobalStateFieldId.Balances -> gsi.balances.keySet.to(SortedSet)))
      combined = base ++ sidecar
      filtered = GlobalStateKey.nonSystemNamespaceEntries(combined)
    } yield
      expect.same(filtered.keySet, base.keySet) &&
        expect(sidecar.keys.forall(GlobalStateKey.isSystemNamespaceHex)) &&
        expect(filtered.forall { case (k, _) => !GlobalStateKey.isSystemNamespaceHex(k) })
  }
}
