package io.constellationnetwork.node.shared.app

import java.security.KeyPair

import cats.effect.kernel.Ref
import cats.effect.std.{Random, Supervisor}

import io.constellationnetwork.domain.allowance_list.AllowanceListEntry
import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.config.types.SharedConfig
import io.constellationnetwork.node.shared.http.p2p.SharedP2PClient
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.logger.LoggerBundle
import io.constellationnetwork.node.shared.modules._
import io.constellationnetwork.node.shared.resources.SharedResources
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{CurrencyStateProofSelector, GlobalStateProofSelector}
import io.constellationnetwork.security.{HashSelect, HasherSelector, SecurityProvider}

import fs2.concurrent.SignallingRef

trait NodeShared[F[_], A <: CliMethod] {
  implicit val random: Random[F]
  implicit val securityProvider: SecurityProvider[F]
  implicit val kryoPool: KryoSerializer[F]
  implicit val jsonSerializer: JsonSerializer[F]
  implicit val metrics: Metrics[F]
  implicit val supervisor: Supervisor[F]
  implicit val hasherSelector: HasherSelector[F]

  implicit val globalStateProofSelector: GlobalStateProofSelector
  implicit val currencyStateProofSelector: CurrencyStateProofSelector

  val keyPair: KeyPair
  lazy val nodeId: PeerId = PeerId.fromPublic(keyPair.getPublic)
  val generation: Generation
  val seedlist: Option[Set[SeedlistEntry]]

  val sharedConfig: SharedConfig

  val sharedResources: SharedResources[F]
  val sharedP2PClient: SharedP2PClient[F]
  val sharedQueues: SharedQueues[F]
  val sharedStorages: SharedStorages[F]
  val sharedServices: SharedServices[F, A]
  val sharedPrograms: SharedPrograms[F, A]
  val sharedValidators: SharedValidators[F]
  val prioritySeedlist: Option[Set[SeedlistEntry]]
  val customAllowanceList: Option[Set[AllowanceListEntry]]

  val hashSelect: HashSelect

  val loggerBundle: LoggerBundle[F]

  /** Split-safety (#261, eta axis): the deferred handle that lets the gl0 follower / `createContext` GSAM's committee-eta resolver walk the
    * SAME chain the leader's resolver walks. The follower GSAM is built in [[TessellationIOApp]] (early, before any chain store exists), so
    * its `EtaStateManager` is wired against a chain-walk closure that reads THIS Ref; gl0's `Main.run` flows
    * `chainStore.vrfOutputsForPeriod` into it once `GlobalSnapshotConsensus.make` has built the chain store (mirrors the leader's own
    * `chainStoreForLookupRef` deferred handle). Stays `None` for every non-gl0 layer (cl0/cl1/dl1/gl1) — the follower eta walk then returns
    * an empty list, byte-identical to the prior `noopEtaChainWalk`, so those layers' startup is unchanged.
    *
    * Type is a plain `Long => F[List[(Long, Array[Byte])]]` (a `vrfOutputsForPeriod(sourcePeriod, etaRotationSnapshots)`-shaped closure
    * with the rotation length already partially applied) so node-shared has no dependency on the dag-l0 `NakamotoChainStore` type.
    */
  val nakamotoFollowerEtaChainWalkRef: Ref[F, Option[Long => F[List[(Long, Array[Byte])]]]]

  def restartSignal: SignallingRef[F, Option[A]]
  def stopSignal: SignallingRef[F, Boolean]
}
