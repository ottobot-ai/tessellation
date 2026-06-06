package io.constellationnetwork.currency.l0.snapshot.programs

import java.security.KeyPair

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.currency.dataApplication.{BaseDataApplicationL0Service, L0NodeContext}
import io.constellationnetwork.currency.l0.snapshot.CurrencyConsensusManager
import io.constellationnetwork.currency.l0.snapshot.schema.{CurrencyConsensusOutcome, Finished}
import io.constellationnetwork.currency.l0.snapshot.services.{StateChannelBinarySender, StateChannelSnapshotService}
import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.collateral.{Collateral, OwnCollateralNotSatisfied}
import io.constellationnetwork.node.shared.domain.genesis.{GenesisFS => GenesisLoader}
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.http.p2p.clients.StateChannelSnapshotClient
import io.constellationnetwork.node.shared.infrastructure.consensus._
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.EventTrigger
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.IdentifierStorage
import io.constellationnetwork.schema.CurrencyStateProofSelector
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.{L0Peer, PeerId}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import fs2.io.file.Path
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait Genesis[F[_]] {
  def acceptSignedGenesis(dataApplication: Option[BaseDataApplicationL0Service[F]])(genesis: Signed[CurrencySnapshot])(
    implicit context: L0NodeContext[F],
    hasher: Hasher[F],
    currencyStateProofSelector: CurrencyStateProofSelector
  ): F[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Hash, Address)]

  def accept(dataApplication: Option[BaseDataApplicationL0Service[F]])(genesisPath: Path)(
    implicit context: L0NodeContext[F],
    hasher: Hasher[F],
    currencyStateProofSelector: CurrencyStateProofSelector
  ): F[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Hash, Address)]

  def create(dataApplication: Option[BaseDataApplicationL0Service[F]])(
    balancesPath: Path,
    keyPair: KeyPair
  )(implicit hasher: Hasher[F]): F[Unit]
}

object Genesis {
  def make[F[_]: Async: Parallel: SecurityProvider: JsonSerializer](
    keyPair: KeyPair,
    collateral: Collateral[F],
    stateChannelSnapshotService: StateChannelSnapshotService[F],
    stateChannelSnapshotClient: StateChannelSnapshotClient[F],
    stateChannelBinarySender: StateChannelBinarySender[F],
    globalL0Peer: L0Peer,
    nodeId: PeerId,
    genesisLoader: GenesisLoader[F, CurrencySnapshot],
    identifierStorage: IdentifierStorage[F],
    l0Service: GlobalL0Service[F],
    // Cluster-wide static shard count (`SharedConfig.nakamoto.sharding.numShards`). `1` ⇒ unsharded
    // (production default today); `> 1` ⇒ execution-sharding active. Gates the genesis-bridge enqueue below.
    numShards: Int
  ): Genesis[F] = new Genesis[F] {
    private val logger = Slf4jLogger.getLogger

    override def acceptSignedGenesis(
      dataApplication: Option[BaseDataApplicationL0Service[F]]
    )(
      genesis: Signed[CurrencySnapshot]
    )(
      implicit context: L0NodeContext[F],
      hasher: Hasher[F],
      currencyStateProofSelector: CurrencyStateProofSelector
    ): F[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Hash, Address)] = for {
      hashedGenesis <- genesis.toHashed[F]
      firstIncrementalSnapshot <- CurrencySnapshot.mkFirstIncrementalSnapshot[F](hashedGenesis)
      signedFirstIncrementalSnapshot <- firstIncrementalSnapshot.sign(keyPair)

      _ <- collateral
        .hasCollateral(nodeId)
        .flatMap(OwnCollateralNotSatisfied.raiseError[F, Unit].unlessA)

      _ <- dataApplication
        .traverse(app => app.setCalculatedState(firstIncrementalSnapshot.ordinal, app.genesis.calculated))

      signedBinary <- stateChannelSnapshotService.createGenesisBinary(hashedGenesis.signed)
      identifier = signedBinary.value.toAddress
      _ <- identifierStorage.setInitial(identifier)
      _ <- logger.info(s"Address from genesis data is ${identifier.show}")
      hashedBinary <- signedBinary.toHashed
      binaryHash = hashedBinary.hash
      _ <- stateChannelSnapshotClient.send(identifier, signedBinary)(globalL0Peer)

      signedIncrementalBinary <- stateChannelSnapshotService.createBinary(
        signedFirstIncrementalSnapshot,
        binaryHash,
        None,
        None
      )
      hashedIncrementalBinary <- signedIncrementalBinary.toHashed
      incrementalBinaryHash = hashedIncrementalBinary.hash
      _ <- stateChannelSnapshotClient.send(identifier, signedIncrementalBinary)(globalL0Peer)

      // EXECUTION-SHARDING genesis-bridge (#259 / numShards>1). The direct `send`s above land on ONLY the single
      // configured upstream `globalL0Peer` and are fire-and-forget (no retry, no enqueue). At `numShards>1` the shard
      // checkpoint producer chain-links the shard's per-MG binaries off `Hash.empty`, reading them from the per-shard
      // `ShardBinaryBuffer`, which is fed ONLY by the gossiped `MetagraphBinary` topic
      // (`NakamotoSyncDaemon.bufferReceivedBinaryForShard`). A fresh metagraph's GENESIS-full + first-incremental
      // therefore never reach the buffer on every committee member, so `chainLinkOrder` unfolds nothing, the MG is
      // omitted from the checkpoint, gl0's committed `lastCurrencySnapshots(identifier)` stays empty, and cl1 cannot
      // bootstrap (`TransactionService` times out). Ordinal-2+ incrementals avoid this because `consume` enqueues them
      // into `StateChannelBinarySender`, whose background worker RETRIES the send — each successful send triggers the
      // receiving gl0's `broadcastMetagraphBinary` gossip, reaching every committee member's buffer.
      //
      // Fix (Option A — the GOSSIPED, retried route, NOT a node-local "buffer what I received directly"): enqueue both
      // genesis binaries onto that SAME retried path so the gossip lands on every committee member's shard buffer
      // deterministically (the #261 invariant — the genesis-full reaching the buffer must NOT depend on which single
      // gl0 happened to receive the direct send). The enqueue is purely ADDITIVE — the direct `send`s are kept
      // unchanged. `lastGlobalSnapshotSigners = None`: at genesis bootstrap there is no global-snapshot signer set yet;
      // the sender's peer selector falls back to a random allowed peer (`BinaryPoster.selectPeer`).
      //
      // Gated on `numShards>1`: at `numShards=1` the genesis-full reaches gl0's GSI via the standard `scEvents`
      // chain-link (no shard buffer involved), so this branch does NOT fire and the unsharded path is byte-identical.
      _ <- stateChannelBinarySender
        .enqueue(hashedBinary, hashedGenesis.ordinal, none)
        .productR(stateChannelBinarySender.enqueue(hashedIncrementalBinary, signedFirstIncrementalSnapshot.ordinal, none))
        .productR(
          logger.info(
            s"Genesis-bridge (numShards=$numShards): enqueued genesis-full ${binaryHash.show} (ord=${hashedGenesis.ordinal.show}) " +
              s"and first-incremental ${incrementalBinaryHash.show} (ord=${signedFirstIncrementalSnapshot.ordinal.show}) " +
              s"onto the retried/gossiped StateChannelBinarySender path so they reach every committee member's shard buffer"
          )
        )
        .whenA(numShards > 1)

      _ <- logger.info(s"Genesis binary ${binaryHash.show} and ${incrementalBinaryHash.show} accepted and sent to Global L0")
    } yield (signedFirstIncrementalSnapshot, hashedGenesis.info.toCurrencySnapshotInfo, incrementalBinaryHash, identifier)

    override def accept(dataApplication: Option[BaseDataApplicationL0Service[F]])(genesisPath: Path)(
      implicit context: L0NodeContext[F],
      hasher: Hasher[F],
      currencyStateProofSelector: CurrencyStateProofSelector
    ): F[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Hash, Address)] = genesisLoader
      .loadSignedGenesis(genesisPath)
      .flatTap { genesis =>
        dataApplication
          .traverse(app => app.setCalculatedState(genesis.ordinal, app.genesis.calculated))
      }
      .flatMap(acceptSignedGenesis(dataApplication))

    def create(dataApplication: Option[BaseDataApplicationL0Service[F]])(
      balancesPath: Path,
      keyPair: KeyPair
    )(implicit hasher: Hasher[F]): F[Unit] = {
      def mkBalances =
        genesisLoader
          .loadBalances(balancesPath)
          .map(_.map(a => (a.address, a.balance)).toMap)

      def mkDataApplicationPart =
        dataApplication.traverse(da => da.serializedOnChainGenesis.map(DataApplicationPartV1(_, List.empty, Hash.empty)))

      for {
        balances <- mkBalances
        dataApplicationPart <- mkDataApplicationPart
        (latestSnapshot, _) <- l0Service.pullLatestSnapshot

        genesis = CurrencySnapshot.mkGenesis(balances, dataApplicationPart, latestSnapshot.some)
        signedGenesis <- genesis.sign(keyPair)
        signedBinary <- stateChannelSnapshotService.createGenesisBinary(signedGenesis)
        identifier = signedBinary.value.toAddress
        _ <- genesisLoader.write(signedGenesis, identifier, balancesPath.resolveSibling(""))
        _ <- logger.info(
          s"genesis.snapshot and genesis.address have been created for the metagraph ${identifier.show} in ${balancesPath.resolveSibling("")}"
        )
      } yield ()
    }
  }

}
