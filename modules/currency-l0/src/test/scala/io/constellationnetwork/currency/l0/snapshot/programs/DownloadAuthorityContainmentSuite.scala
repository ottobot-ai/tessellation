package io.constellationnetwork.currency.l0.snapshot.programs

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.snapshot.{PeerSelect, PeerSelection}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.{L0Peer, PeerId}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.validator.StateProofValidator

import com.comcast.ip4s.{Host, Port}
import weaver.MutableIOSuite

object DownloadAuthorityContainmentSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: Resource[IO, Res] = for {
    securityProvider <- SecurityProvider.forAsync[IO]
    implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    hasher = Hasher.forJson[IO]
  } yield (jsonSerializer, hasher, securityProvider)

  implicit private val stateProofSelector: CurrencyStateProofSelector = CurrencyStateProofSelector.instance

  private val selectedPeer = L0Peer(
    PeerId(Hex("selected-peer")),
    Host.fromString("127.0.0.1").get,
    Port.fromInt(9000).get
  )

  private val emptyInfo = CurrencySnapshotInfo(
    lastTxRefs = SortedMap.empty,
    balances = SortedMap.empty,
    lastMessages = None,
    lastFeeTxRefs = None,
    lastAllowSpendRefs = None,
    activeAllowSpends = None,
    globalSnapshotSyncView = None,
    lastTokenLockRefs = None,
    activeTokenLocks = None
  )

  private def signedSnapshot(info: CurrencySnapshotInfo, keyPair: java.security.KeyPair)(
    implicit jsonSerializer: JsonSerializer[IO],
    hasher: Hasher[IO],
    securityProvider: SecurityProvider[IO]
  ): IO[Signed[CurrencyIncrementalSnapshot]] =
    for {
      stateProof <- CurrencySnapshotInfo.stateProofBuilder[IO].buildProof(info, SnapshotOrdinal.MinValue)
      value = CurrencyIncrementalSnapshot(
        ordinal = SnapshotOrdinal.MinValue,
        height = Height.MinValue,
        subHeight = SubHeight.MinValue,
        lastSnapshotHash = Hash.empty,
        blocks = SortedSet.empty,
        rewards = SortedSet.empty,
        tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
        stateProof = stateProof,
        epochProgress = EpochProgress.MinValue,
        dataApplication = None,
        messages = None,
        globalSnapshotSyncs = None,
        feeTransactions = None,
        artifacts = None,
        allowSpendBlocks = None,
        tokenLockBlocks = None,
        globalSyncView = None
      )
      signed <- Signed.forAsyncHasher[IO, CurrencyIncrementalSnapshot](value, keyPair)
    } yield signed

  private def fixedSelection(selection: PeerSelection): PeerSelect[IO] = new PeerSelect[IO] {
    def select: IO[PeerSelection] = selection.pure[IO]
  }

  test("selected ML0 peer cannot substitute another signed snapshot tuple") { res =>
    implicit val (jsonSerializer, hasher, securityProvider) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      signed <- signedSnapshot(emptyInfo, keyPair)
      hashed <- signed.toHashed[IO]
      selection = PeerSelection(selectedPeer, hashed.ordinal, Hash("different-snapshot"))
      result <- Download
        .selectBoundLatest(fixedSelection(selection), _ => (signed, emptyInfo).pure[IO])
        .attempt
    } yield expect(result.swap.exists(_.isInstanceOf[Download.CurrencySelectedSnapshotMismatch]))
  }

  test("selected ML0 base requires a valid snapshot signature") { res =>
    implicit val (jsonSerializer, hasher, securityProvider) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      signed <- signedSnapshot(emptyInfo, keyPair)
      hashed <- signed.toHashed[IO]
      tampered = signed.copy(value = signed.value.copy(lastSnapshotHash = Hash("tampered")))
      selection = PeerSelection(selectedPeer, hashed.ordinal, hashed.hash)
      result <- Download
        .selectBoundLatest(fixedSelection(selection), _ => (tampered, emptyInfo).pure[IO])
        .attempt
    } yield expect(result.swap.exists(_.isInstanceOf[Signed.InvalidSignatureForHash[_]]))
  }

  test("selected ML0 context must reproduce the signed snapshot state proof") { res =>
    implicit val (jsonSerializer, hasher, securityProvider) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      signed <- signedSnapshot(emptyInfo, keyPair)
      hashed <- signed.toHashed[IO]
      mismatchedInfo = emptyInfo.copy(lastMessages = Some(SortedMap.empty))
      selection = PeerSelection(selectedPeer, hashed.ordinal, hashed.hash)
      result <- Download
        .selectBoundLatest(fixedSelection(selection), _ => (signed, mismatchedInfo).pure[IO])
        .attempt
    } yield expect(result.swap.exists(_.isInstanceOf[StateProofValidator.StateBroken]))
  }
}
