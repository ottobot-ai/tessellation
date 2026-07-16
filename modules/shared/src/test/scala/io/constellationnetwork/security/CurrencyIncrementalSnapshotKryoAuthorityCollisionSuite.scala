package io.constellationnetwork.security

import cats.effect.{IO, Resource}

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotStateProof}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.{SnapshotOrdinal, SnapshotTips}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.shared.sharedKryoRegistrar

import weaver.MutableIOSuite

object CurrencyIncrementalSnapshotKryoAuthorityCollisionSuite extends MutableIOSuite {

  final case class Resources(
    kryoHasher: Hasher[IO],
    jsonHasher: Hasher[IO],
    securityProvider: SecurityProvider[IO]
  )

  type Res = Resources

  override def sharedResource: Resource[IO, Res] =
    KryoSerializer.forAsync[IO](sharedKryoRegistrar).flatMap { implicit kryo =>
      JsonSerializer.forAsync[IO].asResource.flatMap { implicit json =>
        SecurityProvider.forAsync[IO].map { securityProvider =>
          Resources(Hasher.forKryo[IO], Hasher.forJson[IO], securityProvider)
        }
      }
    }

  private val snapshotA = CurrencyIncrementalSnapshot(
    ordinal = SnapshotOrdinal.unsafeApply(1L),
    height = Height.MinValue,
    subHeight = SubHeight.MinValue,
    lastSnapshotHash = Hash.empty,
    blocks = SortedSet.empty,
    rewards = SortedSet.empty,
    tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
    stateProof = CurrencySnapshotStateProof(
      lastTxRefsProof = Hash("1" * 64),
      balancesProof = Hash("2" * 64),
      lastMessagesProof = None,
      lastFeeTxRefsProof = None,
      lastAllowSpendRefsProof = None,
      activeAllowSpends = None,
      globalSnapshotSync = None,
      lastTokenLockRefsProof = None,
      activeTokenLocks = None
    ),
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

  private val snapshotB = snapshotA.copy(
    stateProof = snapshotA.stateProof.copy(activeAllowSpends = Some(Hash("3" * 64)))
  )

  test("legacy Kryo projection drops a framework state-proof commitment from the snapshot hash") { res =>
    for {
      kryoHashA <- res.kryoHasher.hash(snapshotA)
      kryoHashB <- res.kryoHasher.hash(snapshotB)
      jsonHashA <- res.jsonHasher.hash(snapshotA)
      jsonHashB <- res.jsonHasher.hash(snapshotB)
    } yield
      expect.eql(kryoHashA, kryoHashB) &&
        expect(jsonHashA != jsonHashB)
  }

  test("a Kryo signature transfers across the dropped commitment while a current JSON signature does not") { res =>
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      kryoSignedA <- {
        implicit val hasher: Hasher[IO] = res.kryoHasher
        Signed.forAsyncHasher[IO, CurrencyIncrementalSnapshot](snapshotA, keyPair)
      }
      kryoAValid <- {
        implicit val hasher: Hasher[IO] = res.kryoHasher
        kryoSignedA.hasValidSignature[IO]
      }
      kryoBValid <- {
        implicit val hasher: Hasher[IO] = res.kryoHasher
        kryoSignedA.copy(value = snapshotB).hasValidSignature[IO]
      }
      jsonSignedA <- {
        implicit val hasher: Hasher[IO] = res.jsonHasher
        Signed.forAsyncHasher[IO, CurrencyIncrementalSnapshot](snapshotA, keyPair)
      }
      jsonAValid <- {
        implicit val hasher: Hasher[IO] = res.jsonHasher
        jsonSignedA.hasValidSignature[IO]
      }
      jsonBValid <- {
        implicit val hasher: Hasher[IO] = res.jsonHasher
        jsonSignedA.copy(value = snapshotB).hasValidSignature[IO]
      }
    } yield
      expect(kryoAValid) &&
        expect(kryoBValid) &&
        expect(jsonAValid) &&
        expect(!jsonBValid)
  }
}
