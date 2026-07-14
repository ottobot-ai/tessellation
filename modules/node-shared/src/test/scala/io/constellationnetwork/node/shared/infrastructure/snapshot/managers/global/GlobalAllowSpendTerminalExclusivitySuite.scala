package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

object GlobalAllowSpendTerminalExclusivitySuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def allowSpend(
    sourceKey: java.security.KeyPair,
    destination: Address,
    lastValid: Long
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpend]] =
    Signed.forAsyncHasher(
      AllowSpend(
        source = sourceKey.getPublic.toAddress,
        destination = destination,
        currencyId = None,
        amount = SwapAmount(PosLong.unsafeFrom(100L)),
        fee = AllowSpendFee(NonNegLong.unsafeFrom(0L)),
        parent = AllowSpendReference.empty,
        lastValidEpochProgress = EpochProgress(NonNegLong.unsafeFrom(lastValid)),
        approvers = List(destination)
      ),
      sourceKey
    )

  test("native validation view rejects past expiry, accepts equality and future, and preserves metagraph scopes") { res =>
    implicit val (hasher, securityProvider, _) = res
    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destination <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      metagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      expired <- allowSpend(sourceKey, destination, 9L)
      boundary <- allowSpend(sourceKey, destination, 10L)
      future <- allowSpend(sourceKey, destination, 11L)
      source = sourceKey.getPublic.toAddress
      native = SortedMap(source -> SortedSet(expired, boundary, future))
      metagraphScoped = SortedMap(source -> SortedSet(expired, boundary, future))
      active = SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]](
        None -> native,
        metagraph.some -> metagraphScoped
      )
      filtered = GlobalSnapshotAcceptanceManager.filterNativeAllowSpendsForEpoch(
        active,
        EpochProgress(NonNegLong.unsafeFrom(10L))
      )
    } yield
      expect.all(
        filtered(None)(source) == SortedSet(boundary, future),
        filtered(metagraph.some) == metagraphScoped,
        filtered.keySet == active.keySet
      )
  }

  test("terminal guard hashes only native references and rejects any consume-expiry overlap") { res =>
    implicit val (_, securityProvider, _) = res
    for {
      producer <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      source <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      destination <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      metagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      overlapping = Hash("11" * 32)
      nativeOnly = Hash("22" * 32)
      metagraphRef = Hash("33" * 32)
      actions = SortedMap(
        producer -> List(
          SpendAction(
            NonEmptyList.of(
              SpendTransaction(overlapping.some, None, SwapAmount(PosLong.unsafeFrom(1L)), source, destination),
              SpendTransaction(nativeOnly.some, None, SwapAmount(PosLong.unsafeFrom(1L)), source, destination),
              SpendTransaction(metagraphRef.some, CurrencyId(metagraph).some, SwapAmount(PosLong.unsafeFrom(1L)), source, destination)
            )
          )
        )
      )
      consumed = GlobalSnapshotAcceptanceManager.nativeConsumedAllowSpendRefs(actions)
      rejected = GlobalSnapshotAcceptanceManager.ensureNativeAllowSpendTerminalDisjointness(
        consumed,
        SortedSet(overlapping, metagraphRef)
      )
      accepted = GlobalSnapshotAcceptanceManager.ensureNativeAllowSpendTerminalDisjointness(
        consumed,
        SortedSet(metagraphRef)
      )
    } yield
      expect.all(
        consumed == SortedSet(overlapping, nativeOnly),
        rejected == Left(GlobalSnapshotAcceptanceManager.NativeAllowSpendTerminalOverlap(SortedSet(overlapping))),
        accepted == Right(())
      )
  }
}
