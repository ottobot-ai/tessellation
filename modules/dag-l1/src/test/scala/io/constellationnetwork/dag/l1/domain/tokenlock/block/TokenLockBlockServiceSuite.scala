package io.constellationnetwork.dag.l1.domain.tokenlock.block

import java.util.UUID

import cats.data.NonEmptySet
import cats.effect.IO
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.auto._
import weaver.SimpleIOSuite

object TokenLockBlockServiceSuite extends SimpleIOSuite {

  test("replacement candidates come from the exact active snapshot and only include referenced hashes") {
    SecurityProvider.forAsync[IO].use { implicit securityProvider =>
      JsonSerializer.forAsync[IO].flatMap { implicit jsonSerializer =>
        implicit val hasher: Hasher[IO] = Hasher.forJson[IO]

        for {
          sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          blockKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          source = sourceKeyPair.getPublic.toAddress
          target <- Signed.forAsyncHasher(
            TokenLock(
              source,
              TokenLockAmount(10L),
              TokenLockFee(0L),
              TokenLockReference.empty,
              none,
              EpochProgress(100L).some,
              none
            ),
            sourceKeyPair
          )
          targetHashed <- target.toHashed[IO]
          unrelated <- Signed.forAsyncHasher(
            TokenLock(
              source,
              TokenLockAmount(20L),
              TokenLockFee(0L),
              TokenLockReference.empty,
              none,
              EpochProgress(100L).some,
              none
            ),
            sourceKeyPair
          )
          replacement <- Signed.forAsyncHasher(
            TokenLock(
              source,
              TokenLockAmount(11L),
              TokenLockFee(0L),
              TokenLockReference.empty,
              none,
              EpochProgress(100L).some,
              targetHashed.hash.some
            ),
            sourceKeyPair
          )
          block <- Signed.forAsyncHasher(
            TokenLockBlock(RoundId(new UUID(0L, 1L)), NonEmptySet.one(replacement)),
            blockKeyPair
          )
          candidates <- TokenLockBlockService.replacementCandidates(
            block,
            SortedMap(source -> SortedSet(target, unrelated))
          )
        } yield expect(candidates.map(_.hash) == List(targetHashed.hash))
      }
    }
  }
}
