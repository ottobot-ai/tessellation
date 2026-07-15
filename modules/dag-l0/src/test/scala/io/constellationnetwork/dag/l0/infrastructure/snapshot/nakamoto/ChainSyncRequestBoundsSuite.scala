package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.IO

import io.grpc.Status
import weaver.SimpleIOSuite

object ChainSyncRequestBoundsSuite extends SimpleIOSuite {

  test("hash request bounds accept the protocol maximum") {
    IO.pure(expect(ChainSyncServer.validateHashCount("snapshot hashes", ChainSyncServer.MaxHashesPerRequest).isRight))
  }

  test("hash request bounds reject before serving one item above the protocol maximum") {
    val result = ChainSyncServer.validateHashCount("snapshot hashes", ChainSyncServer.MaxHashesPerRequest + 1)
    val status = Status.RESOURCE_EXHAUSTED.withDescription(result.swap.toOption.getOrElse("missing rejection"))

    IO.pure(expect.all(result.isLeft, status.getCode == Status.Code.RESOURCE_EXHAUSTED))
  }

  test("chain sync is unavailable until the restored head is seeded") {
    val result = ChainSyncServer.chainSeedAvailability(None)

    IO.pure(
      expect.all(
        result.isLeft,
        result.swap.toOption.exists(_.getCode == Status.Code.UNAVAILABLE),
        result.swap.toOption.flatMap(status => Option(status.getDescription)).contains("GL0 local bootstrap is not complete")
      )
    )
  }

  test("chain sync preserves the chain-seed failure as the unavailable cause") {
    val failure = new IllegalStateException("invalid recovery head")
    val result = ChainSyncServer.chainSeedAvailability(Some(Left(failure)))

    IO.pure(
      expect.all(
        result.isLeft,
        result.swap.toOption.exists(_.getCode == Status.Code.UNAVAILABLE),
        result.swap.toOption.flatMap(status => Option(status.getCause)).contains(failure)
      )
    )
  }

  test("chain sync is available only after successful chain seeding") {
    IO.pure(expect(ChainSyncServer.chainSeedAvailability(Some(Right(()))).isRight))
  }
}
