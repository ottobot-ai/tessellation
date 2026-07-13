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
}
