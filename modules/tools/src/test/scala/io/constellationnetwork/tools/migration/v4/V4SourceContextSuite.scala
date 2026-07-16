package io.constellationnetwork.tools.migration.v4

import cats.effect.IO

import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.tools.migration.v4.V4SourceEncoding.{V4BrotliJson, V4KryoV1}

import shapeless.test.illTyped
import weaver.SimpleIOSuite

object V4SourceContextSuite extends SimpleIOSuite {

  private val nonDevBoundaries = List(
    AppEnvironment.Mainnet -> 2572384L,
    AppEnvironment.Testnet -> 1933590L,
    AppEnvironment.Integrationnet -> 1527434L
  )

  test("upstream-v4 source era is Kryo before and at each inclusive network boundary") {
    val checks = nonDevBoundaries.flatMap {
      case (environment, boundary) =>
        List(boundary - 1L, boundary).map { value =>
          expect.same(V4KryoV1, V4SourceContext(environment, SnapshotOrdinal.unsafeApply(value)).encoding)
        }
    }

    IO.pure(checks.reduce(_ && _))
  }

  test("upstream-v4 source era changes to Brotli JSON immediately after each network boundary") {
    val checks = nonDevBoundaries.map {
      case (environment, boundary) =>
        expect.same(V4BrotliJson, V4SourceContext(environment, SnapshotOrdinal.unsafeApply(boundary + 1L)).encoding)
    }

    IO.pure(checks.reduce(_ && _))
  }

  test("dev's ordinal-zero boundary has no valid predecessor and remains inclusive") {
    val boundary = SnapshotOrdinal.MinValue

    IO.pure(
      expect.same(None, SnapshotOrdinal.partialPrevious.partialPrevious(boundary)) &&
        expect.same(V4KryoV1, V4SourceContext(AppEnvironment.Dev, boundary).encoding) &&
        expect.same(V4BrotliJson, V4SourceContext(AppEnvironment.Dev, SnapshotOrdinal.unsafeApply(1L)).encoding)
    )
  }

  test("callers cannot construct or copy a source context with a supplied encoding") {
    illTyped(
      """new V4SourceContext(AppEnvironment.Mainnet, SnapshotOrdinal.MinValue) {}"""
    )
    illTyped(
      """V4SourceContext(AppEnvironment.Mainnet, SnapshotOrdinal.MinValue, V4SourceEncoding.V4BrotliJson)"""
    )
    illTyped(
      """V4SourceContext(AppEnvironment.Mainnet, SnapshotOrdinal.MinValue).copy()"""
    )

    IO.pure(success)
  }
}
