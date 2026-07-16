package io.constellationnetwork.node.shared.domain.queue

import cats.effect.{Deferred, IO}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.queue.ReservedIngressQueue.{Limits, OfferResult, Usage}

import weaver.SimpleIOSuite

object ReservedIngressQueueSuite extends SimpleIOSuite {

  test("item and byte reservations remain charged until processing completes") {
    for {
      queue <- ReservedIngressQueue.bounded[IO, String](Limits(maxItems = 2, maxBytes = 10L, maxItemBytes = 8L))
      first <- queue.tryOffer("first", 4L)
      second <- queue.tryOffer("second", 6L)
      full <- queue.tryOffer("third", 1L)
      before <- queue.usage
      processed <- queue.takeAndUse(IO.pure)
      after <- queue.usage
      replacement <- queue.tryOffer("third", 4L)
      finalUsage <- queue.usage
    } yield expect.all(
      first == OfferResult.Accepted,
      second == OfferResult.Accepted,
      full == OfferResult.ItemCapacityExceeded(2),
      before == Usage(2, 10L),
      processed == "first",
      after == Usage(1, 6L),
      replacement == OfferResult.Accepted,
      finalUsage == Usage(2, 10L)
    )
  }

  test("byte capacity, per-item size, and invalid sizes reject without changing usage") {
    for {
      queue <- ReservedIngressQueue.bounded[IO, String](Limits(maxItems = 3, maxBytes = 10L, maxItemBytes = 8L))
      accepted <- queue.tryOffer("accepted", 6L)
      byteFull <- queue.tryOffer("byte-full", 5L)
      tooLarge <- queue.tryOffer("too-large", 9L)
      zero <- queue.tryOffer("zero", 0L)
      negative <- queue.tryOffer("negative", -1L)
      usage <- queue.usage
    } yield expect.all(
      accepted == OfferResult.Accepted,
      byteFull == OfferResult.ByteCapacityExceeded(actual = 5L, available = 4L, maximum = 10L),
      tooLarge == OfferResult.ItemTooLarge(actual = 9L, maximum = 8L),
      zero == OfferResult.InvalidRetainedBytes(0L),
      negative == OfferResult.InvalidRetainedBytes(-1L),
      usage == Usage(1, 6L)
    )
  }

  test("an in-flight item remains reserved and blocks replacement admission") {
    for {
      queue <- ReservedIngressQueue.bounded[IO, String](Limits(maxItems = 1, maxBytes = 8L, maxItemBytes = 8L))
      _ <- queue.tryOffer("held", 8L)
      entered <- Deferred[IO, Unit]
      continue <- Deferred[IO, Unit]
      fiber <- queue.takeAndUse(value => entered.complete(()) >> continue.get.as(value)).start
      _ <- entered.get.timeout(1.second)
      whileInFlight <- queue.usage
      rejected <- queue.tryOffer("replacement", 1L)
      _ <- continue.complete(())
      result <- fiber.joinWithNever
      after <- queue.usage
    } yield expect.all(
      whileInFlight == Usage(1, 8L),
      rejected == OfferResult.ItemCapacityExceeded(1),
      result == "held",
      after == Usage(0, 0L)
    )
  }

  test("processing failure releases the reservation without masking the error") {
    val failure = new RuntimeException("processing failed")

    for {
      queue <- ReservedIngressQueue.bounded[IO, String](Limits(maxItems = 1, maxBytes = 8L, maxItemBytes = 8L))
      _ <- queue.tryOffer("failed", 4L)
      result <- queue.takeAndUse(_ => IO.raiseError[Unit](failure)).attempt
      usage <- queue.usage
    } yield expect.same(Left(failure), result).and(expect.same(Usage(0, 0L), usage))
  }

  test("a synchronously throwing processing callback releases the reservation") {
    val failure = new RuntimeException("callback threw before returning an effect")

    for {
      queue <- ReservedIngressQueue.bounded[IO, String](Limits(maxItems = 1, maxBytes = 8L, maxItemBytes = 8L))
      _ <- queue.tryOffer("failed", 4L)
      result <- queue.takeAndUse[Unit](_ => throw failure).attempt
      usage <- queue.usage
    } yield expect.same(Left(failure), result).and(expect.same(Usage(0, 0L), usage))
  }

  test("processing cancellation releases the reservation") {
    for {
      queue <- ReservedIngressQueue.bounded[IO, String](Limits(maxItems = 1, maxBytes = 8L, maxItemBytes = 8L))
      _ <- queue.tryOffer("canceled", 4L)
      entered <- Deferred[IO, Unit]
      never <- Deferred[IO, Unit]
      fiber <- queue.takeAndUse(_ => entered.complete(()) >> never.get).start
      _ <- entered.get.timeout(1.second)
      _ <- fiber.cancel
      usage <- queue.usage
      replacement <- queue.tryOffer("replacement", 8L)
    } yield expect.same(Usage(0, 0L), usage).and(expect.same(OfferResult.Accepted, replacement))
  }

  test("concurrent offers cannot exceed one item reservation") {
    for {
      queue <- ReservedIngressQueue.bounded[IO, Int](Limits(maxItems = 1, maxBytes = 1L, maxItemBytes = 1L))
      results <- List(1, 2).parTraverse(value => queue.tryOffer(value, 1L))
      usage <- queue.usage
    } yield expect.same(1, results.count(_ == OfferResult.Accepted)).and(expect.same(Usage(1, 1L), usage))
  }

  test("a concurrent admission burst remains bounded and all consumed reservations return to zero") {
    val offers = (1 to 256).toList

    for {
      queue <- ReservedIngressQueue.bounded[IO, Int](Limits(maxItems = 16, maxBytes = 64L, maxItemBytes = 8L))
      results <- offers.parTraverse(value => queue.tryOffer(value, (value % 8 + 1).toLong))
      accepted = results.count(_ == OfferResult.Accepted)
      peak <- queue.usage
      _ <- List.fill(accepted)(queue.takeAndUse(IO.pure)).parSequence
      after <- queue.usage
    } yield expect(peak.outstandingItems <= 16)
      .and(expect(peak.outstandingBytes <= 64L))
      .and(expect.same(accepted, peak.outstandingItems))
      .and(expect.same(Usage(0, 0L), after))
  }

  test("Long.MaxValue byte limits do not overflow accounting") {
    for {
      queue <- ReservedIngressQueue.bounded[IO, String](
        Limits(maxItems = 2, maxBytes = Long.MaxValue, maxItemBytes = Long.MaxValue)
      )
      accepted <- queue.tryOffer("maximum", Long.MaxValue)
      rejected <- queue.tryOffer("overflow", 1L)
      atMaximum <- queue.usage
      _ <- queue.takeAndUse(IO.pure)
      after <- queue.usage
    } yield expect.same(OfferResult.Accepted, accepted)
      .and(expect.same(OfferResult.ByteCapacityExceeded(1L, 0L, Long.MaxValue), rejected))
      .and(expect.same(Usage(1, Long.MaxValue), atMaximum))
      .and(expect.same(Usage(0, 0L), after))
  }

  test("invalid limits fail construction") {
    List(
      Limits(maxItems = 0, maxBytes = 1L, maxItemBytes = 1L),
      Limits(maxItems = 1, maxBytes = 0L, maxItemBytes = 1L),
      Limits(maxItems = 1, maxBytes = 1L, maxItemBytes = 0L),
      Limits(maxItems = 1, maxBytes = 1L, maxItemBytes = 2L)
    ).parTraverse(ReservedIngressQueue.bounded[IO, Unit](_).attempt).map { results =>
      expect(results.forall(_.left.exists(_.isInstanceOf[IllegalArgumentException])))
    }
  }
}
