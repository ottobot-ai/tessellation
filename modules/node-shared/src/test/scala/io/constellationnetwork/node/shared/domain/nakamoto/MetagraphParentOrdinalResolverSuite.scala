package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.{SnapshotOrdinal, SnapshotTips}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.MutableIOSuite

/** Tests for [[MetagraphParentOrdinalResolver]] — the #213/#290 fix that derives the metagraph parent ordinal from the INCOMING binary's
  * own content (ordinal − 1) instead of reading it from the gl0 GSI currency partitions (which `calculateLastCurrencySnapshots` drops via
  * `.filterNot(_.isEmpty)` whenever the metagraph produced no state in the window → the permanent admission deadlock).
  *
  * The crux test is `tip-match + both currency partitions empty → resolves via ordinal − 1`: that is the EXACT state the bug produced
  * (`lastStateChannelSnapshotHashes[mg]` written, both currency partitions dropped). The legacy `resolve` path returns `None` there; the
  * new `resolveFromBinary` path returns `Some(ordinal − 1)`.
  */
object MetagraphParentOrdinalResolverSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private val mgAddr: Address = Address.fromBytes("mg-resolver".getBytes("UTF-8"))
  private val tipHash: Hash = Hash("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
  private val otherHash: Hash = Hash("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

  /** A `GlobalStateReader` stub that answers the `LastStateChannelSnapshotHashes` key with `tipHashOpt` (the mg's recorded tip on this
    * peer) and returns `None` for EVERYTHING else — including both currency partitions (`LastIncrementalCurrencySnapshots` /
    * `LastCurrencySnapshots`). That `None` for the currency partitions is precisely the post-`.filterNot(_.isEmpty)` state that deadlocks
    * the legacy GSI resolver; `resolveFromBinary` must not depend on it.
    */
  private def readerWithTip(tipHashOpt: Option[Hash]): GlobalStateReader[IO] =
    new GlobalStateReader[IO] {
      def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] =
        key.fieldId match {
          case GlobalStateFieldId.LastStateChannelSnapshotHashes =>
            // `getLastStateChannelSnapshotHash` decodes the value as `Hash`; V =:= Hash at this call site.
            IO.pure(tipHashOpt.asInstanceOf[Option[V]])
          case _ =>
            // Both currency partitions (and anything else) are absent — the post-`.filterNot(_.isEmpty)` deadlock state.
            IO.pure(none[V])
        }
      def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] = IO.pure(Map.empty[GlobalStateKey, V])
      def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty[Hex, V])
    }

  /** Build a signed incremental snapshot at `ordinal`. Field shape mirrors `GlobalSnapshotStateChannelEventsProcessorSuite`. */
  private def mkIncrementalSnapshot(
    ordinal: Long
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[CurrencyIncrementalSnapshot]] = {
    val snapshot = CurrencyIncrementalSnapshot(
      SnapshotOrdinal.unsafeApply(ordinal),
      Height.MinValue,
      SubHeight.MinValue,
      Hash.empty,
      SortedSet.empty,
      SortedSet.empty,
      SnapshotTips(SortedSet.empty, SortedSet.empty),
      CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
      EpochProgress.MinValue,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
    KeyPairGenerator.makeKeyPair[IO].flatMap(kp => Signed.forAsyncHasher[IO, CurrencyIncrementalSnapshot](snapshot, kp))
  }

  /** Serialize a signed incremental snapshot to the `content` bytes that ride inside `StateChannelSnapshotBinary.content` — the SAME
    * `JsonSerializer` path the production decode (`GlobalSnapshotStateChannelEventsProcessor.deserialize`) consumes.
    */
  private def contentBytesForIncremental(
    ordinal: Long
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO], json: JsonSerializer[IO]): IO[Array[Byte]] =
    mkIncrementalSnapshot(ordinal).flatMap(signed => JsonSerializer[IO].serialize(signed))

  // ─── parentOrdinalFromContent (the reader-independent, load-bearing derivation) ───────────────────────

  test("parentOrdinalFromContent: incremental ordinal N -> Some(N - 1)") { res =>
    implicit val (h, sp, json) = res
    for {
      content <- contentBytesForIncremental(5L)
      result <- MetagraphParentOrdinalResolver.parentOrdinalFromContent[IO](content)
    } yield expect(result == Some(4L))
  }

  test("parentOrdinalFromContent: incremental ordinal 1 (first incremental over genesis) -> Some(0)") { res =>
    implicit val (h, sp, json) = res
    for {
      content <- contentBytesForIncremental(1L)
      result <- MetagraphParentOrdinalResolver.parentOrdinalFromContent[IO](content)
    } yield expect(result == Some(0L))
  }

  test("parentOrdinalFromContent: ordinal 0 clamps the parent to 0 (no underflow)") { res =>
    implicit val (h, sp, json) = res
    for {
      content <- contentBytesForIncremental(0L)
      result <- MetagraphParentOrdinalResolver.parentOrdinalFromContent[IO](content)
    } yield expect(result == Some(0L))
  }

  test("parentOrdinalFromContent: undecodable content -> None (fail-closed)") { res =>
    implicit val (h, sp, json) = res
    val garbage = "not a currency snapshot".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    MetagraphParentOrdinalResolver.parentOrdinalFromContent[IO](garbage).map(result => expect(result == None))
  }

  test("parentOrdinalFromContent: pure function of content - identical bytes give identical ordinal") { res =>
    implicit val (h, sp, json) = res
    for {
      content <- contentBytesForIncremental(9L)
      a <- MetagraphParentOrdinalResolver.parentOrdinalFromContent[IO](content)
      b <- MetagraphParentOrdinalResolver.parentOrdinalFromContent[IO](content)
    } yield expect(a == Some(8L)) && expect(a == b)
  }

  // ─── resolveFromBinary (identity guard + ordinal-from-content) ────────────────────────────────────────

  test("resolveFromBinary: THE BUG CASE - tip matches but both currency partitions empty -> resolves via ordinal - 1") { res =>
    implicit val (h, sp, json) = res
    // reader: tip == parentHash, currency partitions empty (the `.filterNot(_.isEmpty)` drop state).
    val reader = readerWithTip(Some(tipHash))
    for {
      content <- contentBytesForIncremental(7L) // this binary is ordinal 7 -> parent is ordinal 6
      result <- MetagraphParentOrdinalResolver.resolveFromBinary[IO](reader, mgAddr, tipHash, content)
    } yield expect(result == Some(6L))
  }

  test("resolveFromBinary: identity guard - parentHash != recorded tip -> None (even with valid content)") { res =>
    implicit val (h, sp, json) = res
    // reader's recorded tip is `tipHash`, but the incoming binary claims a different parent.
    val reader = readerWithTip(Some(tipHash))
    for {
      content <- contentBytesForIncremental(7L)
      result <- MetagraphParentOrdinalResolver.resolveFromBinary[IO](reader, mgAddr, otherHash, content)
    } yield expect(result == None)
  }

  test("resolveFromBinary: no GSI entry for the metagraph (pre-bootstrap) -> None") { res =>
    implicit val (h, sp, json) = res
    val reader = readerWithTip(None)
    for {
      content <- contentBytesForIncremental(7L)
      result <- MetagraphParentOrdinalResolver.resolveFromBinary[IO](reader, mgAddr, tipHash, content)
    } yield expect(result == None)
  }

  test("resolveFromBinary: tip matches + content undecodable -> None (fail-closed)") { res =>
    implicit val (h, sp, json) = res
    val reader = readerWithTip(Some(tipHash))
    val garbage = "garbage".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    MetagraphParentOrdinalResolver.resolveFromBinary[IO](reader, mgAddr, tipHash, garbage).map(result => expect(result == None))
  }

  test("resolveFromBinary: genesis - first incremental (ordinal 1) over a recorded genesis tip -> Some(0)") { res =>
    implicit val (h, sp, json) = res
    val reader = readerWithTip(Some(tipHash))
    for {
      content <- contentBytesForIncremental(1L)
      result <- MetagraphParentOrdinalResolver.resolveFromBinary[IO](reader, mgAddr, tipHash, content)
    } yield expect(result == Some(0L))
  }
}
