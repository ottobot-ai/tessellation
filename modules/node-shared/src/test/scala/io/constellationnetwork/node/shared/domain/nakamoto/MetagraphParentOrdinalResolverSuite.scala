package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSyncView
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, StrictMptRead}
import io.constellationnetwork.schema.{SnapshotOrdinal, SnapshotTips}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.MutableIOSuite

/** Exact-anchor regression tests for [[MetagraphParentOrdinalResolver]]. The metagraph ordinal remains continuity data; committee eta,
  * active keys, and KES period must use the separately signed and Phase-2-verified GL0 reference.
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
  private val tipHash: Hash = Hash("a" * 64)
  private val otherHash: Hash = Hash("b" * 64)
  private val anchor: GlobalSyncView =
    GlobalSyncView(SnapshotOrdinal.unsafeApply(73L), Hash("c" * 64), EpochProgress.MinValue)

  /** Only the ML0 tip exists. Currency partitions are deliberately absent, matching the empty-framework-state case. */
  private def readerWithTip(tipHashOpt: Option[Hash]): GlobalStateReader[IO] =
    new GlobalStateReader[IO] {
      def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] =
        key.fieldId match {
          case GlobalStateFieldId.LastStateChannelSnapshotHashes => IO.pure(tipHashOpt.asInstanceOf[Option[V]])
          case _                                                 => IO.pure(none[V])
        }

      def getStrict[V: ImmutableCodec](key: GlobalStateKey): IO[StrictMptRead[V]] =
        get[V](key).map {
          case Some(value) => StrictMptRead.Present(value, ImmutableCodec[V].immutableBytes(value))
          case None        => StrictMptRead.Absent
        }

      def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] = IO.pure(Map.empty)
      def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty)
      def getAllForPrefixStrict[V: ImmutableCodec](
        prefix: Hex
      ): IO[List[io.constellationnetwork.schema.mpt.StrictMptEntry[V]]] = IO.pure(List.empty)
    }

  private def mkIncrementalSnapshot(ordinal: Long, globalSyncView: Option[GlobalSyncView])(
    implicit sp: SecurityProvider[IO],
    h: Hasher[IO]
  ): IO[Signed[CurrencyIncrementalSnapshot]] = {
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
      globalSyncView
    )
    KeyPairGenerator.makeKeyPair[IO].flatMap(kp => Signed.forAsyncHasher[IO, CurrencyIncrementalSnapshot](snapshot, kp))
  }

  private def contentBytes(ordinal: Long, globalSyncView: Option[GlobalSyncView])(
    implicit sp: SecurityProvider[IO],
    h: Hasher[IO],
    json: JsonSerializer[IO]
  ): IO[Array[Byte]] =
    mkIncrementalSnapshot(ordinal, globalSyncView).flatMap { signed =>
      JsonSerializer[IO].serialize[Signed[CurrencyIncrementalSnapshot]](signed)
    }

  test("currency context keeps ML0 continuity separate from the exact signed GL0 anchor") { res =>
    implicit val (h, sp, json) = res
    for {
      content <- contentBytes(7L, anchor.some)
      result <- MetagraphParentOrdinalResolver.currencyContextFromContent[IO](content)
    } yield expect(result.contains(MetagraphParentOrdinalResolver.CurrencyBinaryContext(6L, anchor)))
  }

  test("currency content without an exact GL0 anchor fails closed") { res =>
    implicit val (h, sp, json) = res
    for {
      content <- contentBytes(7L, None)
      result <- MetagraphParentOrdinalResolver.currencyContextFromContent[IO](content)
    } yield expect(result.isEmpty)
  }

  test("malformed or opaque content fails closed") { res =>
    implicit val json: JsonSerializer[IO] = res._3
    MetagraphParentOrdinalResolver
      .currencyContextFromContent[IO]("opaque".getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .map(result => expect(result.isEmpty))
  }

  test("tip identity plus signed content resolves without an ML0 currency partition") { res =>
    implicit val (h, sp, json) = res
    for {
      content <- contentBytes(7L, anchor.some)
      result <- MetagraphParentOrdinalResolver.resolveCurrencyContextFromBinary[IO](
        readerWithTip(tipHash.some),
        mgAddr,
        tipHash,
        content
      )
    } yield expect(result.contains(MetagraphParentOrdinalResolver.CurrencyBinaryContext(6L, anchor)))
  }

  test("mismatched or absent ML0 parent identity fails closed") { res =>
    implicit val (h, sp, json) = res
    for {
      content <- contentBytes(7L, anchor.some)
      mismatch <- MetagraphParentOrdinalResolver.resolveCurrencyContextFromBinary[IO](
        readerWithTip(tipHash.some),
        mgAddr,
        otherHash,
        content
      )
      absent <- MetagraphParentOrdinalResolver.resolveCurrencyContextFromBinary[IO](
        readerWithTip(None),
        mgAddr,
        tipHash,
        content
      )
    } yield expect(mismatch.isEmpty) && expect(absent.isEmpty)
  }

  test("Phase-2 capability binds the exact checked GL0 ordinal and hash") { _ =>
    val context = MetagraphParentOrdinalResolver.CurrencyBinaryContext(6L, anchor)
    for {
      verified <- MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext.verify[IO](context) { (ordinal, hash) =>
        IO.pure(ordinal === anchor.ordinal && hash === anchor.hash)
      }
    } yield
      expect(
        verified.exists(v => v.metagraphParentOrdinal == 6L && v.gl0AnchorOrdinal === anchor.ordinal && v.gl0AnchorHash === anchor.hash)
      )
  }

  test("Phase-2 capability is not minted when exact-hash qualification fails") { _ =>
    val context = MetagraphParentOrdinalResolver.CurrencyBinaryContext(6L, anchor)
    MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext
      .verify[IO](context)((_, _) => IO.pure(false))
      .map(result => expect(result.isEmpty))
  }
}
