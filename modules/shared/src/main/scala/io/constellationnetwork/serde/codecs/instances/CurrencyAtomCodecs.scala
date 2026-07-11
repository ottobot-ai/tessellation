package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.currency.dataApplication.FeeTransaction
import io.constellationnetwork.schema.artifact._
import io.constellationnetwork.schema.cluster.SessionToken
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageType}
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.swap.{CurrencyId, SwapAmount}
import io.constellationnetwork.schema.tokenLock.TokenLockAmount
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.StringCodec.{codec => stringCodec}
import io.constellationnetwork.serde.codecs.UUIDCodec.{codec => uuidCodec}
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencyIdCodec.{codec => currencyIdCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import scodec.codecs.{discriminated, provide, uint8}
import scodec.{Attempt, Codec, Err}
import shapeless.{::, HNil}

/** Canonical scodec codecs for a batch of simple currency-path atoms.
  *
  * Covers: `MessageType`, `SessionToken`, `RoundId`, `SnapshotVersion`, `CurrencyMessage`, `FeeTransaction`, and `SpendTransaction`.
  *
  * All are leaf atoms with no further-unbuilt dependencies.
  */
object CurrencyAtomCodecs {

  // ---- MessageType (enumeratum string enum) ------------------------------
  // Discriminator bytes for MessageType. These are FROZEN — do not renumber.
  //   0x00: Owner
  //   0x01: Staking

  implicit val messageTypeCodec: Codec[MessageType] =
    discriminated[MessageType]
      .by(uint8)
      .typecase(0, provide(MessageType.Owner))
      .typecase(1, provide(MessageType.Staking))

  implicit val messageTypeImmutableCodec: ImmutableCodec[MessageType] =
    ImmutableCodec.fromScodecCodec(messageTypeCodec)

  // ---- Generation (PosLong) via shape typeclass -------------------------
  private val generationCodec: Codec[Generation] = Codec[Generation]

  // ---- SessionToken = Generation wrapper -------------------------------
  implicit val sessionTokenCodec: Codec[SessionToken] =
    generationCodec.xmap[SessionToken](SessionToken(_), _.value)

  implicit val sessionTokenImmutableCodec: ImmutableCodec[SessionToken] =
    ImmutableCodec.fromScodecCodec(sessionTokenCodec)

  // ---- RoundId = UUID wrapper ------------------------------------------
  implicit val roundIdCodec: Codec[RoundId] =
    uuidCodec.xmap[RoundId](RoundId(_), _.value)

  implicit val roundIdImmutableCodec: ImmutableCodec[RoundId] =
    ImmutableCodec.fromScodecCodec(roundIdCodec)

  // ---- SnapshotVersion (refined string with regex) ---------------------
  // String Refined MatchesRegex["^(0\\.0\\.1|1\\.0\\.0)$"] — decode validates the pattern.
  private val versionPredicate = """^(0\.0\.1|1\.0\.0)$""".r

  implicit val snapshotVersionCodec: Codec[SnapshotVersion] =
    stringCodec.exmap(
      s =>
        if (versionPredicate.matches(s))
          Attempt.successful(SnapshotVersion(Refined.unsafeApply[String, MatchesRegex["^(0\\.0\\.1|1\\.0\\.0)$"]](s)))
        else
          Attempt.failure(Err(s"SnapshotVersion decode: '$s' is not a valid version")),
      (v: SnapshotVersion) => Attempt.successful(v.version.value)
    )

  implicit val snapshotVersionImmutableCodec: ImmutableCodec[SnapshotVersion] =
    ImmutableCodec.fromScodecCodec(snapshotVersionCodec)

  // ---- CurrencyMessage -------------------------------------------------
  implicit val currencyMessageCodec: Codec[CurrencyMessage] =
    (messageTypeCodec :: addressCodec :: addressCodec :: Codec[io.constellationnetwork.schema.currencyMessage.MessageOrdinal])
      .xmap[CurrencyMessage](
        { case mt :: a :: m :: po :: HNil => CurrencyMessage(mt, a, m, po) },
        c => c.messageType :: c.address :: c.metagraphId :: c.parentOrdinal :: HNil
      )

  implicit val currencyMessageImmutableCodec: ImmutableCodec[CurrencyMessage] =
    ImmutableCodec.fromScodecCodec(currencyMessageCodec)

  // ---- FeeTransaction --------------------------------------------------
  implicit val feeTransactionCodec: Codec[FeeTransaction] =
    (addressCodec :: addressCodec :: Codec[io.constellationnetwork.schema.balance.Amount] :: hashCodec)
      .xmap[FeeTransaction](
        { case src :: dst :: amt :: ref :: HNil => FeeTransaction(src, dst, amt, ref) },
        f => f.source :: f.destination :: f.amount :: f.dataUpdateRef :: HNil
      )

  implicit val feeTransactionImmutableCodec: ImmutableCodec[FeeTransaction] =
    ImmutableCodec.fromScodecCodec(feeTransactionCodec)

  // ---- SpendTransaction ------------------------------------------------
  private val optionalHashCodec: Codec[Option[Hash]] = option(hashCodec)
  private val optionalCurrencyIdCodec: Codec[Option[CurrencyId]] = option(currencyIdCodec)
  private val swapAmountCodec: Codec[SwapAmount] = Codec[SwapAmount]

  implicit val spendTransactionCodec: Codec[SpendTransaction] =
    (optionalHashCodec :: optionalCurrencyIdCodec :: swapAmountCodec :: addressCodec :: addressCodec)
      .xmap[SpendTransaction](
        {
          case ref :: cid :: amt :: src :: dst :: HNil =>
            SpendTransaction(ref, cid, amt, src, dst)
        },
        s => s.allowSpendRef :: s.currencyId :: s.amount :: s.source :: s.destination :: HNil
      )

  implicit val spendTransactionImmutableCodec: ImmutableCodec[SpendTransaction] =
    ImmutableCodec.fromScodecCodec(spendTransactionCodec)

  // ---- TokenUnlock (SharedArtifact member) -----------------------------
  // Defined here because it's structurally atomic; the `SharedArtifact` discriminator is in a
  // separate codec.
  private val tokenLockAmountCodec: Codec[TokenLockAmount] = Codec[TokenLockAmount]

  implicit val tokenUnlockCodec: Codec[TokenUnlock] =
    (hashCodec :: tokenLockAmountCodec :: optionalCurrencyIdCodec :: addressCodec)
      .xmap[TokenUnlock](
        { case ref :: amt :: cid :: src :: HNil => TokenUnlock(ref, amt, cid, src) },
        t => t.tokenLockRef :: t.amount :: t.currencyId :: t.source :: HNil
      )

  implicit val tokenUnlockImmutableCodec: ImmutableCodec[TokenUnlock] =
    ImmutableCodec.fromScodecCodec(tokenUnlockCodec)

  // ---- AllowSpendExpiration (SharedArtifact member) --------------------
  implicit val allowSpendExpirationCodec: Codec[AllowSpendExpiration] =
    hashCodec.xmap[AllowSpendExpiration](AllowSpendExpiration(_), _.allowSpendRef)

  implicit val allowSpendExpirationImmutableCodec: ImmutableCodec[AllowSpendExpiration] =
    ImmutableCodec.fromScodecCodec(allowSpendExpirationCodec)
}
