package io.constellationnetwork.tools.migration.v4

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonBrotliBinarySerializer.BrotliDecodeLimits
import io.constellationnetwork.json.{JsonBrotliBinarySerializer, JsonSerializer}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.tools.migration.v4.V4CurrencyJsonEnvelopeVerificationError._
import io.constellationnetwork.tools.migration.v4.V4SourceEncoding.V4BrotliJson

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.encoder.BrotliOutputStream
import com.aayushatharva.brotli4j.encoder.Encoder.Parameters
import io.circe.syntax._
import io.circe.{Json, Printer}
import shapeless.test.illTyped
import weaver.{Expectations, MutableIOSuite}

object V4CurrencyBrotliJsonEnvelopeVerifierSuite extends MutableIOSuite {

  final case class Resources(json: JsonSerializer[IO], securityProvider: SecurityProvider[IO])
  final case class Fixture(signed: Signed[Json], hash: Hash, bytes: Array[Byte])

  type Res = Resources

  override def sharedResource: Resource[IO, Res] =
    JsonSerializer.forAsync[IO].asResource.flatMap { json =>
      SecurityProvider.forAsync[IO].map(Resources(json, _))
    }

  private val sourceOrdinal = SnapshotOrdinal.unsafeApply(1L)
  private val sourceContext = V4SourceContext(AppEnvironment.Dev, sourceOrdinal)
  private val canonicalPrinter = Printer(dropNullValues = true, indent = "", sortKeys = true)

  private val adjustmentAddress = Address.fromBytes(Array.fill(32)(7.toByte))
  private val balanceAdjustment = Json.obj(
    "BalanceAdjustment" -> Json.obj(
      "address" -> adjustmentAddress.asJson,
      "reason" -> Json.obj("SpendTransactionNotApplied" -> Json.obj()),
      "reference" -> Json.arr(Hash("4" * 64).asJson),
      "increase" -> Json.fromLong(7L)
    )
  )

  private val sourceValue = Json.obj(
    "ordinal" -> sourceOrdinal.asJson,
    "height" -> Json.fromLong(0L),
    "subHeight" -> Json.fromLong(0L),
    "lastSnapshotHash" -> Hash.empty.asJson,
    "blocks" -> Json.arr(),
    "rewards" -> Json.arr(),
    "tips" -> Json.obj("deprecated" -> Json.arr(), "remainedActive" -> Json.arr()),
    "stateProof" -> Json.obj(
      "lastTxRefsProof" -> Hash("1" * 64).asJson,
      "balancesProof" -> Hash("2" * 64).asJson,
      "activeAllowSpends" -> Hash("3" * 64).asJson
    ),
    "epochProgress" -> Json.fromLong(0L),
    "artifacts" -> Json.arr(balanceAdjustment),
    "version" -> Json.fromString("0.0.1")
  )

  private def fixture(value: Json = sourceValue)(implicit
    json: JsonSerializer[IO],
    securityProvider: SecurityProvider[IO]
  ): IO[Fixture] = {
    implicit val sourceHasher: Hasher[IO] = Hasher.forJson[IO]

    for {
      firstKey <- KeyPairGenerator.makeKeyPair[IO]
      secondKey <- KeyPairGenerator.makeKeyPair[IO]
      signed <- Signed.forAsyncHasher[IO, Json](value, firstKey).flatMap(_.signAlsoWith(secondKey))
      hash <- sourceHasher.hash(value)
      bytes <- json.serialize(signed)
    } yield Fixture(signed, hash, bytes)
  }

  private def generousLimits(bytes: Array[Byte]): BrotliDecodeLimits =
    BrotliDecodeLimits(bytes.length.toLong, 1024L * 1024L)

  private def compressRawJson(raw: String): IO[Array[Byte]] =
    IO.blocking {
      Brotli4jLoader.ensureAvailability()
      val output = new ByteArrayOutputStream()
      val brotli = new BrotliOutputStream(output, new Parameters().setQuality(2))

      try brotli.write(raw.getBytes(StandardCharsets.UTF_8))
      finally brotli.close()

      output.toByteArray
    }

  private def failClosed(
    result: Either[V4CurrencyJsonEnvelopeVerificationError, CryptographicallyValidV4CurrencyJsonEnvelope]
  ): Expectations =
    result match {
      case Left(DecodeFailure(_))          => success
      case Left(NonCanonicalEnvelopeBytes) => success
      case other                           => failure(s"expected structural/canonical rejection, got $other")
    }

  test("canonical raw v4 envelope preserves a historical-only BalanceAdjustment JSON member") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    for {
      source <- fixture()
      result <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](
        source.bytes,
        sourceContext,
        source.hash,
        generousLimits(source.bytes)
      )
      check <- result match {
        case Right(verified) =>
          Hash.fromBytesForSync[IO](verified.valueHashPreimageBytes).map { canonicalHash =>
            expect.same(source.signed.proofs, verified.legacyProofs) &&
              expect.same(source.hash, verified.valueHash) &&
              expect.same(source.hash, canonicalHash) &&
              expect.same(sourceContext, verified.context) &&
              expect(verified.originalCompressedBytes.sameElements(source.bytes))
          }
        case Left(error) => IO.pure(failure(s"expected cryptographically valid raw envelope, got $error"))
      }
    } yield check
  }

  test("wrong source era rejects before touching a null source") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    val kryoContext = V4SourceContext(AppEnvironment.Dev, SnapshotOrdinal.MinValue)

    V4CurrencyBrotliJsonEnvelopeVerifier
      .verify[IO](null, kryoContext, Hash.empty, BrotliDecodeLimits(1L, 1L))
      .map {
        case Left(WrongSourceEra(context, required)) =>
          expect.same(kryoContext, context) && expect.same(V4BrotliJson, required)
        case other => failure(s"expected wrong-era rejection, got $other")
      }
  }

  test("null context, source, expected hash, limits, and invalid limits are contained typed failures") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    val validLimits = BrotliDecodeLimits(1L, 1L)
    val invalidLimits = List(
      BrotliDecodeLimits(0L, 1L),
      BrotliDecodeLimits(1L, 0L),
      BrotliDecodeLimits(Int.MaxValue.toLong + 1L, 1L),
      BrotliDecodeLimits(1L, Int.MaxValue.toLong + 1L)
    )

    for {
      missingContext <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](Array.emptyByteArray, null, Hash.empty, validLimits).attempt
      missingSource <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](null, sourceContext, Hash.empty, validLimits).attempt
      missingHash <- V4CurrencyBrotliJsonEnvelopeVerifier
        .verify[IO](Array.emptyByteArray, sourceContext, null.asInstanceOf[Hash], validLimits)
        .attempt
      missingLimits <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](Array.emptyByteArray, sourceContext, Hash.empty, null).attempt
      invalid <- invalidLimits.traverse { limits =>
        V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](Array.emptyByteArray, sourceContext, Hash.empty, limits).attempt
      }
    } yield
      expect.same(Right(Left(MissingSourceContext)), missingContext) &&
        expect.same(Right(Left(MissingCompressedSource)), missingSource) &&
        expect.same(Right(Left(MissingExpectedValueHash)), missingHash) &&
        expect.same(Right(Left(InvalidDecodeLimits(None))), missingLimits) &&
        expect(
          invalid.zip(invalidLimits).forall {
            case (Right(Left(InvalidDecodeLimits(Some(actual)))), expected) => actual == expected
            case _                                                         => false
          }
        )
  }

  test("embedded ordinal and externally supplied value hash are independently enforced") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    val otherContext = V4SourceContext(AppEnvironment.Dev, SnapshotOrdinal.unsafeApply(2L))
    val wrongHash = Hash("f" * 64)

    for {
      source <- fixture()
      ordinalResult <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](
        source.bytes,
        otherContext,
        source.hash,
        generousLimits(source.bytes)
      )
      hashResult <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](
        source.bytes,
        sourceContext,
        wrongHash,
        generousLimits(source.bytes)
      )
    } yield {
      val ordinalCheck = ordinalResult match {
        case Left(OrdinalMismatch(expected, actual)) =>
          expect.same(otherContext.ordinal, expected) && expect.same(sourceOrdinal, actual)
        case other => failure(s"expected ordinal mismatch, got $other")
      }
      val hashCheck = hashResult match {
        case Left(HashMismatch(expected, actual)) => expect.same(wrongHash, expected) && expect(actual != wrongHash)
        case other                                => failure(s"expected hash mismatch, got $other")
      }

      ordinalCheck && hashCheck
    }
  }

  test("changing the signed raw value invalidates every retained proof") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider
    implicit val sourceHasher: Hasher[IO] = Hasher.forJson[IO]

    for {
      source <- fixture()
      changedValue = source.signed.value.mapObject(_.add("lastSnapshotHash", Hash("a" * 64).asJson))
      changed = source.signed.copy(value = changedValue)
      changedHash <- sourceHasher.hash(changedValue)
      changedBytes <- json.serialize(changed)
      result <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](
        changedBytes,
        sourceContext,
        changedHash,
        generousLimits(changedBytes)
      )
    } yield result match {
      case Left(InvalidSignatures(proofs)) => expect.same(changed.proofs, proofs)
      case other                           => failure(s"expected invalid-signature rejection, got $other")
    }
  }

  test("compressed and decompressed limits reject before semantic verification") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    for {
      source <- fixture()
      compressed <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](
        source.bytes,
        sourceContext,
        source.hash,
        BrotliDecodeLimits(source.bytes.length.toLong - 1L, 1024L * 1024L)
      )
      decompressed <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](
        source.bytes,
        sourceContext,
        source.hash,
        BrotliDecodeLimits(source.bytes.length.toLong, 1L)
      )
    } yield {
      val compressedCheck = compressed match {
        case Left(DecodeFailure(_: JsonBrotliBinarySerializer.CompressedContentTooLarge)) => success
        case other => failure(s"expected compressed-size failure, got $other")
      }
      val decompressedCheck = decompressed match {
        case Left(DecodeFailure(_: JsonBrotliBinarySerializer.DecompressedContentTooLarge)) => success
        case other => failure(s"expected decompressed-size failure, got $other")
      }

      compressedCheck && decompressedCheck
    }
  }

  test("malformed, truncated, duplicate-key, and trailing input fail closed") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    for {
      source <- fixture()
      malformed <- compressRawJson("{")
      valueRaw = canonicalPrinter.print(source.signed.value)
      proofsRaw = canonicalPrinter.print(source.signed.proofs.asJson)
      duplicate <- compressRawJson(s"{\"value\":$valueRaw,\"value\":$valueRaw,\"proofs\":$proofsRaw}")
      trailing <- compressRawJson(canonicalPrinter.print(source.signed.asJson) + " trailing")
      inputs = List(malformed, source.bytes.dropRight(1), duplicate, trailing)
      results <- inputs.traverse { bytes =>
        V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](bytes, sourceContext, source.hash, generousLimits(bytes))
      }
    } yield results.map(failClosed).reduce(_ && _)
  }

  test("canonical envelope byte check rejects appended Brotli garbage") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    for {
      source <- fixture()
      appended = source.bytes ++ Array[Byte](0x00, 0x01, 0x02)
      result <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](
        appended,
        sourceContext,
        source.hash,
        generousLimits(appended)
      )
    } yield failClosed(result)
  }

  test("outer and currency key grammar rejects extras, missing required keys, explicit nulls, and duplicate proofs") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    for {
      source <- fixture()
      outer = source.signed.asJson
      extraOuter = outer.mapObject(_.add("extra", Json.True))
      missingOuter = outer.mapObject(_.remove("proofs"))
      extraValue = source.signed.value.mapObject(_.add("forkOnly", Json.True))
      missingValue = source.signed.value.mapObject(_.remove("version"))
      explicitNullValue = source.signed.value.mapObject(_.add("messages", Json.Null))
      proofs = outer.hcursor.downField("proofs").focus.flatMap(_.asArray).get
      duplicateProofs = outer.mapObject(_.add("proofs", Json.fromValues(proofs :+ proofs.head)))
      unsortedProofs = outer.mapObject(_.add("proofs", Json.fromValues(proofs.reverse)))
      ordinaryJson = List[Json](
        extraOuter,
        missingOuter,
        Signed(extraValue, source.signed.proofs).asJson,
        Signed(missingValue, source.signed.proofs).asJson
      )
      ordinaryBytes <- ordinaryJson.traverse(value => json.serialize[Json](value))
      explicitNullBytes <- compressRawJson(Signed(explicitNullValue, source.signed.proofs).asJson.noSpaces)
      duplicateProofBytes <- json.serialize(duplicateProofs)
      unsortedProofBytes <- json.serialize(unsortedProofs)
      inputs = ordinaryBytes ++ List(explicitNullBytes, duplicateProofBytes, unsortedProofBytes)
      results <- inputs.traverse { bytes =>
        V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](bytes, sourceContext, source.hash, generousLimits(bytes))
      }
    } yield results.map(failClosed).reduce(_ && _)
  }

  test("retained envelope and value-hash preimage bytes cannot be mutated through caller references") { res =>
    implicit val json: JsonSerializer[IO] = res.json
    implicit val securityProvider: SecurityProvider[IO] = res.securityProvider

    for {
      source <- fixture()
      pristineSource = source.bytes.clone()
      result <- V4CurrencyBrotliJsonEnvelopeVerifier.verify[IO](
        source.bytes,
        sourceContext,
        source.hash,
        generousLimits(source.bytes)
      )
    } yield result match {
      case Right(verified) =>
        val pristineCanonical = verified.valueHashPreimageBytes
        source.bytes(0) = (source.bytes(0) ^ 0xff).toByte
        val firstSourceRead = verified.originalCompressedBytes
        val firstCanonicalRead = verified.valueHashPreimageBytes
        firstSourceRead(0) = (firstSourceRead(0) ^ 0xff).toByte
        firstCanonicalRead(0) = (firstCanonicalRead(0) ^ 0xff).toByte

        expect(verified.originalCompressedBytes.sameElements(pristineSource)) &&
          expect(verified.valueHashPreimageBytes.sameElements(pristineCanonical)) &&
          expect(!verified.originalCompressedBytes.sameElements(source.bytes)) &&
          expect(!verified.originalCompressedBytes.sameElements(firstSourceRead)) &&
          expect(!verified.valueHashPreimageBytes.sameElements(firstCanonicalRead))
      case Left(error) => failure(s"expected cryptographically valid raw envelope, got $error")
    }
  }

  test("raw stage-1 evidence exposes no parsed object, active snapshot, hasher, or public constructor") { _ =>
    illTyped("""null.asInstanceOf[CryptographicallyValidV4CurrencyJsonEnvelope].value""")
    illTyped("""null.asInstanceOf[CryptographicallyValidV4CurrencyJsonEnvelope].json""")
    illTyped("""null.asInstanceOf[CryptographicallyValidV4CurrencyJsonEnvelope].hasher""")
    illTyped(
      """new CryptographicallyValidV4CurrencyJsonEnvelope(Array.emptyByteArray, Array.emptyByteArray, null, null, null)"""
    )
    illTyped(
      """val active: io.constellationnetwork.security.signature.Signed[io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot] = null.asInstanceOf[CryptographicallyValidV4CurrencyJsonEnvelope]"""
    )

    IO.pure(success)
  }
}
