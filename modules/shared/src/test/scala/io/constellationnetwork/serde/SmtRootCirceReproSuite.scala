package io.constellationnetwork.serde

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.foldable._
import cats.syntax.traverse._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher

import eu.timepit.refined.auto._
import io.circe.syntax._
import weaver.MutableIOSuite

/** PART-1 repro: round-trips a `GlobalSnapshotStateProof` (and a `Signed[GlobalIncrementalSnapshot]` carrying one) with `smtRoot =
  * Some(...)` through the EXACT circe path the cl1/dl1 follower uses, to confirm/exonerate the hypothesis that the circe decoder mishandles
  * the 19th field and breaks `toHashedWithSignatureCheck` at ordinal 256.
  *
  * The follower's re-hash is purely a function of the ENCODER (Hasher.forJson → JsonSerializer.serialize → Blake2b). The signed value the
  * follower holds is `decode(wireBytes)`. So the load-bearing property is hash-stability: encode(decode(encode(x))) == encode(x).
  */
object SmtRootCirceReproSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    JsonSerializer.forAsync[IO].asResource.flatMap { implicit json =>
      val hasher = Hasher.forJson[IO]
      SecurityProvider.forAsync[IO].map(sp => (json, hasher, sp))
    }

  private def h(b: String): Hash = Hash(b * 32)

  private val someSmtRoot: Hash = h("ab")
  // The signed currency-snapshots partition roots (field 4). Set to Some throughout so this suite ALSO proves the proof codec / circe
  // path round-trips + re-hashes stably with the currency-roots field populated (not just smtRoot).
  private val someCurrencyRoots: CurrencySnapshotMptRoots = CurrencySnapshotMptRoots(h("cc"), h("dd"))

  private def proofWith(smt: Option[Hash]): GlobalSnapshotStateProof =
    GlobalSnapshotStateProof(
      lastStateChannelSnapshotHashesProof = h("11"),
      lastTxRefsProof = h("22"),
      balancesProof = h("33"),
      lastCurrencySnapshotsProof = Some(someCurrencyRoots),
      activeAllowSpends = None,
      activeTokenLocks = None,
      tokenLockBalances = None,
      lastAllowSpendRefs = None,
      lastTokenLockRefs = None,
      updateNodeParameters = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = None,
      nodeCollateralWithdrawals = None,
      priceState = None,
      lastGlobalSnapshotsWithCurrency = None,
      mptRoot = Some(h("99")),
      historicalStakeSnapshots = Some(h("88")),
      smtRoot = smt
    )

  private def snapshotWith(proof: GlobalSnapshotStateProof): GlobalIncrementalSnapshot =
    GlobalIncrementalSnapshot(
      ordinal = SnapshotOrdinal.unsafeApply(256L),
      height = Height(0L),
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      shardCheckpoints = SortedMap.empty,
      rewards = SortedSet.empty,
      delegateRewards = Some(SortedMap.empty),
      epochProgress = EpochProgress.MinValue,
      nextFacilitators = NonEmptyList.one(PeerId(Hex("aa" * 64))),
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = proof,
      allowSpendBlocks = Some(SortedSet.empty),
      tokenLockBlocks = Some(SortedSet.empty),
      spendActions = Some(SortedMap.empty),
      updateNodeParameters = Some(SortedMap.empty),
      artifacts = Some(SortedSet.empty),
      activeDelegatedStakes = Some(SortedMap.empty),
      delegatedStakesWithdrawals = Some(SortedMap.empty),
      activeNodeCollaterals = Some(SortedMap.empty),
      nodeCollateralWithdrawals = Some(SortedMap.empty),
      version = SnapshotVersion("0.0.1"),
      slotCertificate = None,
      eta = None
    )

  // ---- (1) bare proof circe round-trip --------------------------------------

  test("(1) GlobalSnapshotStateProof with smtRoot=Some + currencyRoots=Some round-trips through circe (decode∘encode == id)") { _ =>
    val original = proofWith(Some(someSmtRoot))
    val json = original.asJson
    val decoded = json.as[GlobalSnapshotStateProof]
    IO.pure(
      expect(decoded == Right(original))
        .and(expect.same(decoded.toOption.flatMap(_.smtRoot), Some(someSmtRoot)))
        .and(expect.same(decoded.toOption.flatMap(_.lastCurrencySnapshotsProof), Some(someCurrencyRoots)))
    )
  }

  test("(1b) GlobalSnapshotStateProof smtRoot=Some is hash-stable (encode∘decode∘encode == encode)") { _ =>
    val original = proofWith(Some(someSmtRoot))
    val firstJson = original.asJson.noSpaces
    val decoded = original.asJson.as[GlobalSnapshotStateProof].toOption.get
    val secondJson = decoded.asJson.noSpaces
    IO.pure(expect.same(firstJson, secondJson))
  }

  // ---- (2) Signed[GIS] through the real brotli-JSON serializer --------------

  test("(2) Signed[GlobalIncrementalSnapshot] smtRoot=Some survives brotli-JSON ser/deser with byte-identical re-encode") {
    case (json, _, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      val gis = snapshotWith(proofWith(Some(someSmtRoot)))
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        signed <- {
          implicit val hh: Hasher[IO] = Hasher.forJson[IO]
          forAsyncHasher(gis, kp)
        }
        wire <- json.serialize(signed)
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](wire)
        decoded = decodedE.toOption.get
        reWire <- json.serialize(decoded)
      } yield
        expect(decodedE.isRight)
          .and(expect.same(decoded.value.stateProof.smtRoot, Some(someSmtRoot)))
          .and(expect(decoded.value == gis))
          .and(expect(java.util.Arrays.equals(wire, reWire)))
  }

  // ---- (3) the EXACT follower path: toHashedWithSignatureCheck --------------

  test("(3) follower toHashedWithSignatureCheck succeeds for decoded Signed[GIS] with smtRoot=Some") {
    case (json, _, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val gis = snapshotWith(proofWith(Some(someSmtRoot)))
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        signed <- forAsyncHasher(gis, kp)
        wire <- json.serialize(signed)
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](wire)
        decoded = decodedE.toOption.get
        // This is line GlobalL0Service.scala:342/353/405 — the exact follower re-hash + sig check.
        checked <- decoded.toHashedWithSignatureCheck
      } yield expect(checked.isRight)
  }

  // ---- control: same path with smtRoot=None (must already pass at ord<=255) --

  test("(control) smtRoot=None Signed[GIS] toHashedWithSignatureCheck succeeds") {
    case (json, _, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val gis = snapshotWith(proofWith(None))
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        signed <- forAsyncHasher(gis, kp)
        wire <- json.serialize(signed)
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](wire)
        decoded = decodedE.toOption.get
        checked <- decoded.toHashedWithSignatureCheck
      } yield expect(checked.isRight).and(expect(decoded.value.stateProof.smtRoot.isEmpty))
  }

  // ---- REAL production bytes: the actual failing ord-256 snapshot from disk ----

  private def readResource(name: String): Array[Byte] = {
    val path = s"/serde/real/$name"
    val s = Option(getClass.getResourceAsStream(path)).getOrElse(throw new IllegalStateException(s"missing $path"))
    try s.readAllBytes()
    finally s.close()
  }

  // gl0's CLAIMED hash for ord 256 = the hash-storage filename hardlinked to the ordinal/256 file
  // (md5-identical across all 8 gl0 nodes). This is what gl0 computed at produce/sign time.
  private val gl0ClaimedHash256 = "0a8213d1c814597a14ef6ee0f50b5168f8476434ab7afb90bde775a125f63d19"

  test("(REAL-256) production ord-256 deserializes and the follower toHashedWithSignatureCheck reproduces the verdict") {
    case (json, _, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val raw = readResource("gl0_incremental_256.brotli")
      for {
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](raw)
        _ <- IO(println(s"[REAL-256] deserialize isRight=${decodedE.isRight}"))
        _ <- decodedE match {
          case Left(e)  => IO(println(s"[REAL-256] DECODE FAILED: $e"))
          case Right(_) => IO.unit
        }
        signed = decodedE.toOption.get
        _ <- IO(println(s"[REAL-256] stateProof.smtRoot=${signed.value.stateProof.smtRoot}"))
        _ <- IO(println(s"[REAL-256] stateProof.mptRoot=${signed.value.stateProof.mptRoot}"))
        _ <- IO(println(s"[REAL-256] stateProof.historicalStakeSnapshots=${signed.value.stateProof.historicalStakeSnapshots}"))
        _ <- IO(
          println(
            s"[REAL-256] inventory: blocks=${signed.value.blocks.size} scSnapshots=${signed.value.stateChannelSnapshots.size} " +
              s"rewards=${signed.value.rewards.size} delegateRewards=${signed.value.delegateRewards.map(_.size)} " +
              s"slotCertificate=${signed.value.slotCertificate.isDefined} eta=${signed.value.eta} proofs=${signed.proofs.length}"
          )
        )
        // the follower's recomputed value-hash (over the ENCODER, JSON path)
        recomputed <- hasher.hash(signed.value)
        _ <- IO(println(s"[REAL-256] gl0ClaimedHash = $gl0ClaimedHash256"))
        _ <- IO(println(s"[REAL-256] followerRehash = ${recomputed.value}"))
        _ <- IO(println(s"[REAL-256] hashesMatch    = ${recomputed.value == gl0ClaimedHash256}"))
        checked <- signed.toHashedWithSignatureCheck
        _ <- IO(println(s"[REAL-256] toHashedWithSignatureCheck isRight=${checked.isRight}"))
      } yield expect(decodedE.isRight)
  }

  test("(REAL-256-diff) re-encode the decoded ord-256 and locate the first byte/field that diverges") {
    case (json, _, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      val raw = readResource("gl0_incremental_256.brotli")
      for {
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](raw)
        signed = decodedE.toOption.get
        // canonical JSON the encoder emits for the decoded value (what the re-hash sees)
        reEmitted = signed.value.asJson.printWith(io.circe.Printer(dropNullValues = true, indent = "", sortKeys = true))
        // decode the ORIGINAL wire JSON to raw circe Json (no model round-trip) to compare apples-to-apples
        rawJsonStr <- IO {
          import java.io.ByteArrayInputStream
          import com.aayushatharva.brotli4j.Brotli4jLoader
          import com.aayushatharva.brotli4j.decoder.BrotliInputStream
          Brotli4jLoader.ensureAvailability()
          val bis = new BrotliInputStream(new ByteArrayInputStream(raw))
          try scala.io.Source.fromInputStream(bis).mkString
          finally bis.close()
        }
        origJson <- IO.fromEither(io.circe.parser.parse(rawJsonStr))
        origValueJson = origJson.hcursor.downField("value").focus.get
        origCanonical = origValueJson.printWith(io.circe.Printer(dropNullValues = true, indent = "", sortKeys = true))
        _ <- IO(println(s"[DIFF] origCanonical.length   = ${origCanonical.length}"))
        _ <- IO(println(s"[DIFF] reEmitted.length       = ${reEmitted.length}"))
        _ <- IO(println(s"[DIFF] canonical JSON equal   = ${origCanonical == reEmitted}"))
        _ <- IO {
          if (origCanonical != reEmitted) {
            val idx = origCanonical.zip(reEmitted).indexWhere { case (a, b) => a != b }
            val at = if (idx < 0) math.min(origCanonical.length, reEmitted.length) else idx
            val lo = math.max(0, at - 80)
            println(s"[DIFF] first divergence at char $at")
            println(s"[DIFF] ORIG …${origCanonical.slice(lo, at + 80)}…")
            println(s"[DIFF] RE   …${reEmitted.slice(lo, at + 80)}…")
          }
        }
      } yield expect(decodedE.isRight)
  }

  /** MECHANISM TEST: if any serve/store path serves a 256 whose `stateProof.smtRoot` differs from what was SIGNED (e.g. a GSI-rebuild path
    * that drops smtRoot to None, while the signature covers smtRoot=Some), the follower's `toHashedWithSignatureCheck` MUST yield
    * InvalidSignatureForHash. This pins whether "smtRoot present in sig but altered/absent in served value" reproduces the production
    * symptom.
    */
  test("(MECHANISM) signature over smtRoot=Some but served value has smtRoot stripped → InvalidSignatureForHash") {
    case (json, _, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val raw = readResource("gl0_incremental_256.brotli")
      for {
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](raw)
        signed = decodedE.toOption.get
        // signature is over smtRoot=Some; now serve a value with smtRoot stripped to None (proofs unchanged)
        mutatedValue = signed.value.copy(stateProof = signed.value.stateProof.copy(smtRoot = None))
        served = Signed(mutatedValue, signed.proofs)
        checkedStripped <- served.toHashedWithSignatureCheck
        // and the inverse: signature over smtRoot=Some, served with a DIFFERENT smtRoot value
        mutated2 = signed.value.copy(stateProof = signed.value.stateProof.copy(smtRoot = Some(h("de"))))
        served2 = Signed(mutated2, signed.proofs)
        checkedAltered <- served2.toHashedWithSignatureCheck
        _ <- IO(println(s"[MECHANISM] smtRoot stripped→None: isLeft(invalid)=${checkedStripped.isLeft}"))
        _ <- IO(println(s"[MECHANISM] smtRoot altered:       isLeft(invalid)=${checkedAltered.isLeft}"))
      } yield expect(checkedStripped.isLeft).and(expect(checkedAltered.isLeft))
  }

  /** Exhaustively simulate the HTTP serve→client path on the REAL ord-256 value: encode with EVERY printer the serve path could pick
    * (BlockingEntityEncoder = noSpaces+dropNullValues+NO sortKeys; standard http4s circe = noSpaces, NO dropNull; canonical = the
    * JsonSerializer printer), then decode via the follower's decoder and re-hash. If ANY variant breaks the follower re-hash, that pins the
    * transport asymmetry.
    */
  test("(HTTP-sim) every serve printer of real ord-256 survives follower decode→re-hash byte-stably") {
    case (json, _, sp) =>
      implicit val secProv: SecurityProvider[IO] = sp
      implicit val ser: JsonSerializer[IO] = json
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val raw = readResource("gl0_incremental_256.brotli")
      val servePrinter = io.circe.Printer.noSpaces.copy(dropNullValues = true) // BlockingEntityEncoder.defaultPrinter
      val stdPrinter = io.circe.Printer.noSpaces // standard http4s circeEntityEncoder
      val canonical = io.circe.Printer(dropNullValues = true, indent = "", sortKeys = true)
      for {
        decodedE <- json.deserialize[Signed[GlobalIncrementalSnapshot]](raw)
        signed = decodedE.toOption.get
        baseHash <- hasher.hash(signed.value)
        // encode the SIGNED envelope (value+proofs) the way the route does (Ok(snapshot))
        results <- List("serve(dropNull,noSort)" -> servePrinter, "std(noDropNull)" -> stdPrinter, "canonical" -> canonical).traverse {
          case (label, printer) =>
            val wireJson = signed.asJson.printWith(printer)
            IO.fromEither(io.circe.parser.decode[Signed[GlobalIncrementalSnapshot]](wireJson)).flatMap { redecoded =>
              hasher.hash(redecoded.value).map { rh =>
                (label, redecoded.value == signed.value, rh.value == baseHash.value, redecoded.value.stateProof.smtRoot)
              }
            }
        }
        _ <- results.traverse_ {
          case (label, valueEq, hashEq, smt) =>
            IO(println(s"[HTTP-sim] $label valueEq=$valueEq hashEq=$hashEq smtRoot=${smt.isDefined}"))
        }
      } yield expect(results.forall { case (_, _, hashEq, _) => hashEq })
  }
}
