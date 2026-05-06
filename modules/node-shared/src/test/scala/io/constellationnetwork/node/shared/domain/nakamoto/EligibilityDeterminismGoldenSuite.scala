package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.RatioInstances._
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.nakamoto.LddConfig

import weaver.SimpleIOSuite

/** Determinism golden vectors for VRF eligibility.
  *
  * Locks the byte-exact `Ratio` outputs of `EligibilityChecker.threshold` across the LDD phase regimes (dormant boundary, ramp, baseline)
  * against `LddConfig.Default`. Future refactors of `Ratio`, the `Log1p` / `Exp` interpreters, or `LddConfig` that change a numeric output
  * will fail this suite with a numerator / denominator delta — BEFORE shipping.
  *
  * Captured against:
  *   - LddConfig.Default: ψ=1, γ=15, fA=1/2, fB=1/20
  *   - Log1pInterpreter precision = 8
  *   - ExpInterpreter precision = 38
  *   - Lentz max iterations = 10000
  *
  * If you intentionally change the captured arithmetic and need to refresh these vectors, run the suite once, copy each actual
  * `Ratio(numerator, denominator)` from the failure into the corresponding `expected` literal, and document why the change is consensus-safe
  * in the commit message.
  */
object EligibilityDeterminismGoldenSuite extends SimpleIOSuite {

  private val config: LddConfig = LddConfig.Default

  // Match production interpreter precisions (see EligibilityCheckerSuite + Bifrost prod).
  private val checkerIO: IO[EligibilityChecker[IO]] =
    for {
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8)
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38)
    } yield EligibilityChecker.make[IO](log1p, exp)

  /** A single threshold golden vector. */
  private case class GoldenVector(
    label: String,
    slotGap: Long,
    relativeStake: Ratio,
    expected: Ratio
  )

  // ---- Threshold golden vectors -------------------------------------------------------
  //
  // Regime mapping for LddConfig.Default (offset=1, lddCutoff=15):
  //   - gap = 0: δ < ψ → difficulty = 0 → threshold = 0 (dormant)
  //   - gap = 1: δ == ψ, ramp formula degenerates ((slotGap-offset)/(γ-ψ) = 0/14 = 0) → threshold = 0
  //   - gaps 2..14: ramp regime, difficulty = (1/2) × (gap - 1) / 14, exact rational
  //   - gaps ≥ 15: baseline regime, difficulty = 1/20

  private val goldens: List[GoldenVector] = List(
    // -- Dormant boundary --
    GoldenVector(
      label = "gap=0, stake=1: dormant (δ < ψ) → 0",
      slotGap = 0L,
      relativeStake = Ratio.One,
      expected = Ratio.Zero
    ),
    GoldenVector(
      label = "gap=1, stake=1/2: ψ-buffer (δ == ψ, ramp formula = 0) → 0",
      slotGap = 1L,
      relativeStake = Ratio(1, 2),
      expected = Ratio.Zero
    ),
    // -- Ramp regime (low / medium / high stake) --
    GoldenVector(
      label = "gap=5, stake=1/10 (low): ramp",
      slotGap = 5L,
      relativeStake = Ratio(1, 10),
      expected = Ratio(
        BigInt(
          "475123528161024259877926116121895373013892060172260934515432807651547981328813485499402571822293462065826899356409667266944484230506940779142130773132353706106866500967909764223613605481576052489155127274765372872498892361902649046114325596946949741607103190513703558868273477209775451010192178462349444515395033032947667381782837486339056701194109566354405996244644910768153833807026221274136626653943956933087937719021602127365228249819114555605538832950872228307"
        ),
        BigInt(
          "31060191744459933332839818360508332251910234301191277946913671083049744207152940046918114860011122533188153228591520418455981612824271063026171332841668206017321510583799892065024000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        )
      )
    ),
    GoldenVector(
      label = "gap=10, stake=1/2 (medium): ramp",
      slotGap = 10L,
      relativeStake = Ratio(1, 2),
      expected = Ratio(
        BigInt(
          "1226274051304700234533090041577327394736246725411975674009467361154332834423005282561799310237373053168687859013885641113818745942314047784987885994987646202893336641054953019381261141632829596491821756907268278877977751117787697166083186853545586733520624266153193649244302549877847827831600929467430787289787661364715723288187936540015091242700630771365169771109955304134733676127840811029483648433361058822324217613853118041446762034986068940077085729532502592209502604965747528022715773983058911480650512862558545212115516395153209977626876646365243317109044884833161612003627105312511652788378072660039347852832306318436361044226025185310638864996595360520920792076959284716481986566983673510827335315352483752280029796866659867008828818007400317609603494863231561338978302790168743163233835073710491342074356839761013332246651163567714665736108116998581064210540850174565820007411704416061307683910680022926657600464603576957465180701489018350992365955707986288882886264535427364193099637231711951071279810821061"
        ),
        BigInt(
          "6957759773555856205429145185407315766316952072578414034574957237765951931001665692281768407450050509834852356941685124721510938910801967474728759147996791591244829846803751984754878207666350356650040492101910826494004794628893007951112788888575710326167690104454051208907437596104785586822459069906074584574263976278150462512594411441696345155947202113549223693616831689625466268534827435009079825533354274020092387043191981351768876646502563282245271183349859480778118655936339477609454986358812761179551477003212939762994417890486556515023473654211253467324537821396755257376112640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        )
      )
    ),
    // -- Baseline regime (low / medium / high stake) --
    GoldenVector(
      label = "gap=15, stake=1 (high, boundary): baseline",
      slotGap = 15L,
      relativeStake = Ratio.One,
      expected = Ratio(
        BigInt(
          "52941744985501034178226425208546940511232153603276643900197675488135146078194728808617168805905428751021209451202828795937622109426051618057309146509298026256743294916656355140814936064613624478046830429353821160406177419704974497265804419134971978931429520931516232803240118967375362308519063957489654157034286354462085320194306512093571489041213860233198003679305591"
        ),
        BigInt(
          "1058834899714592382164569538056763580874752000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        )
      )
    ),
    GoldenVector(
      label = "gap=30, stake=1/3: baseline",
      slotGap = 30L,
      relativeStake = Ratio(1, 3),
      expected = Ratio(
        BigInt(
          "1431463278829940227091531906055453563050946562389391408250762456899236956152510990629329980602494393747732540461835663024862897739168728659602285096670394816992978550148353199755167402608579147860989273913761642801041268706771800166049513245211459390101755329493387188305305237678332635231516041621913717367071266859465433623185264319"
        ),
        BigInt(
          "84440017697962588560912514987158537542565888000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        )
      )
    ),
    GoldenVector(
      label = "gap=60, stake=1/100 (low): baseline (deep recovery)",
      slotGap = 60L,
      relativeStake = Ratio(1, 100),
      expected = Ratio(
        BigInt(
          "3178375122633307373983268671775961922272093050976572772456130150828066765064450960405187145592252230947158587341491467431154503116763989307874145253157815483502768709257532424471810923857932556892628743468400408274806551"
        ),
        BigInt(
          "6198062294394448773120000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        )
      )
    )
  )

  goldens.foreach { gv =>
    test(s"threshold golden — ${gv.label}") {
      for {
        checker <- checkerIO
        actual <- checker.threshold(gv.relativeStake, gv.slotGap, config)
      } yield expect.eql(gv.expected, actual)
    }
  }

  // ---- Eligibility decision golden ----------------------------------------------------
  //
  // The `checkEligibility` path is `threshold > vrfOutputAsRatio(vrfOutput)`. We exercise the
  // decision against two hardcoded 64-byte VRF outputs — one well below any plausible threshold
  // (eligible across all regimes), and one well above (ineligible everywhere). This locks both
  // the `vrfOutputAsRatio` mapping AND the comparison semantics.

  // VRF output that maps to a very small Ratio (near 0): leading bytes all zero except a low tail.
  private val tinyVrfOutput: Array[Byte] = {
    val a = new Array[Byte](64)
    a(63) = 0x01.toByte
    a
  }

  // VRF output that maps to a very large Ratio (near 1): all bytes 0xFF.
  private val maxVrfOutput: Array[Byte] = Array.fill[Byte](64)(0xff.toByte)

  pureTest("vrfOutputAsRatio — tinyVrfOutput maps to 1 / 2^512") {
    val r = EligibilityChecker.vrfOutputAsRatio(tinyVrfOutput)
    val expected = Ratio(BigInt(1), BigInt(2).pow(512))
    expect.eql(expected, r)
  }

  pureTest("vrfOutputAsRatio — maxVrfOutput maps to (2^512 - 1) / 2^512") {
    val r = EligibilityChecker.vrfOutputAsRatio(maxVrfOutput)
    val expected = Ratio(BigInt(2).pow(512) - 1, BigInt(2).pow(512))
    expect.eql(expected, r)
  }

  test("eligibility decision — tinyVrfOutput is eligible at gap=15, stake=1 (baseline)") {
    for {
      checker <- checkerIO
      thresh <- checker.threshold(Ratio.One, 15L, config)
      testVal = EligibilityChecker.vrfOutputAsRatio(tinyVrfOutput)
    } yield expect(thresh > testVal)
  }

  test("eligibility decision — maxVrfOutput is NOT eligible at gap=15, stake=1 (baseline)") {
    for {
      checker <- checkerIO
      thresh <- checker.threshold(Ratio.One, 15L, config)
      testVal = EligibilityChecker.vrfOutputAsRatio(maxVrfOutput)
    } yield expect(thresh < testVal)
  }

  test("eligibility decision — tinyVrfOutput is NOT eligible at gap=0 (dormant: threshold=0)") {
    for {
      checker <- checkerIO
      thresh <- checker.threshold(Ratio.One, 0L, config)
      testVal = EligibilityChecker.vrfOutputAsRatio(tinyVrfOutput)
      // Dormant: threshold = 0, testVal > 0 → NOT eligible (strict >).
    } yield expect(thresh == Ratio.Zero).and(expect(!(thresh > testVal)))
  }
}
