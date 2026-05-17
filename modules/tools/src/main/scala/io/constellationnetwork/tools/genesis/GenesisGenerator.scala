package io.constellationnetwork.tools.genesis

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files => JFiles, Paths => JPaths}
import java.security.spec.ECGenParameterSpec
import java.security.{KeyPair, KeyPairGenerator => JKeyPairGenerator, SecureRandom => JSecureRandom}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.OperationalKeyMaker
import io.constellationnetwork.security.key.ops._
import io.constellationnetwork.security.key.{ECDSA, secp256k}
import io.constellationnetwork.security.signature.Signing

import eu.timepit.refined.refineV
import eu.timepit.refined.types.numeric.NonNegLong

/** Tier-1 test-vector generator. Emits a byte-deterministic `l0-genesis.json` for a given seed + flag set. See
  * `docs/nakamoto/IMPLEMENTATION-PLAN-POST-VALIDATION.md` §1.1 and `project_test_vector_pattern` memory for the design rationale
  * (genesis-template + generator + fixture library, mirroring Cardano's `cardano-cli genesis create-cardano`).
  *
  * Determinism contract: for a given (seed, flag-set), the emitted JSON file is byte-identical across runs. ECDSA signatures are NOT
  * deterministic in this codebase — see `L0GenesisLoader` docstring; the fixture stores RAW events plus delegator private-key hex and
  * signing happens at load time. This means the fixture-on-disk is reproducible, even if the runtime in-memory `Signed[...]` values aren't
  * byte-equal across cluster restarts.
  */
object GenesisGenerator {

  /** Reuses the existing operator credential at `nodes/N/key.p12` when `keysFromDir.isDefined`. Otherwise generates a fresh keypair via the
    * deterministic SHA1PRNG path so the operator address + peerId are seed-derived.
    */
  case class GeneratorOpts(
    outputDir: String,
    numOperators: Int,
    stakeDistribution: List[BigDecimal],
    stakeBudgetDatum: Long,
    collateralPerOperator: Long,
    initialBalancesCsv: Option[String],
    seed: Long,
    keysFromDir: Option[String],
    networkMagic: String,
    startingEpochProgress: Long
  )

  case class GeneratedOutputs(
    l0Genesis: L0GenesisData,
    cl1Genesis: Option[Cl1GenesisData],
    // §1.2 Slice 3b: per-operator KES SK byte blobs the caller writes to per-operator key
    // directories. Held in-memory inside the generator; the CLI dispatcher (`Main.generateGenesis`)
    // persists them via `writeKesSecretKeys`. Empty when KES generation is disabled.
    kesSecretKeys: List[OperatorKesSecretKey] = List.empty
  )

  /** Per-operator KES SK material. The CLI side writes `bytes` to `<output-dir>/keys/operator-<operatorIndex>/kes-sk.bin` with `chmod
    * 0600`.
    *
    * The byte blob is the same `SecretKeyCodec.encodeProductSk` output that the gl0 startup path will later consume from a disk-backed
    * `SecureStore` (Slice 4) to reopen the master key.
    */
  case class OperatorKesSecretKey(operatorIndex: Int, peerId: PeerId, bytes: Array[Byte])

  private def deterministicRng(seed: Long, salt: String): JSecureRandom = {
    val rng = JSecureRandom.getInstance("SHA1PRNG")
    // Mix seed + salt so independent draws (operator-key generation, delegator-key generation,
    // tokenLockRef hashing) each pull from distinct streams that are still reproducible from
    // the same root seed. Salt-naming convention: "<purpose>:<index>".
    val saltBytes = s"tessellation-nakamoto-genesis|$seed|$salt".getBytes("UTF-8")
    rng.setSeed(saltBytes)
    rng
  }

  /** Deterministic ECDSA key generation. BouncyCastle's `KeyPairGenerator.initialize(spec, rng)` accepts an arbitrary `SecureRandom` —
    * feeding it a `SHA1PRNG` seeded from the generator seed gives reproducible public/private bytes. This is the foundation of the
    * byte-determinism contract for operator + delegator + owner addresses.
    */
  def deterministicKeyPair[F[_]: Async: SecurityProvider](seed: Long, salt: String): F[KeyPair] =
    Async[F].delay {
      val rng = deterministicRng(seed, salt)
      val ecSpec = new ECGenParameterSpec(secp256k)
      val kpg = JKeyPairGenerator.getInstance(ECDSA, SecurityProvider[F].provider)
      kpg.initialize(ecSpec, rng)
      kpg.generateKeyPair()
    }

  /** Synthetic `tokenLockRef` hash derived deterministically from (seed, kind, index). For Tier-1 these references don't have to resolve
    * against `activeTokenLocks` because the `UpdateDelegatedStakeValidator` only fires on incoming transactions, NOT on already-seeded
    * `activeDelegatedStakes` records (verified by inspection — `validateCreateDelegatedStake` is called only from `DelegatedStakesRoutes`
    * and `UpdateDelegatedStakeAcceptanceManager`, both of which validate `signed` against `lastContext` where `lastContext` IS the seeded
    * GSI).
    */
  def syntheticTokenLockRef(seed: Long, kind: String, index: Int): Hash =
    Hash.fromBytes(s"$seed|$kind|$index".getBytes("UTF-8"))

  /** Allocate per-operator stake amounts given a relative-weight list and a budget. Uses BigDecimal arithmetic so the per-operator amounts
    * are deterministic regardless of source order; the residual (due to integer truncation) goes to operator 0.
    */
  def allocateStakeAmounts(weights: List[BigDecimal], budget: Long): List[Long] = {
    val total = weights.foldLeft(BigDecimal(0))(_ + _)
    if (total <= BigDecimal(0)) List.fill(weights.size)(0L)
    else {
      val raw = weights.map(w => (w / total * BigDecimal(budget)).toBigInt.toLong)
      val residual = budget - raw.sum
      if (raw.isEmpty) raw else (raw.head + residual) :: raw.tail
    }
  }

  /** Build a single `UpdateDelegatedStake.Create` event for a (delegator-address, operator-peerId) pair. The event carries the seeded
    * amount + synthetic tokenLockRef + empty parent (this is the first stake for the delegator).
    */
  def buildStakeEvent(
    delegatorAddr: Address,
    operatorPeerId: PeerId,
    amountLong: Long,
    tokenLockRef: Hash
  ): Either[String, UpdateDelegatedStake.Create] =
    for {
      amt <- refineV[eu.timepit.refined.numeric.NonNegative](amountLong)
    } yield
      UpdateDelegatedStake.Create(
        source = delegatorAddr,
        nodeId = operatorPeerId,
        amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(amt.value)),
        fee = DelegatedStakeFee(NonNegLong(0L)),
        tokenLockRef = tokenLockRef,
        parent = DelegatedStakeReference.empty
      )

  def buildCollateralEvent(
    ownerAddr: Address,
    operatorPeerId: PeerId,
    amountLong: Long,
    tokenLockRef: Hash
  ): Either[String, UpdateNodeCollateral.Create] =
    for {
      amt <- refineV[eu.timepit.refined.numeric.NonNegative](amountLong)
    } yield
      UpdateNodeCollateral.Create(
        source = ownerAddr,
        nodeId = operatorPeerId,
        amount = NodeCollateralAmount(NonNegLong.unsafeFrom(amt.value)),
        fee = NodeCollateralFee(NonNegLong(0L)),
        tokenLockRef = tokenLockRef,
        parent = NodeCollateralReference.empty
      )

  /** Encode an operator's private key as PKCS8 hex (the format the loader's `keyPairFromHex` expects). Public key is recoverable from the
    * private key via EC point multiplication, so we don't need to embed it separately in the fixture.
    */
  def encodePrivateKey(kp: KeyPair): String = Hex.fromBytes(kp.getPrivate.getEncoded).value

  /** Top-level entry point. Produces `(l0Genesis, optional cl1Genesis)`. The cl1Genesis is synthesized when `initialBalancesCsv` is
    * supplied; otherwise None (the caller can still commit the l0-only fixture for stake-distribution regression tests).
    */
  def generate[F[_]: Async: SecurityProvider](opts: GeneratorOpts, invocation: String): F[GeneratedOutputs] =
    for {
      _ <- validateOpts[F](opts)
      operatorKeys <- (0 until opts.numOperators).toList.traverse { i =>
        opts.keysFromDir match {
          case Some(dir) =>
            // Reuse the existing nodes/N/key.p12 keystore (the canonical operator credential).
            val keyPath = s"$dir/$i/key.p12"
            io.constellationnetwork.keytool.KeyStoreUtils.readKeyPairFromStore[F](
              keyPath,
              "alias",
              "password".toCharArray,
              "password".toCharArray
            )
          case None =>
            deterministicKeyPair[F](opts.seed, s"operator:$i")
        }
      }
      delegatorKeys <- (0 until opts.numOperators).toList.traverse { i =>
        deterministicKeyPair[F](opts.seed, s"delegator:$i")
      }
      collateralOwnerKeys <- (0 until opts.numOperators).toList.traverse { i =>
        deterministicKeyPair[F](opts.seed, s"collateral-owner:$i")
      }
      operators = operatorKeys.zipWithIndex.map {
        case (kp, _) =>
          L0GenesisOperator(
            peerId = PeerId.fromPublic(kp.getPublic).value.value,
            address = kp.getPublic.toAddress.value.value,
            vrfPublicKey = None,
            kesPublicKey = None
          )
      }
      // §1.2 Slice 3b: per-operator KES master key + registration cert. The KES seed is drawn
      // from a `SecureRandom` (NOT derived from the operator's long-term key) — forward security
      // requires that compromise of the long-term key does not leak the KES SK or any past KES
      // signature. The registration signature `Sign_ed25519(kesVk.value)` is what binds the master
      // VK to the operator identity for downstream verifiers.
      //
      // The encoded SK bytes (raw `SecretKeyCodec.encodeProductSk` output) are carried in
      // `GeneratedOutputs.kesSecretKeys` for the CLI dispatcher to persist via
      // `writeKesSecretKeys`. They do NOT land in the L0 genesis JSON — only the public VK +
      // registration signature do.
      kesMaterial <- (0 until opts.numOperators).toList.traverse { i =>
        val kp = operatorKeys(i)
        val peerId = PeerId.fromPublic(kp.getPublic)
        for {
          seed <- Async[F].delay {
            val s = new Array[Byte](32)
            new JSecureRandom().nextBytes(s)
            s
          }
          skVk <- OperationalKeyMaker.generateFreshKesKeyMaterial[F](seed)
          (skBytes, vk) = skVk
          _ <- Async[F].delay(java.util.Arrays.fill(seed, 0.toByte))
          // Sign the RAW vk bytes (NOT the hex string) with the operator's long-term Ed25519 key.
          // Receivers verify with `Signing.verifySignature(vk.value, longTermSig)` using the
          // operator's long-term pubkey (recovered from `L0GenesisOperator.peerId`).
          regSig <- Signing.signData[F](vk.value)(kp.getPrivate)
          registration = L0GenesisKesRegistration(
            peerId = peerId.value.value,
            kesVk = Hex.fromBytes(vk.value).value,
            kesVkStep = vk.step,
            longTermSig = Hex.fromBytes(regSig).value,
            // Genesis operators register with offset 0 — their KES tree's step 0 == global eta
            // period 0. Mid-life joiners (Slice 10 #179) supply a non-zero offset via the runtime
            // registration tx; they do not flow through this generator.
            offset = 0L
          )
          sk = OperatorKesSecretKey(operatorIndex = i, peerId = peerId, bytes = skBytes)
        } yield (registration, sk)
      }
      kesRegistrations = kesMaterial.map(_._1)
      kesSecretKeys = kesMaterial.map(_._2)
      stakeAmounts = allocateStakeAmounts(opts.stakeDistribution, opts.stakeBudgetDatum)
      delegatedStakes = delegatorKeys.zipWithIndex.flatMap {
        case (dKp, i) =>
          val operatorPeerId = PeerId.fromPublic(operatorKeys(i).getPublic)
          val delegatorAddr = dKp.getPublic.toAddress
          val amount = stakeAmounts.lift(i).getOrElse(0L)
          val tokenLockRef = syntheticTokenLockRef(opts.seed, "stake", i)
          buildStakeEvent(delegatorAddr, operatorPeerId, amount, tokenLockRef).toOption.map { evt =>
            L0GenesisDelegatedStake(
              event = evt,
              delegatorPrivateKeyHex = encodePrivateKey(dKp),
              createdAt = 0L,
              rewards = 0L
            )
          }
      }
      nodeCollaterals =
        if (opts.collateralPerOperator <= 0L) List.empty
        else
          collateralOwnerKeys.zipWithIndex.flatMap {
            case (cKp, i) =>
              val operatorPeerId = PeerId.fromPublic(operatorKeys(i).getPublic)
              val ownerAddr = cKp.getPublic.toAddress
              val tokenLockRef = syntheticTokenLockRef(opts.seed, "collateral", i)
              buildCollateralEvent(ownerAddr, operatorPeerId, opts.collateralPerOperator, tokenLockRef).toOption.map { evt =>
                L0GenesisNodeCollateral(
                  event = evt,
                  ownerPrivateKeyHex = encodePrivateKey(cKp),
                  createdAt = 0L
                )
              }
          }
      // Initial balances: explicit CSV entries (if any) + a per-operator allocation matching the
      // genesis-csv legacy convention (100_000_000_000_000 = 1e6 DAG * 1e8 datum) so wallet tests
      // still find money at the operator addresses.
      initialBalances <- loadOrSynthesizeBalances[F](opts.initialBalancesCsv, operators)
      meta = L0GenesisMeta(
        generatorVersion = s"tools-${io.constellationnetwork.BuildInfo.version}",
        generatedAt = DateTimeFormatter.ISO_INSTANT
          .withZone(ZoneOffset.UTC)
          .format(Instant.ofEpochSecond(0L)), // EPOCH-pinned for byte-determinism; --no-pin-time flag could override
        invocation = invocation,
        seed = opts.seed,
        expectedProperties = List(
          s"${opts.numOperators} operators with stake distribution: ${opts.stakeDistribution.mkString(",")}",
          s"Total stake budget: ${opts.stakeBudgetDatum} datum",
          s"Collateral per operator: ${opts.collateralPerOperator} datum",
          "Stake records signed at load time with synthetic delegator keys",
          "ECDSA signatures non-deterministic by RNG (verifiable but not byte-equal across loads)"
        )
      )
      l0 = L0GenesisData(
        _meta = meta,
        networkMagic = opts.networkMagic,
        activationOrdinal = 0L,
        startingEpochProgress = opts.startingEpochProgress,
        protocolParams = L0GenesisProtocolParams.default.copy(startingEpochProgress = opts.startingEpochProgress),
        operators = operators,
        delegatedStakes = delegatedStakes,
        nodeCollaterals = nodeCollaterals,
        initialBalances = initialBalances,
        kesRegistrations = Some(kesRegistrations)
      )
      cl1 = opts.initialBalancesCsv.map { _ =>
        Cl1GenesisData(
          // Tier-1: single-metagraph. We don't yet know the runtime metagraph address (it's set at
          // metagraph creation), so we leave a placeholder that the user must edit before use.
          metagraphId = "DAG0METAGRAPHPLACEHOLDER000000000000000000",
          activationOrdinal = 0L,
          balances = initialBalances
        )
      }
    } yield GeneratedOutputs(l0, cl1, kesSecretKeys)

  private def validateOpts[F[_]: Async](opts: GeneratorOpts): F[Unit] = {
    val errors = scala.collection.mutable.ListBuffer.empty[String]
    if (opts.numOperators <= 0) errors += s"--num-operators must be positive (got ${opts.numOperators})"
    if (opts.stakeDistribution.nonEmpty && opts.stakeDistribution.size != opts.numOperators) {
      errors += s"--stake-distribution length (${opts.stakeDistribution.size}) must match --num-operators (${opts.numOperators})"
    }
    val sum = opts.stakeDistribution.foldLeft(BigDecimal(0))(_ + _)
    if (opts.stakeDistribution.nonEmpty && (sum < BigDecimal("0.99") || sum > BigDecimal("1.01"))) {
      errors += s"--stake-distribution sum must be ≈ 1.0 (got $sum)"
    }
    if (opts.stakeBudgetDatum < 0L) errors += s"--stake-budget-datum must be non-negative (got ${opts.stakeBudgetDatum})"
    if (opts.collateralPerOperator < 0L) errors += s"--collateral-per-operator must be non-negative (got ${opts.collateralPerOperator})"
    Async[F].whenA(errors.nonEmpty)(
      Async[F].raiseError(new IllegalArgumentException(s"Invalid generator options: ${errors.mkString("; ")}"))
    )
  }

  /** Persist each per-operator KES SK blob to `<outputDir>/keys/operator-<N>/kes-sk.bin`, with the file mode set to `0600` (owner-only
    * read/write). The directory layout is the one a future disk-backed [[io.constellationnetwork.security.kes.SecureStore]] (Slice 4) will
    * mount: one file per key entry, file name = `keyName`, bytes = raw `SecretKeyCodec.encodeProductSk` output.
    *
    * POSIX permissions are best-effort; on file systems that do not support POSIX permissions (e.g. tmpfs on some CI sandboxes), the chmod
    * step is logged but does not fail the write.
    */
  def writeKesSecretKeys[F[_]: Async](outputDir: String, keys: List[OperatorKesSecretKey]): F[List[String]] =
    keys.traverse { sk =>
      Async[F].delay {
        val dir = JPaths.get(outputDir, "keys", s"operator-${sk.operatorIndex}")
        JFiles.createDirectories(dir)
        val skFile = dir.resolve("kes-sk.bin")
        JFiles.write(skFile, sk.bytes)
        // Restrict to owner-only. Skip silently on file systems without POSIX support — the
        // alternative (failing the write) breaks the cross-platform contract; the docs note this.
        scala.util.Try {
          val perms = PosixFilePermissions.fromString("rw-------")
          JFiles.setPosixFilePermissions(skFile, perms)
        }
        skFile.toAbsolutePath.toString
      }
    }

  private def loadOrSynthesizeBalances[F[_]: Async](
    csvPath: Option[String],
    operators: List[L0GenesisOperator]
  ): F[List[L0GenesisBalance]] =
    csvPath match {
      case Some(path) =>
        Async[F].blocking {
          val raw = scala.io.Source.fromFile(path)("UTF-8").getLines().toList
          raw.flatMap { line =>
            val trimmed = line.trim
            if (trimmed.isEmpty || trimmed.startsWith("#")) None
            else {
              val parts = trimmed.split(",", 2)
              if (parts.length != 2) None
              else {
                val addr = parts(0).trim
                val balOpt = scala.util.Try(parts(1).trim.toLong).toOption
                balOpt.flatMap { bal =>
                  refineV[DAGAddressRefined](addr).toOption.map(_ => L0GenesisBalance(addr, bal))
                }
              }
            }
          }
        }
      case None =>
        // No CSV provided — synthesize a per-operator stipend so the operator addresses have
        // balances. Default: 1e6 DAG * 1e8 datum = 100_000_000_000_000, matching the legacy
        // `docker-env-setup.sh` allocation.
        Async[F].pure(operators.map(op => L0GenesisBalance(op.address, 100000000000000L)))
    }
}
