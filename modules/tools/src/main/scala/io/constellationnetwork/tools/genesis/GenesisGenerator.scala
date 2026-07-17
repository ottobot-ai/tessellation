package io.constellationnetwork.tools.genesis

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.nio.file.{Files => JFiles, Path => JPath, Paths => JPaths, _}
import java.security.spec.ECGenParameterSpec
import java.security.{KeyPair, KeyPairGenerator => JKeyPairGenerator, SecureRandom => JSecureRandom}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.UUID

import cats.effect.kernel.Outcome
import cats.effect.syntax.all._
import cats.effect.{Async, Ref}
import cats.syntax.all._

import scala.jdk.CollectionConverters._

import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.OperationalKeyMaker
import io.constellationnetwork.security.key.ops._
import io.constellationnetwork.security.key.{ECDSA, secp256k}
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.vrf.VrfKeyDeriver

import eu.timepit.refined.refineV
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}

/** Greenfield current-hash genesis generator. A generated `l0-genesis.json` is a one-time canonical public artifact, not a reproducible
  * derivation from `--seed`: KES keys, economic owner keys, and ECDSA signatures use secure randomness. `--seed` controls only explicitly
  * test-only operator credentials and non-secret fixture allocation inputs.
  *
  * Stake/collateral records are emitted as fully signed public bundles. Their owner keys are generated from secure randomness and written
  * separately with owner-only permissions; no economic private key is derived from the public fixture seed or embedded in genesis JSON.
  */
object GenesisGenerator {

  /** Reuses the existing operator credential at `nodes/N/key.p12` when `keysFromDir.isDefined`. Public-seed-derived operator credentials
    * exist only behind the explicit test-only flag.
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
    operatorKeyAlias: String,
    operatorKeyPasswordEnv: Option[String],
    testOnlyDeterministicOperatorKeys: Boolean,
    networkMagic: String,
    startingEpochProgress: Long
  )

  case class GeneratedOutputs(
    l0Genesis: L0GenesisData,
    cl1Genesis: Option[Cl1GenesisData],
    // §1.2 Slice 3b: per-operator KES SK byte blobs the caller writes to per-operator key
    // directories. Held in-memory inside the generator; the CLI dispatcher (`Main.generateGenesis`)
    // persists them via `writeKesSecretKeys`. Empty when KES generation is disabled.
    kesSecretKeys: List[OperatorKesSecretKey] = List.empty,
    economicSecretKeys: List[EconomicSecretKey] = List.empty
  )

  /** Per-operator KES SK material. The CLI side writes `bytes` to `<output-dir>/keys/operator-<operatorIndex>/kes-sk.bin` with `chmod
    * 0600`.
    *
    * The byte blob is the same `SecretKeyCodec.encodeProductSk` output that the gl0 startup path will later consume from a disk-backed
    * `SecureStore` (Slice 4) to reopen the master key.
    */
  case class OperatorKesSecretKey(operatorIndex: Int, peerId: PeerId, bytes: Array[Byte])

  case class EconomicSecretKey(operatorIndex: Int, role: String, address: Address, bytes: Array[Byte])

  case class PersistedGenesisPaths(
    l0Genesis: String,
    cl1Genesis: Option[String],
    kesSecretKeys: List[String],
    economicSecretKeys: List[String]
  )

  val FirstLiveOrdinal: SnapshotOrdinal = SnapshotOrdinal.MinValue.next

  private val SecretDirectoryPermissions = PosixFilePermissions.fromString("rwx------")
  private val SecretFilePermissions = PosixFilePermissions.fromString("rw-------")
  private val PublicFilePermissions = PosixFilePermissions.fromString("rw-r--r--")
  private val UnsafeOutputPermissions =
    Set(
      PosixFilePermission.GROUP_WRITE,
      PosixFilePermission.OTHERS_WRITE
    )

  private def deterministicRng(seed: Long, salt: String): JSecureRandom = {
    val rng = JSecureRandom.getInstance("SHA1PRNG")
    // This deterministic stream is restricted to explicitly test-only operator-key generation.
    val saltBytes = s"tessellation-nakamoto-genesis|$seed|$salt".getBytes("UTF-8")
    rng.setSeed(saltBytes)
    rng
  }

  /** Test-only deterministic ECDSA operator-key generation. Economic owner keys always use the platform CSPRNG.
    */
  def deterministicKeyPair[F[_]: Async: SecurityProvider](seed: Long, salt: String): F[KeyPair] =
    Async[F].delay {
      val rng = deterministicRng(seed, salt)
      val ecSpec = new ECGenParameterSpec(secp256k)
      val kpg = JKeyPairGenerator.getInstance(ECDSA, SecurityProvider[F].provider)
      kpg.initialize(ecSpec, rng)
      kpg.generateKeyPair()
    }

  private def secureEconomicKeyPair[F[_]: Async: SecurityProvider]: F[KeyPair] =
    Async[F].delay {
      val ecSpec = new ECGenParameterSpec(secp256k)
      val kpg = JKeyPairGenerator.getInstance(ECDSA, SecurityProvider[F].provider)
      kpg.initialize(ecSpec, new JSecureRandom())
      kpg.generateKeyPair()
    }

  private def buildBackingTokenLock(source: Address, amount: Long): Either[String, TokenLock] =
    Either.cond(
      amount > 0L,
      TokenLock(
        source = source,
        amount = TokenLockAmount(PosLong.unsafeFrom(amount)),
        fee = TokenLockFee(NonNegLong(0L)),
        parent = TokenLockReference.empty,
        currencyId = None,
        unlockEpoch = None,
        replaceTokenLockRef = None
      ),
      s"Genesis backing token-lock amount must be positive, got $amount"
    )

  private def liftBuild[F[_]: Async, A](result: Either[String, A]): F[A] =
    result.leftMap(new IllegalArgumentException(_)).liftTo[F]

  /** Allocate per-operator stake amounts given a relative-weight list and a budget. Uses BigDecimal arithmetic so the per-operator amounts
    * are deterministic regardless of source order; the residual (due to integer truncation) goes to operator 0.
    */
  def allocateStakeAmounts(weights: List[BigDecimal], budget: Long): List[Long] = {
    require(budget >= 0L, s"Stake budget must be non-negative, got $budget")
    require(weights.forall(_ >= BigDecimal(0)), "Stake weights must be non-negative")

    val total = weights.foldLeft(BigDecimal(0))(_ + _)
    if (total <= BigDecimal(0)) List.fill(weights.size)(0L)
    else {
      val raw = weights.map(w => (w / total * BigDecimal(budget)).toBigInt)
      val residual = BigInt(budget) - raw.sum
      val allocated = if (raw.isEmpty) raw else (raw.head + residual) :: raw.tail
      require(allocated.forall(_.isValidLong), "Allocated stake is outside the protocol Long domain")
      allocated.map(_.toLong)
    }
  }

  /** Build a single `UpdateDelegatedStake.Create` event for a (delegator-address, operator-peerId) pair. The event carries the configured
    * amount, the hash of its exact signed backing lock, and an empty parent because this is the delegator's first stake.
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

  /** Top-level entry point. Produces `(l0Genesis, optional cl1Genesis)`. The cl1Genesis is synthesized when `initialBalancesCsv` is
    * supplied; otherwise None (the caller can still commit the l0-only fixture for stake-distribution regression tests).
    */
  def generate[F[_]: Async: HasherSelector: SecurityProvider](
    opts: GeneratorOpts,
    invocation: String
  ): F[GeneratedOutputs] =
    Ref.of[F, List[Array[Byte]]](Nil).flatMap { trackedSecrets =>
      val generated = HasherSelector[F].forOrdinal(FirstLiveOrdinal) { selectedHasher =>
        Async[F]
          .raiseError[Unit](
            new IllegalArgumentException(
              s"generate-genesis supports only the greenfield current JSON hash at first live ordinal ${FirstLiveOrdinal.value.value}; " +
                s"the configured selector returned ${selectedHasher.getLogic(FirstLiveOrdinal)}"
            )
          )
          .unlessA(selectedHasher.getLogic(FirstLiveOrdinal) == JsonHash) >> {
          implicit val hasher: Hasher[F] = selectedHasher
          generateAtActivationHasher(opts, invocation, bytes => trackedSecrets.update(bytes :: _))
        }
      }

      generated.guaranteeCase {
        case Outcome.Succeeded(_) => Async[F].unit
        case _                    => trackedSecrets.get.flatMap(zeroByteArrays[F])
      }
    }

  private def generateAtActivationHasher[F[_]: Async: Hasher: SecurityProvider](
    opts: GeneratorOpts,
    invocation: String,
    trackSecret: Array[Byte] => F[Unit]
  ): F[GeneratedOutputs] =
    for {
      _ <- validateOpts[F](opts)
      operatorKeys <- (0 until opts.numOperators).toList.traverse { i =>
        opts.keysFromDir match {
          case Some(dir) =>
            val keyPath = s"$dir/$i/key.p12"
            val passwordEnv = opts.operatorKeyPasswordEnv.getOrElse {
              throw new IllegalArgumentException("--operator-key-password-env is required with --keys-from")
            }
            for {
              password <- Async[F].delay(
                sys.env
                  .get(passwordEnv)
                  .filter(_.nonEmpty)
                  .getOrElse(throw new IllegalArgumentException(s"Operator keystore password environment variable is unset: $passwordEnv"))
                  .toCharArray
              )
              keyPair <- io.constellationnetwork.keytool.KeyStoreUtils
                .readKeyPairFromStore[F](
                  keyPath,
                  opts.operatorKeyAlias,
                  password,
                  password
                )
                .guarantee(Async[F].delay(java.util.Arrays.fill(password, '\u0000')))
            } yield keyPair
          case None =>
            if (opts.testOnlyDeterministicOperatorKeys) deterministicKeyPair[F](opts.seed, s"operator:$i")
            else
              Async[F].raiseError[KeyPair](
                new IllegalArgumentException(
                  "Production genesis generation requires --keys-from; public-seed-derived operator keys are test-only"
                )
              )
        }
      }
      delegatorKeys <- (0 until opts.numOperators).toList.traverse(_ => secureEconomicKeyPair[F])
      collateralOwnerKeys <-
        if (opts.collateralPerOperator > 0L)
          (0 until opts.numOperators).toList.traverse(_ => secureEconomicKeyPair[F])
        else List.empty[KeyPair].pure[F]
      // Per-operator atomic KES+VRF registration. The KES seed is drawn
      // from a `SecureRandom` (NOT derived from the operator's long-term key) — forward security
      // requires that compromise of the long-term key does not leak the KES SK or any past KES
      // signature. VRF generation uses the shared runtime derivation helper, but the signature still
      // binds the resulting VRF VK explicitly: carried/derived keys are never authority by themselves.
      //
      // The encoded SK bytes (raw `SecretKeyCodec.encodeProductSk` output) are carried in
      // `GeneratedOutputs.kesSecretKeys` for the CLI dispatcher to persist via
      // `writeKesSecretKeys`. They do NOT land in the L0 genesis JSON — only the public VK +
      // registration signature do.
      kesMaterial <- (0 until opts.numOperators).toList.traverse { i =>
        val kp = operatorKeys(i)
        val peerId = PeerId.fromPublic(kp.getPublic)
        val address = kp.getPublic.toAddress.value.value
        val (_, vrfVk) = VrfKeyDeriver.deriveVrfKeyPair(kp)
        for {
          seed <- Async[F].delay {
            val s = new Array[Byte](32)
            new JSecureRandom().nextBytes(s)
            s
          }
          skVk <- Async[F].uncancelable { _ =>
            OperationalKeyMaker
              .generateFreshKesKeyMaterial[F](seed)
              .guarantee(Async[F].delay(java.util.Arrays.fill(seed, 0.toByte)))
              .flatTap { case (skBytes, _) => trackSecret(skBytes) }
          }
          (skBytes, vk) = skVk
          preimage = L0GenesisOperator.signaturePreimage(
            opts.networkMagic,
            activationOrdinal = 0L,
            opts.startingEpochProgress,
            peerId.value.toBytes,
            address,
            vk.value,
            vk.step,
            kesPeriodOffset = 0L,
            vrfVk
          )
          regSig <- Signing.signData[F](preimage)(kp.getPrivate)
          operator = L0GenesisOperator(
            peerId = peerId.value.value,
            address = address,
            kesMasterVk = Hex.fromBytes(vk.value).value,
            kesMasterVkStep = vk.step,
            kesPeriodOffset = 0L,
            vrfVk = Hex.fromBytes(vrfVk).value,
            longTermSignature = Hex.fromBytes(regSig).value
          )
          sk = OperatorKesSecretKey(operatorIndex = i, peerId = peerId, bytes = skBytes)
        } yield (operator, sk)
      }
      operators = kesMaterial.map(_._1)
      kesSecretKeys = kesMaterial.map(_._2)
      stakeAmounts = allocateStakeAmounts(opts.stakeDistribution, opts.stakeBudgetDatum)
      delegatedStakes <- delegatorKeys.zipWithIndex.flatTraverse {
        case (dKp, i) =>
          val operatorPeerId = PeerId.fromPublic(operatorKeys(i).getPublic)
          val delegatorAddr = dKp.getPublic.toAddress
          val amount = stakeAmounts.lift(i).getOrElse(0L)
          if (amount <= 0L) List.empty[L0GenesisDelegatedStake].pure[F]
          else
            for {
              lock <- liftBuild[F, TokenLock](buildBackingTokenLock(delegatorAddr, amount))
              signedLock <- Signed.forAsyncHasher[F, TokenLock](lock, dKp)
              tokenLockRef <- TokenLockReference.of[F](signedLock)
              event <- liftBuild[F, UpdateDelegatedStake.Create](
                buildStakeEvent(delegatorAddr, operatorPeerId, amount, tokenLockRef.hash)
              )
              signedEvent <- Signed.forAsyncHasher[F, UpdateDelegatedStake.Create](event, dKp)
            } yield
              List(
                L0GenesisDelegatedStake(
                  signedEvent = signedEvent,
                  signedBackingTokenLock = signedLock,
                  createdAt = 0L,
                  rewards = 0L
                )
              )
      }
      nodeCollaterals <-
        if (opts.collateralPerOperator <= 0L) List.empty[L0GenesisNodeCollateral].pure[F]
        else
          collateralOwnerKeys.zipWithIndex.traverse {
            case (cKp, i) =>
              val operatorPeerId = PeerId.fromPublic(operatorKeys(i).getPublic)
              val ownerAddr = cKp.getPublic.toAddress
              for {
                lock <- liftBuild[F, TokenLock](buildBackingTokenLock(ownerAddr, opts.collateralPerOperator))
                signedLock <- Signed.forAsyncHasher[F, TokenLock](lock, cKp)
                tokenLockRef <- TokenLockReference.of[F](signedLock)
                event <- liftBuild[F, UpdateNodeCollateral.Create](
                  buildCollateralEvent(
                    ownerAddr,
                    operatorPeerId,
                    opts.collateralPerOperator,
                    tokenLockRef.hash
                  )
                )
                signedEvent <- Signed.forAsyncHasher[F, UpdateNodeCollateral.Create](event, cKp)
              } yield
                L0GenesisNodeCollateral(
                  signedEvent = signedEvent,
                  signedBackingTokenLock = signedLock,
                  createdAt = 0L
                )
          }
      economicKeyPairs =
        delegatorKeys.zipWithIndex.collect {
          case (keyPair, index) if stakeAmounts.lift(index).exists(_ > 0L) =>
            (keyPair, index, "delegator")
        } ++ collateralOwnerKeys.zipWithIndex.map {
          case (keyPair, index) =>
            (keyPair, index, "collateral-owner")
        }
      economicSecretKeys <- economicKeyPairs.traverse {
        case (keyPair, index, role) =>
          Async[F].uncancelable { _ =>
            Async[F]
              .delay(EconomicSecretKey(index, role, keyPair.getPublic.toAddress, keyPair.getPrivate.getEncoded))
              .flatTap(secret => trackSecret(secret.bytes))
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
          .format(Instant.ofEpochSecond(0L)),
        invocation = invocation,
        seed = opts.seed,
        expectedProperties = List(
          s"${opts.numOperators} operators with stake distribution: ${opts.stakeDistribution.mkString(",")}",
          s"Total stake budget: ${opts.stakeBudgetDatum} datum",
          s"Collateral per operator: ${opts.collateralPerOperator} datum",
          "Stake and collateral records carry exact signed native backing locks",
          "Economic owner private keys are absent from genesis JSON"
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
        initialBalances = initialBalances
      )
      _ <- l0.validatedGenesisAllocation
        .leftMap(message => new IllegalArgumentException(s"Generated L0 genesis allocation is invalid: $message"))
        .liftTo[F]
      cl1 = opts.initialBalancesCsv.map { _ =>
        Cl1GenesisData(
          // Tier-1: single-metagraph. We don't yet know the runtime metagraph address (it's set at
          // metagraph creation), so we leave a placeholder that the user must edit before use.
          metagraphId = "DAG0METAGRAPHPLACEHOLDER000000000000000000",
          activationOrdinal = 0L,
          balances = initialBalances
        )
      }
    } yield GeneratedOutputs(l0, cl1, kesSecretKeys, economicSecretKeys)

  private def validateOpts[F[_]: Async](opts: GeneratorOpts): F[Unit] = {
    val errors = scala.collection.mutable.ListBuffer.empty[String]
    if (opts.numOperators <= 0) errors += s"--num-operators must be positive (got ${opts.numOperators})"
    if (opts.stakeDistribution.nonEmpty && opts.stakeDistribution.size != opts.numOperators) {
      errors += s"--stake-distribution length (${opts.stakeDistribution.size}) must match --num-operators (${opts.numOperators})"
    }
    if (opts.stakeDistribution.exists(_ < BigDecimal(0)))
      errors += "--stake-distribution weights must be non-negative"
    val sum = opts.stakeDistribution.foldLeft(BigDecimal(0))(_ + _)
    if (opts.stakeDistribution.nonEmpty && (sum < BigDecimal("0.99") || sum > BigDecimal("1.01"))) {
      errors += s"--stake-distribution sum must be ≈ 1.0 (got $sum)"
    }
    if (opts.stakeBudgetDatum < 0L) errors += s"--stake-budget-datum must be non-negative (got ${opts.stakeBudgetDatum})"
    if (opts.collateralPerOperator < 0L) errors += s"--collateral-per-operator must be non-negative (got ${opts.collateralPerOperator})"
    if (opts.keysFromDir.isEmpty && !opts.testOnlyDeterministicOperatorKeys)
      errors += "--keys-from is required unless --test-only-deterministic-operator-keys is explicitly set"
    if (opts.keysFromDir.nonEmpty && opts.operatorKeyAlias.isEmpty)
      errors += "--operator-key-alias must be non-empty with --keys-from"
    if (opts.keysFromDir.nonEmpty && opts.operatorKeyPasswordEnv.forall(_.isEmpty))
      errors += "--operator-key-password-env is required with --keys-from"
    if (opts.operatorKeyPasswordEnv.exists(!_.matches("[A-Za-z_][A-Za-z0-9_]*")))
      errors += "--operator-key-password-env must be a valid environment-variable name"
    Async[F].whenA(errors.nonEmpty)(
      Async[F].raiseError(new IllegalArgumentException(s"Invalid generator options: ${errors.mkString("; ")}"))
    )
  }

  private def requireSafeOutputDirectory(outputDir: String): JPath = {
    val root = JPaths.get(outputDir).toAbsolutePath.normalize()
    val filesystemRoot = Option(root.getRoot).getOrElse(throw new IllegalArgumentException(s"Output path is not absolute: $root"))
    root.iterator().asScala.foldLeft(filesystemRoot) { (current, component) =>
      val next = current.resolve(component)
      if (JFiles.isSymbolicLink(next))
        throw new IllegalArgumentException(s"Genesis output path contains a symlink component: $next")
      next
    }
    if (!JFiles.exists(root, LinkOption.NOFOLLOW_LINKS))
      throw new IllegalArgumentException(s"Genesis output directory does not exist: $root")
    if (JFiles.isSymbolicLink(root) || !JFiles.isDirectory(root, LinkOption.NOFOLLOW_LINKS))
      throw new IllegalArgumentException(s"Genesis output path must be a real directory, not a symlink: $root")
    if (!JFiles.getFileStore(root).supportsFileAttributeView("posix"))
      throw new IllegalArgumentException(s"Genesis secret custody requires a POSIX file system: $root")

    val permissions = JFiles.getPosixFilePermissions(root, LinkOption.NOFOLLOW_LINKS).asScala
    if (permissions.exists(UnsafeOutputPermissions))
      throw new IllegalArgumentException(s"Genesis output directory is group/world writable: $root")
    root
  }

  private def fsyncDirectory(path: JPath): Unit = {
    val channel = FileChannel.open(path, StandardOpenOption.READ)
    try channel.force(true)
    finally channel.close()
  }

  private def ensureSecretDirectory(path: JPath): Unit =
    if (JFiles.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      if (JFiles.isSymbolicLink(path) || !JFiles.isDirectory(path, LinkOption.NOFOLLOW_LINKS))
        throw new IllegalArgumentException(s"Secret path is not a real directory: $path")
      val actual = JFiles.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS)
      if (actual != SecretDirectoryPermissions)
        throw new IllegalArgumentException(
          s"Existing secret directory must have mode 0700, found ${PosixFilePermissions.toString(actual)}: $path"
        )
    } else {
      JFiles.createDirectory(path, PosixFilePermissions.asFileAttribute(SecretDirectoryPermissions))
      val actual = JFiles.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS)
      if (actual != SecretDirectoryPermissions)
        throw new IllegalStateException(s"Secret directory was not created with mode 0700: $path")
      fsyncDirectory(path)
      fsyncDirectory(path.getParent)
    }

  private def writeFully(channel: FileChannel, bytes: Array[Byte]): Unit = {
    val buffer = ByteBuffer.wrap(bytes)
    while (buffer.hasRemaining) channel.write(buffer)
    channel.force(true)
  }

  private def writeSecretFile(path: JPath, bytes: Array[Byte]): String = {
    if (bytes.isEmpty) throw new IllegalArgumentException(s"Refusing to persist an empty secret: $path")
    val options =
      Set[OpenOption](StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS).asJava
    val channel = FileChannel.open(path, options, PosixFilePermissions.asFileAttribute(SecretFilePermissions))
    try writeFully(channel, bytes)
    finally channel.close()

    val actual = JFiles.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS)
    if (JFiles.isSymbolicLink(path) || actual != SecretFilePermissions)
      throw new IllegalStateException(s"Secret file was not created as a non-symlink with mode 0600: $path")
    fsyncDirectory(path.getParent)
    path.toAbsolutePath.toString
  }

  private def zeroByteArrays[F[_]: Async](arrays: List[Array[Byte]]): F[Unit] =
    Async[F].delay(arrays.foreach(bytes => java.util.Arrays.fill(bytes, 0.toByte)))

  private def zeroSecrets(kes: List[OperatorKesSecretKey], economic: List[EconomicSecretKey]): Unit = {
    kes.foreach(secret => java.util.Arrays.fill(secret.bytes, 0.toByte))
    economic.foreach(secret => java.util.Arrays.fill(secret.bytes, 0.toByte))
  }

  def zeroGeneratedSecrets[F[_]: Async](outputs: GeneratedOutputs): F[Unit] =
    Async[F].delay(zeroSecrets(outputs.kesSecretKeys, outputs.economicSecretKeys))

  private def writeAllSecretKeysBlocking(
    outputDir: String,
    kes: List[OperatorKesSecretKey],
    economic: List[EconomicSecretKey]
  ): (List[String], List[String]) = {
    val root = requireSafeOutputDirectory(outputDir)
    val keysRoot = root.resolve("keys")
    ensureSecretDirectory(keysRoot)

    val kesPaths = kes.map { secret =>
      if (secret.operatorIndex < 0) throw new IllegalArgumentException("KES operator index must be non-negative")
      val operatorDir = keysRoot.resolve(s"operator-${secret.operatorIndex}")
      ensureSecretDirectory(operatorDir)
      writeSecretFile(operatorDir.resolve("kes-sk.bin"), secret.bytes)
    }

    val economicRoot = keysRoot.resolve("economic")
    if (economic.nonEmpty) ensureSecretDirectory(economicRoot)
    val economicPaths = economic.map { secret =>
      if (secret.operatorIndex < 0) throw new IllegalArgumentException("Economic-key operator index must be non-negative")
      if (!secret.role.matches("[a-z][a-z0-9-]*"))
        throw new IllegalArgumentException(s"Unsafe economic-key role: ${secret.role}")
      val operatorDir = economicRoot.resolve(s"operator-${secret.operatorIndex}")
      ensureSecretDirectory(operatorDir)
      writeSecretFile(operatorDir.resolve(s"${secret.role}.pkcs8"), secret.bytes)
    }

    (kesPaths, economicPaths)
  }

  /** Writes every generated secret before any public genesis anchor can be published. POSIX support is mandatory: secret directories are
    * created as 0700, files are created once as 0600, and pre-existing files or unsafe/symlink directories fail closed. All caller-owned
    * secret byte arrays are zeroed whether creation succeeds, fails, or is cancelled.
    */
  def writeAllSecretKeys[F[_]: Async](
    outputDir: String,
    kes: List[OperatorKesSecretKey],
    economic: List[EconomicSecretKey]
  ): F[(List[String], List[String])] =
    Async[F]
      .blocking(writeAllSecretKeysBlocking(outputDir, kes, economic))
      .guarantee(Async[F].delay(zeroSecrets(kes, economic)))

  def writeKesSecretKeys[F[_]: Async](outputDir: String, keys: List[OperatorKesSecretKey]): F[List[String]] =
    writeAllSecretKeys(outputDir, keys, Nil).map(_._1)

  def writeEconomicSecretKeys[F[_]: Async](outputDir: String, keys: List[EconomicSecretKey]): F[List[String]] =
    writeAllSecretKeys(outputDir, Nil, keys).map(_._2)

  private def writePublicFileAtomically(outputRoot: JPath, fileName: String, contents: String): String = {
    if (!Set("l0-genesis.json", "cl1-genesis.json")(fileName))
      throw new IllegalArgumentException(s"Unsupported public genesis artifact name: $fileName")

    val target = outputRoot.resolve(fileName)
    if (JFiles.exists(target, LinkOption.NOFOLLOW_LINKS))
      throw new IllegalArgumentException(s"Refusing to replace existing public genesis artifact: $target")

    val temporary = outputRoot.resolve(s".$fileName.${UUID.randomUUID()}.tmp")
    val bytes = contents.getBytes(StandardCharsets.UTF_8)
    try {
      val options =
        Set[OpenOption](StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS).asJava
      val channel = FileChannel.open(temporary, options, PosixFilePermissions.asFileAttribute(PublicFilePermissions))
      try writeFully(channel, bytes)
      finally channel.close()
      JFiles.move(temporary, target, StandardCopyOption.ATOMIC_MOVE)
      fsyncDirectory(outputRoot)
      target.toAbsolutePath.toString
    } finally {
      JFiles.deleteIfExists(temporary)
      java.util.Arrays.fill(bytes, 0.toByte)
    }
  }

  /** Persists generated artifacts in failure-safe order: all secrets, optional CL1 public data, then the canonical L0 anchor as the final
    * atomic publication. Failure before the last step can leave owner-only orphan secrets for recovery, but never a public L0 anchor whose
    * corresponding secrets were not durably created.
    */
  def persistGeneratedOutputs[F[_]: Async](
    outputDir: String,
    outputs: GeneratedOutputs,
    l0Json: String,
    cl1Json: Option[String]
  ): F[PersistedGenesisPaths] =
    for {
      secretPaths <- writeAllSecretKeys(outputDir, outputs.kesSecretKeys, outputs.economicSecretKeys)
      outputRoot <- Async[F].blocking(requireSafeOutputDirectory(outputDir))
      cl1Path <- cl1Json.traverse(json => Async[F].blocking(writePublicFileAtomically(outputRoot, "cl1-genesis.json", json)))
      l0Path <- Async[F].blocking(writePublicFileAtomically(outputRoot, "l0-genesis.json", l0Json))
    } yield PersistedGenesisPaths(l0Path, cl1Path, secretPaths._1, secretPaths._2)

  private def loadOrSynthesizeBalances[F[_]: Async](
    csvPath: Option[String],
    operators: List[L0GenesisOperator]
  ): F[List[L0GenesisBalance]] =
    csvPath match {
      case Some(path) =>
        Async[F].blocking {
          val source = scala.io.Source.fromFile(path)("UTF-8")
          try
            source
              .getLines()
              .zipWithIndex
              .toList
              .traverse {
                case (line, zeroBasedIndex) =>
                  val lineNumber = zeroBasedIndex + 1
                  val trimmed = line.trim
                  if (trimmed.isEmpty || trimmed.startsWith("#"))
                    Right(none[L0GenesisBalance])
                  else {
                    val parts = trimmed.split(",", -1).toList.map(_.trim)
                    for {
                      pair <- Either.cond(
                        parts.size === 2,
                        parts,
                        s"Genesis balance CSV line $lineNumber must contain exactly address,balance"
                      )
                      address = pair.head
                      balanceText = pair(1)
                      _ <- refineV[DAGAddressRefined](address)
                        .leftMap(error => s"Genesis balance CSV line $lineNumber has an invalid address: $error")
                      balance <- scala.util
                        .Try(balanceText.toLong)
                        .toEither
                        .leftMap(_ => s"Genesis balance CSV line $lineNumber has an invalid Long balance: $balanceText")
                      _ <- Either.cond(
                        balance >= 0L,
                        (),
                        s"Genesis balance CSV line $lineNumber has a negative balance: $balance"
                      )
                    } yield L0GenesisBalance(address, balance).some
                  }
              }
              .leftMap(message => new IllegalArgumentException(message))
              .fold(throw _, _.flatten)
          finally source.close()
        }
      case None =>
        // No CSV provided — synthesize a per-operator stipend so the operator addresses have
        // balances. Default: 1e6 DAG * 1e8 datum = 100_000_000_000_000, matching the legacy
        // `docker-env-setup.sh` allocation.
        Async[F].pure(operators.map(op => L0GenesisBalance(op.address, 100000000000000L)))
    }
}
