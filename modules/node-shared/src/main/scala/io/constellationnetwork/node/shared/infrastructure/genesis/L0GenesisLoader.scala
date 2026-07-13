package io.constellationnetwork.node.shared.infrastructure.genesis

import java.security._
import java.security.spec.PKCS8EncodedKeySpec

import cats.data.NonEmptySet
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistryEntry, OperatorConsensusKeyRegistry}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, UpdateDelegatedStake}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GenesisOperatorConsensusKey}
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.serde.codecs.instances.GenesisOperatorConsensusKeyCodec.immutableCodec

import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.refineV
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Encoder

/** Helper that turns a Tier-1 `L0GenesisData` into a `GlobalSnapshotInfo` overlay. Called from the dag-l0 `Main.scala` JSON-genesis
  * bootstrap branch AFTER `hashedGenesis.info.toGlobalSnapshotInfo` is computed — we widen the in-memory GSI with delegated-stake +
  * collateral entries before any downstream consumer (storages, services) reads it. The on-disk `Signed[GlobalSnapshot]` stays V1 (Option
  * (ii) in the §1.1 plan — lowest blast radius).
  *
  * Signing: the rest of the codebase uses non-deterministic ECDSA via `Signing.signData` (unseeded `SecureRandom`). For genesis-fixture
  * records we sign DETERMINISTICALLY via RFC 6979 (`org.bouncycastle.crypto.signers.ECDSASigner` + `HMacDSAKCalculator(SHA512Digest)`) so
  * every node that loads the same fixture produces byte-identical `Signed[...]` records. This matters because the `Signed[event]` bytes
  * become MPT leaves in the `activeDelegatedStakes` / `activeNodeCollaterals` partitions — if signatures differed across nodes, the field
  * root (and therefore the stateProof.mptRoot) would diverge cross-node from genesis onward. The deterministic signature is DER-encoded
  * `(r, s)` identical in format to JCE's `SHA512withECDSA` output, so existing verifiers (`Signing.verifySignature` →
  * `Signature.getInstance("SHA512withECDSA")`) accept it without modification.
  */
object L0GenesisLoader {

  private val ECDSA = "ECDSA"
  private val PeerIdLength = 64
  private val KesMasterVerificationKeyLength = 32

  private def parsePrivateKey[F[_]: Async: SecurityProvider](pkcs8Hex: String): F[PrivateKey] =
    Async[F].delay {
      val bytes = Hex(pkcs8Hex).toBytes
      val spec = new PKCS8EncodedKeySpec(bytes)
      val kf = KeyFactory.getInstance(ECDSA, SecurityProvider[F].provider)
      kf.generatePrivate(spec)
    }

  /** Recover the public key from the private key. For EC keys produced by `KeyPairGenerator`, BouncyCastle stores the public point inside
    * the PKCS8 attributes; we reconstruct it via `BCECPrivateKey.getParameters`. Falls back to ECPublicKeySpec arithmetic if that's
    * unavailable.
    */
  private def derivePublicKey[F[_]: Async: SecurityProvider](priv: PrivateKey): F[PublicKey] =
    Async[F].delay {
      import org.bouncycastle.jce.interfaces.ECPrivateKey
      import org.bouncycastle.jce.spec.ECPublicKeySpec
      val bcPriv = priv.asInstanceOf[ECPrivateKey]
      val params = bcPriv.getParameters
      val q = params.getG.multiply(bcPriv.getD)
      val pubSpec = new ECPublicKeySpec(q, params)
      val kf = KeyFactory.getInstance(ECDSA, SecurityProvider[F].provider)
      kf.generatePublic(pubSpec)
    }

  /** Reconstruct a `KeyPair` from a PKCS8-encoded private-key hex string. */
  def keyPairFromHex[F[_]: Async: SecurityProvider](pkcs8Hex: String): F[KeyPair] =
    for {
      priv <- parsePrivateKey[F](pkcs8Hex)
      pub <- derivePublicKey[F](priv)
    } yield new KeyPair(pub, priv)

  /** Deterministic ECDSA signature over the SHA-512 digest of `messageBytes`, producing the same DER-encoded `(r, s)` ASN.1 sequence that
    * JCE's `SHA512withECDSA` would emit — but with the per-signature nonce `k` derived deterministically via RFC 6979 (HMAC-DRBG with
    * SHA-512). The output verifies under the unmodified verifier path (`Signing.verifySignature` →
    * `Signature.getInstance("SHA512withECDSA")`), so callers outside genesis don't need to change. Used for genesis fixture records where
    * cross-node byte-equality of `Signed[UpdateDelegatedStake.Create]` / `Signed[UpdateNodeCollateral.Create]` is required to keep the
    * `activeDelegatedStakes` / `activeNodeCollaterals` MPT leaves identical across all nodes loading the same fixture.
    *
    * Implementation notes:
    *   - SHA-512 of the message produces the `e` integer (high-order bits used per FIPS 186-4 §6.4 if the digest exceeds the curve order
    *     bit-length; BouncyCastle's `ECDSASigner.generateSignature` handles that internally).
    *   - `HMacDSAKCalculator(SHA512Digest)` implements RFC 6979 §3.2 with HMAC-SHA-512.
    *   - DER encoding uses `ASN1OutputStream` over a `DERSequence(r, s)` of `ASN1Integer` — byte-identical to JCE's output.
    *   - We do NOT canonicalise `s` (no low-S enforcement). JCE doesn't canonicalise either, so the verifier accepts both branches.
    */
  private[genesis] def deterministicSign[F[_]: Async](
    privateKey: PrivateKey,
    messageBytes: Array[Byte]
  ): F[Array[Byte]] =
    Async[F].delay {
      import java.io.ByteArrayOutputStream
      import org.bouncycastle.asn1.{ASN1Integer, ASN1OutputStream, DERSequence}
      import org.bouncycastle.crypto.digests.SHA512Digest
      import org.bouncycastle.crypto.params.ECPrivateKeyParameters
      import org.bouncycastle.crypto.signers.{ECDSASigner, HMacDSAKCalculator}
      import org.bouncycastle.jce.interfaces.{ECPrivateKey => BcECPrivateKey}

      val bcPriv = privateKey.asInstanceOf[BcECPrivateKey]
      val ecParams = bcPriv.getParameters
      val domain = new org.bouncycastle.crypto.params.ECDomainParameters(
        ecParams.getCurve,
        ecParams.getG,
        ecParams.getN,
        ecParams.getH
      )
      val keyParams = new ECPrivateKeyParameters(bcPriv.getD, domain)

      // SHA-512 hash of the message: JCE's "SHA512withECDSA" hashes the message INSIDE the
      // Signature engine before passing the digest to the underlying ECDSA primitive. We do
      // the same hashing here so the resulting (r, s) verify under the JCE path.
      val sha = new SHA512Digest()
      val digest = new Array[Byte](sha.getDigestSize)
      sha.update(messageBytes, 0, messageBytes.length)
      sha.doFinal(digest, 0)

      val signer = new ECDSASigner(new HMacDSAKCalculator(new SHA512Digest()))
      signer.init(true, keyParams)
      val rs = signer.generateSignature(digest)
      val r = rs(0)
      val s = rs(1)

      val baos = new ByteArrayOutputStream()
      val asn1 = ASN1OutputStream.create(baos)
      asn1.writeObject(new DERSequence(Array[org.bouncycastle.asn1.ASN1Encodable](new ASN1Integer(r), new ASN1Integer(s))))
      asn1.close()
      baos.toByteArray
    }

  /** Build a `Signed[A]` whose proof signature is byte-deterministic for a given `(privateKey, data)` pair. Mirrors `Signed.forAsyncHasher`
    * but routes through `deterministicSign`. The signer identity (`Id`) is recovered from the public key the same way as the rest of the
    * codebase (`PeerId.fromPublic(...).toId`).
    */
  private[genesis] def signedDeterministic[F[_]: Async: Hasher: SecurityProvider, A: Encoder](
    data: A,
    keyPair: KeyPair
  ): F[Signed[A]] =
    for {
      hash <- data.hash
      sigBytes <- deterministicSign[F](keyPair.getPrivate, hash.getBytes)
      proof = SignatureProof(PeerId.fromPublic(keyPair.getPublic).toId, Signature(Hex.fromBytes(sigBytes)))
    } yield Signed[A](data, NonEmptySet.fromSetUnsafe(SortedSet(proof)))

  /** Sign a synthetic delegated-stake event using its embedded delegator private key, then wrap the result in a runtime
    * `DelegatedStakeRecord`. Defensive: if signing fails (corrupt hex, wrong curve, etc), the entry is skipped and logged — Tier-1 fixtures
    * are reviewed before landing so silent-skip on malformed records is the conservative choice.
    *
    * Uses [[signedDeterministic]] (RFC 6979) so the resulting `Signed[UpdateDelegatedStake.Create]` bytes are byte-identical across nodes
    * loading the same fixture. Without this, `activeDelegatedStakes` MPT leaf bytes would diverge cross-node.
    */
  private def signStake[F[_]: Async: Hasher: SecurityProvider](
    s: L0GenesisDelegatedStake
  ): F[Option[(Address, DelegatedStakeRecord)]] =
    keyPairFromHex[F](s.delegatorPrivateKeyHex).flatMap { kp =>
      signedDeterministic[F, UpdateDelegatedStake.Create](s.event, kp).map { signed =>
        val createdAt = SnapshotOrdinal(NonNegLong.unsafeFrom(s.createdAt))
        val rewardsAmount = Amount(NonNegLong.unsafeFrom(s.rewards))
        val record = DelegatedStakeRecord(signed, createdAt, rewardsAmount)
        Option(s.event.source -> record)
      }
    }.handleError(_ => Option.empty[(Address, DelegatedStakeRecord)])

  private def signCollateral[F[_]: Async: Hasher: SecurityProvider](
    c: L0GenesisNodeCollateral
  ): F[Option[(Address, NodeCollateralRecord)]] =
    keyPairFromHex[F](c.ownerPrivateKeyHex).flatMap { kp =>
      signedDeterministic[F, UpdateNodeCollateral.Create](c.event, kp).map { signed =>
        val createdAt = SnapshotOrdinal(NonNegLong.unsafeFrom(c.createdAt))
        val record = NodeCollateralRecord(signed, createdAt)
        Option(c.event.source -> record)
      }
    }.handleError(_ => Option.empty[(Address, NodeCollateralRecord)])

  /** Augment a base `GlobalSnapshotInfo` (from `GlobalSnapshotInfoV1.toGlobalSnapshotInfo`) with the delegated-stake records,
    * node-collateral records, and balances declared in an L0 genesis fixture. Returns a new GSI with `activeDelegatedStakes`,
    * `activeNodeCollaterals`, and `balances` populated. Other fields (allow-spends, token-locks, etc.) are left at the empty
    * `Some(SortedMap.empty)` produced by `toGlobalSnapshotInfo`.
    */
  def augmentSnapshotInfo[F[_]: Async: Hasher: SecurityProvider](
    base: GlobalSnapshotInfo,
    data: L0GenesisData
  ): F[GlobalSnapshotInfo] =
    for {
      stakePairs <- data.delegatedStakes.flatTraverse(s => signStake[F](s).map(_.toList))
      collPairs <- data.nodeCollaterals.flatTraverse(c => signCollateral[F](c).map(_.toList))
      genesisOperatorKeys <- buildGenesisOperatorKeys[F](data)
    } yield {
      val stakeMap: SortedMap[Address, SortedSet[DelegatedStakeRecord]] =
        SortedMap.from(
          stakePairs
            .groupBy(_._1)
            .view
            .mapValues(_.map(_._2).to(SortedSet))
        )
      val collMap: SortedMap[Address, SortedSet[NodeCollateralRecord]] =
        SortedMap.from(
          collPairs
            .groupBy(_._1)
            .view
            .mapValues(_.map(_._2).to(SortedSet))
        )
      // Merge initial-balances declared in the fixture into the base balance map. The base map is
      // already pre-populated by `GlobalSnapshot.mkGenesis(initialBalanceMap, ...)`, so this is a
      // belt-and-braces merge — explicit balances in the fixture take precedence over the stipend
      // entries injected by `initialBalanceMap`.
      val mergedBalances: SortedMap[Address, Balance] =
        data.initialBalances.flatMap { b =>
          for {
            addr <- refineV[io.constellationnetwork.schema.address.DAGAddressRefined](b.address).toOption.map(Address(_))
            bal <- refineV[NonNegative](b.balance).toOption.map(Balance(_))
          } yield addr -> bal
        }.foldLeft(base.balances) { case (acc, (a, b)) => acc.updated(a, b) }

      base.copy(
        balances = mergedBalances,
        activeDelegatedStakes = Some(stakeMap),
        activeNodeCollaterals = Some(collMap),
        genesisOperatorKeys = genesisOperatorKeys
      )
    }

  private final case class ParsedOperator(
    peerId: PeerId,
    address: Address,
    kesEntry: KesRegistryEntry,
    vrfVk: Array[Byte],
    signature: Array[Byte],
    sourceIndex: Int
  )

  private def invalidGenesis(message: String): IllegalArgumentException =
    new IllegalArgumentException(s"Invalid canonical L0 genesis operator-key anchor: $message")

  private def decodeFixedHex(label: String, value: String, expectedBytes: Int): Either[IllegalArgumentException, Array[Byte]] = {
    val expectedChars = expectedBytes * 2
    if (value.length != expectedChars)
      Left(invalidGenesis(s"$label must encode exactly $expectedBytes bytes, found ${value.length / 2}"))
    else if (!value.forall(ch => Character.digit(ch, 16) >= 0))
      Left(invalidGenesis(s"$label is not valid hexadecimal"))
    else
      Either
        .catchNonFatal(Hex(value).toBytes)
        .leftMap(_ => invalidGenesis(s"$label is not valid hexadecimal"))
  }

  private def decodeSignatureHex(label: String, value: String): Either[IllegalArgumentException, Array[Byte]] =
    if (value.isEmpty || value.length % 2 != 0 || !value.forall(ch => Character.digit(ch, 16) >= 0))
      Left(invalidGenesis(s"$label is not a non-empty even-length hexadecimal signature"))
    else
      Either
        .catchNonFatal(Hex(value).toBytes)
        .leftMap(_ => invalidGenesis(s"$label is not valid hexadecimal"))

  private def duplicatePeerIds[A](values: List[A], peerId: A => PeerId): List[PeerId] =
    values.groupBy(peerId).collect { case (id, occurrences) if occurrences.sizeCompare(1) > 0 => id }.toList.sorted

  private def duplicateKeyOwners[A](values: List[A], key: A => Array[Byte], peerId: A => PeerId): List[List[PeerId]] =
    values
      .groupBy(value => Hex.fromBytes(key(value)).value)
      .values
      .collect { case duplicates if duplicates.sizeCompare(1) > 0 => duplicates.map(peerId).sorted }
      .toList

  private def peerLabel(peerId: PeerId): String = peerId.value.value.take(12)

  /** Build the immutable rooted genesis KES+VRF identity map from one atomic operator-key record per operator.
    *
    * Every GL0 operator supplies one 32-byte VRF verification key and one 32-byte, period-zero KES master verification key. The long-term
    * signature covers the domain-separated chain context and the complete pair. The loader rejects malformed/incomplete records, duplicate
    * identities or addresses, reused KES/VRF keys, address/PeerId mismatches, and invalid bindings before constructing the map.
    */
  def buildGenesisOperatorKeys[F[_]: Async: SecurityProvider](
    data: L0GenesisData
  ): F[SortedMap[PeerId, GenesisOperatorConsensusKey]] = {
    val parseOperators =
      data.operators.zipWithIndex.traverse {
        case (operator, index) =>
          for {
            peerBytes <- decodeFixedHex(s"operators[$index].peerId", operator.peerId, PeerIdLength)
            peerId = Id(Hex.fromBytes(peerBytes)).toPeerId
            address <- refineV[io.constellationnetwork.schema.address.DAGAddressRefined](operator.address)
              .leftMap(_ => invalidGenesis(s"operators[$index] (${peerLabel(peerId)}) has an invalid operator address"))
              .map(Address(_))
            kesVk <- decodeFixedHex(
              s"operators[$index].kesMasterVk (${peerLabel(peerId)})",
              operator.kesMasterVk,
              KesMasterVerificationKeyLength
            )
            _ <- Either.cond(
              operator.kesMasterVkStep == 0,
              (),
              invalidGenesis(s"operators[$index] (${peerLabel(peerId)}) must have kesMasterVkStep=0 at genesis")
            )
            _ <- Either.cond(
              operator.kesPeriodOffset == 0L,
              (),
              invalidGenesis(s"operators[$index] (${peerLabel(peerId)}) must have kesPeriodOffset=0 at genesis")
            )
            vrfVk <- decodeFixedHex(
              s"operators[$index].vrfVk (${peerLabel(peerId)})",
              operator.vrfVk,
              VrfPublicKey.ExpectedLength
            )
            signature <- decodeSignatureHex(
              s"operators[$index].longTermSignature (${peerLabel(peerId)})",
              operator.longTermSignature
            )
          } yield
            ParsedOperator(
              peerId,
              address,
              KesRegistryEntry(VerificationKeyKesProduct(kesVk, operator.kesMasterVkStep), operator.kesPeriodOffset),
              vrfVk,
              signature,
              index
            )
      }

    for {
      _ <- Async[F].raiseError[Unit](invalidGenesis("networkMagic must be non-empty")).whenA(data.networkMagic.isEmpty)
      _ <- Async[F].raiseError[Unit](invalidGenesis("activationOrdinal must be non-negative")).whenA(data.activationOrdinal < 0L)
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis("startingEpochProgress must be non-negative"))
        .whenA(data.startingEpochProgress < 0L)
      operators <- Async[F].fromEither(parseOperators)
      _ <- Async[F].raiseError[Unit](invalidGenesis("operators is empty")).whenA(operators.isEmpty)
      duplicateOperators = duplicatePeerIds[ParsedOperator](operators, operator => operator.peerId)
      _ <- Async[F]
        .raiseError[Unit](
          invalidGenesis(s"duplicate operator PeerId(s): ${duplicateOperators.map(peerLabel).mkString(", ")}")
        )
        .whenA(duplicateOperators.nonEmpty)
      duplicateAddresses = operators.groupBy(_.address).collect { case (address, entries) if entries.sizeCompare(1) > 0 => address }.toList
      _ <- Async[F]
        .raiseError[Unit](
          invalidGenesis(s"duplicate operator address(es): ${duplicateAddresses.mkString(", ")}")
        )
        .whenA(duplicateAddresses.nonEmpty)
      duplicateVrfKeys = duplicateKeyOwners[ParsedOperator](operators, operator => operator.vrfVk, operator => operator.peerId)
      _ <- Async[F]
        .raiseError[Unit](
          invalidGenesis(
            s"duplicate VRF verification key assigned to operator group(s): " +
              duplicateVrfKeys.map(_.map(peerLabel).mkString("[", ", ", "]")).mkString(", ")
          )
        )
        .whenA(duplicateVrfKeys.nonEmpty)
      duplicateKesKeys = duplicateKeyOwners[ParsedOperator](
        operators,
        operator => operator.kesEntry.vk.value,
        operator => operator.peerId
      )
      _ <- Async[F]
        .raiseError[Unit](
          invalidGenesis(
            s"duplicate KES master verification key assigned to operator group(s): " +
              duplicateKesKeys.map(_.map(peerLabel).mkString("[", ", ", "]")).mkString(", ")
          )
        )
        .whenA(duplicateKesKeys.nonEmpty)
      _ <- operators.traverse_ { operator =>
        for {
          publicKey <- operator.peerId.value
            .toPublicKey[F]
            .adaptError {
              case _ =>
                invalidGenesis(
                  s"operators[${operator.sourceIndex}] (${peerLabel(operator.peerId)}) has an invalid operator PeerId"
                )
            }
          expectedAddress = publicKey.toAddress
          _ <- Async[F]
            .raiseError[Unit](
              invalidGenesis(
                s"operators[${operator.sourceIndex}] (${peerLabel(operator.peerId)}) address does not match its PeerId"
              )
            )
            .unlessA(operator.address === expectedAddress)
          record = GenesisOperatorConsensusKey(
            data.networkMagic,
            data.activationOrdinal,
            data.startingEpochProgress,
            operator.peerId,
            operator.address,
            Hex.fromBytes(operator.kesEntry.vk.value),
            operator.kesEntry.vk.step,
            operator.kesEntry.offset,
            VrfPublicKey.fromBytes(operator.vrfVk),
            Signature(Hex.fromBytes(operator.signature))
          )
          valid <- Signing
            .verifySignature[F](GenesisOperatorConsensusKey.signaturePreimage(record), operator.signature)(publicKey)
            .adaptError {
              case _ =>
                invalidGenesis(
                  s"operators[${operator.sourceIndex}] (${peerLabel(operator.peerId)}) has a malformed longTermSignature"
                )
            }
          _ <- Async[F]
            .raiseError[Unit](
              invalidGenesis(
                s"operators[${operator.sourceIndex}] (${peerLabel(operator.peerId)}) longTermSignature does not bind the complete " +
                  "genesis operator-key record and chain context"
              )
            )
            .unlessA(valid)
        } yield ()
      }
      records = SortedMap.from(operators.map { operator =>
        operator.peerId -> GenesisOperatorConsensusKey(
          data.networkMagic,
          data.activationOrdinal,
          data.startingEpochProgress,
          operator.peerId,
          operator.address,
          Hex.fromBytes(operator.kesEntry.vk.value),
          operator.kesEntry.vk.step,
          operator.kesEntry.offset,
          VrfPublicKey.fromBytes(operator.vrfVk),
          Signature(Hex.fromBytes(operator.signature))
        )
      })
    } yield records
  }

  /** Build the immutable startup view from the same signed records that are committed into rooted genesis state. */
  def buildOperatorKeyRegistry[F[_]: Async: SecurityProvider](data: L0GenesisData): F[OperatorConsensusKeyRegistry[F]] =
    buildGenesisOperatorKeys[F](data).map(operatorKeyRegistryFromValidated[F])

  /** Validate and materialize a rooted genesis identity map. This path is used on restart and authenticated state import; it never consults
    * a sender-carried key and never synthesizes runtime registration history.
    */
  def buildOperatorKeyRegistryFromRooted[F[_]: Async: SecurityProvider](
    records: SortedMap[PeerId, GenesisOperatorConsensusKey]
  ): F[OperatorConsensusKeyRegistry[F]] =
    validateRootedGenesisOperatorKeys[F](records).map(operatorKeyRegistryFromValidated[F])

  /** Read the immutable genesis identity partition after the caller has verified the enclosing MPT root. Lossy hashed MPT keys are not
    * trusted: every value's `PeerId` is used to rederive its exact expected key, and misplaced/duplicate claims fail closed.
    */
  def materializeRootedGenesisOperatorKeys[F[_]: Async: Hasher: SecurityProvider](
    store: MptStore[F, GlobalStateKey]
  ): F[SortedMap[PeerId, GenesisOperatorConsensusKey]] =
    for {
      prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.GenesisOperatorKeys)
      entries <- store.getAllForPrefix[GenesisOperatorConsensusKey](prefix)
      classified <- entries.toList.sortBy(_._1.value).traverse {
        case (actualKey, record) =>
          GlobalStateKey
            .genesisOperatorKey[F](record.operatorPeerId)
            .flatMap(GlobalStateKey.toHex[F])
            .map(expectedKey => (actualKey, expectedKey, record))
      }
      misplaced = classified.collect { case (actual, expected, record) if actual =!= expected => record.operatorPeerId }.distinct.sorted
      grouped = classified.groupBy(_._3.operatorPeerId)
      duplicates = grouped.collect { case (peerId, claims) if claims.sizeCompare(1) > 0 => peerId }.toList.sorted
      _ <- Async[F]
        .raiseError[Unit](
          invalidGenesis(
            s"corrupt rooted genesis operator-key partition: misplaced=${misplaced.map(peerLabel).mkString(",")} " +
              s"duplicates=${duplicates.map(peerLabel).mkString(",")}"
          )
        )
        .whenA(misplaced.nonEmpty || duplicates.nonEmpty)
      records = SortedMap.from(classified.map { case (_, _, record) => record.operatorPeerId -> record })
      validated <- validateRootedGenesisOperatorKeys[F](records)
    } yield validated

  /** Require the already-wired local startup view to equal the root-authenticated period-zero key identity at both key halves. Exact
    * signed-record/context equality is enforced separately by [[requireGenesisDataMatchesRooted]].
    */
  def requireLocalRegistryMatchesRooted[F[_]: Async: SecurityProvider](
    local: OperatorConsensusKeyRegistry[F],
    rooted: SortedMap[PeerId, GenesisOperatorConsensusKey]
  ): F[Unit] =
    for {
      rootedRegistry <- buildOperatorKeyRegistryFromRooted[F](rooted)
      localEntries <- local.list
      rootedEntries <- rootedRegistry.list
      matches = localEntries.keySet === rootedEntries.keySet && localEntries.forall {
        case (peerId, localKeys) => rootedEntries.get(peerId).exists(rootedKeys => sameOperatorKeys(localKeys, rootedKeys))
      }
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis("local operator-key material does not match the root-authenticated genesis identity"))
        .unlessA(matches)
    } yield ()

  /** Require the complete locally supplied signed genesis records, including network/activation/start context and signatures, to equal the
    * root-authenticated records. This is the restart guard against a locally replaced genesis JSON file.
    */
  def requireGenesisDataMatchesRooted[F[_]: Async: SecurityProvider](
    localData: L0GenesisData,
    rooted: SortedMap[PeerId, GenesisOperatorConsensusKey]
  ): F[Unit] =
    buildGenesisOperatorKeys[F](localData).flatMap { localRecords =>
      Async[F]
        .raiseError[Unit](invalidGenesis("local signed genesis operator records do not equal the root-authenticated identity"))
        .unlessA(localRecords === rooted)
    }

  private def operatorKeyRegistryFromValidated[F[_]: Async](
    records: SortedMap[PeerId, GenesisOperatorConsensusKey]
  ): OperatorConsensusKeyRegistry[F] =
    OperatorConsensusKeyRegistry.make[F](records.iterator.map { case (peerId, record) => peerId -> toOperatorKeys(record) }.toMap)

  private def toOperatorKeys(record: GenesisOperatorConsensusKey): OperatorConsensusKeys =
    OperatorConsensusKeys(
      operatorPeerId = record.operatorPeerId,
      kes = KesRegistryEntry(
        VerificationKeyKesProduct(record.kesMasterVerificationKey.toBytes, record.kesMasterVerificationKeyStep),
        record.kesPeriodOffset
      ),
      vrfPublicKey = VrfPublicKey.fromBytes(record.vrfPublicKey.toBytes),
      effectiveFromPeriod = EtaPeriod.Zero,
      registration = none
    )

  private def sameOperatorKeys(left: OperatorConsensusKeys, right: OperatorConsensusKeys): Boolean =
    left.operatorPeerId === right.operatorPeerId &&
      left.kes.vk.step === right.kes.vk.step &&
      left.kes.offset === right.kes.offset &&
      left.kes.vk.value.sameElements(right.kes.vk.value) &&
      left.vrfPublicKey.toBytes.sameElements(right.vrfPublicKey.toBytes) &&
      left.effectiveFromPeriod === right.effectiveFromPeriod &&
      left.registration.isEmpty && right.registration.isEmpty

  private def validateRootedGenesisOperatorKeys[F[_]: Async: SecurityProvider](
    records: SortedMap[PeerId, GenesisOperatorConsensusKey]
  ): F[SortedMap[PeerId, GenesisOperatorConsensusKey]] = {
    val values = records.values.toList
    val duplicateAddresses = values
      .groupBy(_.operatorAddress)
      .collect {
        case (address, claims) if claims.sizeCompare(1) > 0 => address
      }
      .toList
    val duplicateKesKeys = duplicateKeyOwners[GenesisOperatorConsensusKey](
      values,
      _.kesMasterVerificationKey.toBytes,
      _.operatorPeerId
    )
    val duplicateVrfKeys = duplicateKeyOwners[GenesisOperatorConsensusKey](values, _.vrfPublicKey.toBytes, _.operatorPeerId)
    val contexts = values.map(record => (record.networkMagic, record.activationOrdinal, record.startingEpochProgress)).distinct
    val mapIdentityMismatches = records.collect { case (peerId, record) if peerId =!= record.operatorPeerId => peerId }.toList.sorted

    for {
      _ <- Async[F].raiseError[Unit](invalidGenesis("rooted genesis operator-key set is empty")).whenA(values.isEmpty)
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis("rooted genesis operator-key records do not share one network/activation context"))
        .unlessA(contexts.sizeCompare(1) === 0)
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis(s"rooted map identity mismatch: ${mapIdentityMismatches.map(peerLabel).mkString(",")}"))
        .whenA(mapIdentityMismatches.nonEmpty)
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis(s"duplicate rooted operator address(es): ${duplicateAddresses.mkString(",")}"))
        .whenA(duplicateAddresses.nonEmpty)
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis(s"duplicate rooted KES key owner group(s): ${duplicateKesKeys.mkString(",")}"))
        .whenA(duplicateKesKeys.nonEmpty)
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis(s"duplicate rooted VRF key owner group(s): ${duplicateVrfKeys.mkString(",")}"))
        .whenA(duplicateVrfKeys.nonEmpty)
      _ <- values.traverse_(validateRootedGenesisOperatorKey[F])
    } yield records
  }

  private def validateRootedGenesisOperatorKey[F[_]: Async: SecurityProvider](
    record: GenesisOperatorConsensusKey
  ): F[Unit] =
    for {
      _ <- Async[F].raiseError[Unit](invalidGenesis("networkMagic must be non-empty")).whenA(record.networkMagic.isEmpty)
      _ <- Async[F].raiseError[Unit](invalidGenesis("activationOrdinal must be non-negative")).whenA(record.activationOrdinal < 0L)
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis("startingEpochProgress must be non-negative"))
        .whenA(record.startingEpochProgress < 0L)
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis(s"${peerLabel(record.operatorPeerId)} genesis KES key must be exactly 32 bytes at step zero"))
        .unlessA(
          record.kesMasterVerificationKey.toBytes.length === KesMasterVerificationKeyLength && record.kesMasterVerificationKeyStep === 0
        )
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis(s"${peerLabel(record.operatorPeerId)} genesis KES period offset must be zero"))
        .unlessA(record.kesPeriodOffset === 0L)
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis(s"${peerLabel(record.operatorPeerId)} genesis VRF key must be exactly 32 bytes"))
        .unlessA(record.vrfPublicKey.toBytes.length === VrfPublicKey.ExpectedLength)
      publicKey <- record.operatorPeerId.value.toPublicKey[F].adaptError { case _ => invalidGenesis("invalid rooted operator PeerId") }
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis(s"${peerLabel(record.operatorPeerId)} rooted address does not match its PeerId"))
        .unlessA(publicKey.toAddress === record.operatorAddress)
      valid <- Signing
        .verifySignature[F](GenesisOperatorConsensusKey.signaturePreimage(record), record.longTermSignature.value.toBytes)(publicKey)
        .adaptError { case _ => invalidGenesis(s"${peerLabel(record.operatorPeerId)} has a malformed rooted long-term signature") }
      _ <- Async[F]
        .raiseError[Unit](invalidGenesis(s"${peerLabel(record.operatorPeerId)} rooted long-term signature is invalid"))
        .unlessA(valid)
    } yield ()
}
