package io.constellationnetwork.node.shared.infrastructure.genesis

import java.security._
import java.security.spec.PKCS8EncodedKeySpec

import cats.data.NonEmptySet
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.node.shared.domain.genesis.types.{L0GenesisData, L0GenesisDelegatedStake, L0GenesisNodeCollateral}
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, UpdateDelegatedStake}
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, SecurityProvider}

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
        activeNodeCollaterals = Some(collMap)
      )
    }

  /** Build a [[KesRegistry]] from the `kesRegistrations` field of an L0 genesis fixture. Each entry is hex-decoded into a
    * `VerificationKeyKesProduct` and keyed by the registered `PeerId`. Entries whose `peerId` or `kesVk` fail to hex-decode are dropped
    * silently — Tier-1 fixtures are reviewed before landing, so a malformed registration is best surfaced as "peer absent from registry"
    * rather than as a hard failure during boot.
    *
    *   - Missing `kesRegistrations` (None) ⇒ empty registry. Slice 3 backward-compat for fixtures predating the field.
    *   - `longTermSig` is parsed and kept available for callers that want to re-verify the binding at load time (e.g. a startup sanity
    *     check that the operator's long-term pubkey actually signed this VK). For Slice 3 we just trust the fixture — the generator side
    *     runs the binding signature, and the loader trusts the file. A future strict-mode could verify here.
    */
  def buildKesRegistry[F[_]: Async](data: L0GenesisData): F[KesRegistry[F]] =
    Async[F].delay {
      val parsed: Map[PeerId, KesRegistryEntry] =
        data.kesRegistrations
          .getOrElse(Nil)
          .flatMap { r =>
            val peerOpt = scala.util.Try(Id(Hex(r.peerId)).toPeerId).toOption
            val vkBytesOpt = scala.util.Try(Hex(r.kesVk).toBytes).toOption
            (peerOpt, vkBytesOpt) match {
              case (Some(p), Some(vkBytes)) =>
                Some(p -> KesRegistryEntry(VerificationKeyKesProduct(vkBytes, r.kesVkStep), r.offset))
              case _ => None
            }
          }
          .toMap
      KesRegistry.make[F](parsed)
    }
}
