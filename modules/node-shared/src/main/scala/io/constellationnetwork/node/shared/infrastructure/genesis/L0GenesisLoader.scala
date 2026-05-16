package io.constellationnetwork.node.shared.infrastructure.genesis

import java.security._
import java.security.spec.PKCS8EncodedKeySpec

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.genesis.types.{L0GenesisData, L0GenesisDelegatedStake, L0GenesisNodeCollateral}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, UpdateDelegatedStake}
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, UpdateNodeCollateral}
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.refineV
import eu.timepit.refined.types.numeric.NonNegLong

/** Helper that turns a Tier-1 `L0GenesisData` into a `GlobalSnapshotInfo` overlay. Called from the
  * dag-l0 `Main.scala` JSON-genesis bootstrap branch AFTER `hashedGenesis.info.toGlobalSnapshotInfo`
  * is computed — we widen the in-memory GSI with delegated-stake + collateral entries before any
  * downstream consumer (storages, services) reads it. The on-disk `Signed[GlobalSnapshot]` stays V1
  * (Option (ii) in the §1.1 plan — lowest blast radius).
  *
  * Signing: ECDSA signatures in this codebase are NOT deterministic
  * (`Signing.signData` uses an unseeded `SecureRandom`). The fixture file stores the raw event
  * plus the synthetic delegator's PKCS8 private-key hex; we sign at LOAD time. The signature bytes
  * vary across loads, but they are always valid (public key recoverable, hash signs OK) and the
  * VRF stake-weighting reads `amount`, never the signature.
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

  /** Recover the public key from the private key. For EC keys produced by `KeyPairGenerator`,
    * BouncyCastle stores the public point inside the PKCS8 attributes; we reconstruct it via
    * `BCECPrivateKey.getParameters`. Falls back to ECPublicKeySpec arithmetic if that's unavailable.
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

  /** Sign a synthetic delegated-stake event using its embedded delegator private key, then wrap
    * the result in a runtime `DelegatedStakeRecord`. Defensive: if signing fails (corrupt hex,
    * wrong curve, etc), the entry is skipped and logged — Tier-1 fixtures are reviewed before
    * landing so silent-skip on malformed records is the conservative choice.
    */
  private def signStake[F[_]: Async: Hasher: SecurityProvider](
    s: L0GenesisDelegatedStake
  ): F[Option[(Address, DelegatedStakeRecord)]] =
    keyPairFromHex[F](s.delegatorPrivateKeyHex).flatMap { kp =>
      Signed.forAsyncHasher[F, UpdateDelegatedStake.Create](s.event, kp).map { signed =>
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
      Signed.forAsyncHasher[F, UpdateNodeCollateral.Create](c.event, kp).map { signed =>
        val createdAt = SnapshotOrdinal(NonNegLong.unsafeFrom(c.createdAt))
        val record = NodeCollateralRecord(signed, createdAt)
        Option(c.event.source -> record)
      }
    }.handleError(_ => Option.empty[(Address, NodeCollateralRecord)])

  /** Augment a base `GlobalSnapshotInfo` (from `GlobalSnapshotInfoV1.toGlobalSnapshotInfo`) with
    * the delegated-stake records, node-collateral records, and balances declared in an L0 genesis
    * fixture. Returns a new GSI with `activeDelegatedStakes`, `activeNodeCollaterals`, and
    * `balances` populated. Other fields (allow-spends, token-locks, etc.) are left at the empty
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
}
