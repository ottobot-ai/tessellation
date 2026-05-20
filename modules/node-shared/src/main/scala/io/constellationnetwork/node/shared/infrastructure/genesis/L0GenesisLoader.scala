package io.constellationnetwork.node.shared.infrastructure.genesis

import java.security._
import java.security.spec.PKCS8EncodedKeySpec

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.genesis.types.{L0GenesisData, L0GenesisDelegatedStake, L0GenesisNodeCollateral}
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, UpdateDelegatedStake}
import io.constellationnetwork.schema.nakamoto.EpochStakeSnapshotter
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.refineV
import eu.timepit.refined.types.numeric.NonNegLong

/** Helper that turns a Tier-1 `L0GenesisData` into a `GlobalSnapshotInfo` overlay. Called from the dag-l0 `Main.scala` JSON-genesis
  * bootstrap branch AFTER `hashedGenesis.info.toGlobalSnapshotInfo` is computed — we widen the in-memory GSI with delegated-stake +
  * collateral entries before any downstream consumer (storages, services) reads it. The on-disk `Signed[GlobalSnapshot]` stays V1 (Option
  * (ii) in the §1.1 plan — lowest blast radius).
  *
  * Signing: ECDSA signatures in this codebase are NOT deterministic (`Signing.signData` uses an unseeded `SecureRandom`). The fixture file
  * stores the raw event plus the synthetic delegator's PKCS8 private-key hex; we sign at LOAD time. The signature bytes vary across loads,
  * but they are always valid (public key recoverable, hash signs OK) and the VRF stake-weighting reads `amount`, never the signature.
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

  /** Sign a synthetic delegated-stake event using its embedded delegator private key, then wrap the result in a runtime
    * `DelegatedStakeRecord`. Defensive: if signing fails (corrupt hex, wrong curve, etc), the entry is skipped and logged — Tier-1 fixtures
    * are reviewed before landing so silent-skip on malformed records is the conservative choice.
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

      // §3 NIPoPoW S0.5 — genesis backfill. Stamp the just-built genesis stake distribution under
      // eta-period keys {-2, -1, 0} so the N-2 lookback used by slot-leader eligibility returns the
      // genesis distribution during the first three eta periods (periods 0, 1, 2). Without this, the
      // first three periods fall through to the current-GSI default — correct only by coincidence
      // when the genesis distribution is unchanged. Backfill makes the lookup explicit and turns
      // periods 0/1/2 leader election into a deterministic read against `historicalStakeSnapshots`.
      val augmented = base.copy(
        balances = mergedBalances,
        activeDelegatedStakes = Some(stakeMap),
        activeNodeCollaterals = Some(collMap)
      )
      EpochStakeSnapshotter.backfillGenesisStakeSnapshots(augmented)
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
