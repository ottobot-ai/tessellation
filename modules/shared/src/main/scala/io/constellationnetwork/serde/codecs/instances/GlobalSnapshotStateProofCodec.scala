package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.merkletree.MerkleRoot
import io.constellationnetwork.schema.{GlobalSnapshotStateProof, GlobalSnapshotStateProofV1}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.MerkleRootCodec.{codec => merkleRootCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codecs for the state-proof family:
  *   - `GlobalSnapshotStateProofV1` — 4 fields (3 required Hashes + 1 optional MerkleRoot).
  *   - `GlobalSnapshotStateProof` — 19 fields (V1 + 15 optional Hashes; 14th = `historicalStakeSnapshots`, the §3 NIPoPoW per-field subtree
  *     root over the stake-snapshot partition; 15th = `smtRoot`, the §3 NIPoPoW historical-commitment SMT root).
  *
  * Both are FROZEN consensus types. V1 is the legacy, pre-MPT shape; the 19-field current variant adds optional witness hashes for features
  * that were added incrementally (allow-spends, token locks, delegated staking, node collaterals, price state, multi-currency snapshots,
  * the `mptRoot` covering all partitions, the NIPoPoW historical-stake snapshot partition, and the NIPoPoW historical-commitment
  * `smtRoot`).
  *
  * Field order matches the case class declaration exactly. Adding / reordering / removing a field requires introducing a new era (e.g.
  * `GlobalSnapshotStateProofV2Codec`) — this codec is never mutated. (Greenfield: no on-wire back-compat is owed, so appending the optional
  * `smtRoot` to the current codec is acceptable — old bytes that lacked it never existed in a released chain.)
  *
  * Sizes:
  *   - V1: 32 + 32 + 32 + (1 | 37) = 97 or 129 bytes.
  *   - Current: V1 payload + 15 × (1 | 33) = 112 .. 626 bytes. The 1-byte Option discriminator means the absent case is a single 0x00 byte
  *     — tight for the "legacy snapshot without any of the post-V1 features" case.
  *
  * The schemas are deliberately kept separate (not unified via "V1 is a prefix of current") — historical V1 bytes must decode via V1's
  * codec, and current bytes via the current codec. Mixing them would be an ordinal-era bug.
  */
object GlobalSnapshotStateProofCodec {

  private val optionalMerkleRootCodec: Codec[Option[MerkleRoot]] = option(merkleRootCodec)
  private val optionalHashCodec: Codec[Option[Hash]] = option(hashCodec)

  implicit val v1Codec: Codec[GlobalSnapshotStateProofV1] =
    (hashCodec :: hashCodec :: hashCodec :: optionalMerkleRootCodec)
      .xmap[GlobalSnapshotStateProofV1](
        {
          case sch :: tx :: bal :: curr :: HNil =>
            GlobalSnapshotStateProofV1(sch, tx, bal, curr)
        },
        p =>
          p.lastStateChannelSnapshotHashesProof ::
            p.lastTxRefsProof ::
            p.balancesProof ::
            p.lastCurrencySnapshotsProof ::
            HNil
      )

  implicit val v1ImmutableCodec: ImmutableCodec[GlobalSnapshotStateProofV1] =
    ImmutableCodec.fromScodecCodec(v1Codec)

  implicit val codec: Codec[GlobalSnapshotStateProof] =
    (hashCodec ::
      hashCodec ::
      hashCodec ::
      optionalMerkleRootCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec ::
      optionalHashCodec)
      .xmap[GlobalSnapshotStateProof](
        {
          case sch :: tx :: bal :: curr ::
              allowSpends :: tokenLocks :: tokenLockBalances ::
              lastAllowSpendRefs :: lastTokenLockRefs ::
              updateNodeParams :: activeDelegated :: delegatedWithdrawals ::
              activeCollaterals :: collateralWithdrawals ::
              priceState :: lastGlobalWithCurrency :: mptRoot ::
              historicalStakeSnapshots :: smtRoot :: HNil =>
            GlobalSnapshotStateProof(
              sch,
              tx,
              bal,
              curr,
              allowSpends,
              tokenLocks,
              tokenLockBalances,
              lastAllowSpendRefs,
              lastTokenLockRefs,
              updateNodeParams,
              activeDelegated,
              delegatedWithdrawals,
              activeCollaterals,
              collateralWithdrawals,
              priceState,
              lastGlobalWithCurrency,
              mptRoot,
              historicalStakeSnapshots,
              smtRoot
            )
        },
        p =>
          p.lastStateChannelSnapshotHashesProof ::
            p.lastTxRefsProof ::
            p.balancesProof ::
            p.lastCurrencySnapshotsProof ::
            p.activeAllowSpends ::
            p.activeTokenLocks ::
            p.tokenLockBalances ::
            p.lastAllowSpendRefs ::
            p.lastTokenLockRefs ::
            p.updateNodeParameters ::
            p.activeDelegatedStakes ::
            p.delegatedStakesWithdrawals ::
            p.activeNodeCollaterals ::
            p.nodeCollateralWithdrawals ::
            p.priceState ::
            p.lastGlobalSnapshotsWithCurrency ::
            p.mptRoot ::
            p.historicalStakeSnapshots ::
            p.smtRoot ::
            HNil
      )

  implicit val immutableCodec: ImmutableCodec[GlobalSnapshotStateProof] =
    ImmutableCodec.fromScodecCodec(codec)
}
