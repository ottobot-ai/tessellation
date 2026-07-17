package io.constellationnetwork.node.shared.domain.genesis

import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.UpdateDelegatedStake
import io.constellationnetwork.schema.nakamoto.GenesisOperatorConsensusKey
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.signature.Signed

import derevo.cats.{eqv, show}
import derevo.derive
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.refineV
import fs2.data.csv.RowDecoder
import fs2.data.csv.generic.semiauto.deriveRowDecoder
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}

object types {

  @derive(eqv, show)
  case class GenesisAccount(address: Address, balance: Balance)

  case class GenesisCSVAccount(address: String, balance: Long) {

    def toGenesisAccount: Either[String, GenesisAccount] =
      for {
        dagAddress <- refineV[DAGAddressRefined](address)
        nonNegLong <- refineV[NonNegative](balance)
      } yield GenesisAccount(Address(dagAddress), Balance(nonNegLong))
  }

  object GenesisCSVAccount {
    implicit val rowDecoder: RowDecoder[GenesisCSVAccount] = deriveRowDecoder
  }

  /** Meta block carried at the top of every generated `l0-genesis.json`. Records the generator version, ISO8601 generation timestamp,
    * invocation string, seed, and human-readable expected properties. The block is informational only (it does not feed into consensus
    * state) but is required for the fixture-library review pattern.
    */
  case class L0GenesisMeta(
    generatorVersion: String,
    generatedAt: String,
    invocation: String,
    seed: Long,
    expectedProperties: List[String]
  )

  object L0GenesisMeta {
    implicit val encoder: Encoder[L0GenesisMeta] = deriveEncoder[L0GenesisMeta]
    implicit val decoder: Decoder[L0GenesisMeta] = deriveDecoder[L0GenesisMeta]
  }

  /** Protocol-parameter block for L0 genesis. Defaults track the §1.1 Taktikos defaults. */
  case class L0GenesisProtocolParams(
    lddCutoff: Int,
    etaRotationSnapshots: Long,
    genesisEta: String,
    startingEpochProgress: Long
  )

  object L0GenesisProtocolParams {
    implicit val encoder: Encoder[L0GenesisProtocolParams] = deriveEncoder[L0GenesisProtocolParams]
    implicit val decoder: Decoder[L0GenesisProtocolParams] = deriveDecoder[L0GenesisProtocolParams]

    // INFORMATIONAL genesis-record defaults — consensus does NOT read these. The live, per-env values are authoritative in
    // HOCON (`nakamoto.ldd` γ; `nakamoto.confirmation-depth-k` → derived R = round(3.1·k₁)). Kept in lock-step with
    // `application.conf` so a generated genesis file is not misleading.
    //   - lddCutoff = 16          — γ, mirrors `nakamoto.ldd.cutoff`.
    //   - etaRotationSnapshots    — R, the eta-rotation EPOCH in SNAPSHOTS = round(3.1·k₁); 3174 at the mainnet k₁=1024
    //                               reference (NOT 10·k₁ — the old 2550 label was stale). Consensus derives the real
    //                               per-env R from k₁; this is a record only.
    val default: L0GenesisProtocolParams = L0GenesisProtocolParams(
      lddCutoff = 16,
      etaRotationSnapshots = 3174L,
      genesisEta = "tessellation-nakamoto-genesis",
      startingEpochProgress = 0L
    )
  }

  /** Atomic genesis operator-key record. The long-term operator identity signs one domain-separated preimage that binds the chain context,
    * operator identity/address, KES master verification key metadata, and VRF verification key. No key field is optional and there is no
    * second registration list whose coverage or signature semantics can drift from this record.
    */
  case class L0GenesisOperator(
    peerId: String,
    address: String,
    kesMasterVk: String,
    kesMasterVkStep: Int,
    kesPeriodOffset: Long,
    vrfVk: String,
    longTermSignature: String
  )

  object L0GenesisOperator {
    implicit val encoder: Encoder[L0GenesisOperator] = deriveEncoder[L0GenesisOperator]
    implicit val decoder: Decoder[L0GenesisOperator] = deriveDecoder[L0GenesisOperator]

    /** Canonical long-term-signature preimage for a genesis operator-key record. Length-prefixing every variable-width field and using
      * fixed-width big-endian integers makes concatenation unambiguous. `networkMagic`, activation ordinal, and starting epoch progress
      * bind the record to the intended chain/hard-fork context; a record copied to another network or activation point cannot verify.
      */
    def signaturePreimage(
      networkMagic: String,
      activationOrdinal: Long,
      startingEpochProgress: Long,
      peerId: Array[Byte],
      address: String,
      kesMasterVk: Array[Byte],
      kesMasterVkStep: Int,
      kesPeriodOffset: Long,
      vrfVk: Array[Byte]
    ): Array[Byte] =
      GenesisOperatorConsensusKey.signaturePreimage(
        networkMagic,
        activationOrdinal,
        startingEpochProgress,
        peerId,
        address,
        kesMasterVk,
        kesMasterVkStep,
        kesPeriodOffset,
        vrfVk
      )
  }

  /** Fully signed genesis delegated-stake bundle. Genesis JSON is public consensus input and therefore never contains an owner private key.
    * The loader verifies both signatures and derives the only accepted `tokenLockRef` from `signedBackingTokenLock`.
    */
  case class L0GenesisDelegatedStake(
    signedEvent: Signed[UpdateDelegatedStake.Create],
    signedBackingTokenLock: Signed[TokenLock],
    createdAt: Long,
    rewards: Long
  )

  object L0GenesisDelegatedStake {
    implicit val encoder: Encoder[L0GenesisDelegatedStake] = deriveEncoder[L0GenesisDelegatedStake]
    implicit val decoder: Decoder[L0GenesisDelegatedStake] = deriveDecoder[L0GenesisDelegatedStake]
  }

  /** Fully signed genesis node-collateral bundle. Same security contract as `L0GenesisDelegatedStake`; no owner secret is persisted. */
  case class L0GenesisNodeCollateral(
    signedEvent: Signed[UpdateNodeCollateral.Create],
    signedBackingTokenLock: Signed[TokenLock],
    createdAt: Long
  )

  object L0GenesisNodeCollateral {
    implicit val encoder: Encoder[L0GenesisNodeCollateral] = deriveEncoder[L0GenesisNodeCollateral]
    implicit val decoder: Decoder[L0GenesisNodeCollateral] = deriveDecoder[L0GenesisNodeCollateral]
  }

  /** Initial balance for the synthesized initial-balance set in `l0-genesis.json`. */
  case class L0GenesisBalance(address: String, balance: Long)

  object L0GenesisBalance {
    implicit val encoder: Encoder[L0GenesisBalance] = deriveEncoder[L0GenesisBalance]
    implicit val decoder: Decoder[L0GenesisBalance] = deriveDecoder[L0GenesisBalance]
  }

  /** Top-level L0 genesis schema. See the spec block in `docs/nakamoto/IMPLEMENTATION-PLAN-POST-VALIDATION.md` §1.1 and
    * `project_test_vector_pattern` memory for design rationale.
    *
    * The loader (`GenesisFS.loadL0Genesis`) parses this; the dag-l0 Main.scala dispatcher then uses `initialBalanceMap` for
    * `GlobalSnapshot.mkGenesis`, and applies the `delegatedStakes` and `nodeCollaterals` fields by overlay onto the in-memory
    * `GlobalSnapshotInfo` AFTER `toGlobalSnapshotInfo` is called (Option (ii) in the plan — the on-disk `Signed[GlobalSnapshot]` stays V1).
    */
  case class L0GenesisData(
    _meta: L0GenesisMeta,
    networkMagic: String,
    activationOrdinal: Long,
    startingEpochProgress: Long,
    protocolParams: L0GenesisProtocolParams,
    operators: List[L0GenesisOperator],
    delegatedStakes: List[L0GenesisDelegatedStake],
    nodeCollaterals: List[L0GenesisNodeCollateral],
    initialBalances: List[L0GenesisBalance]
  ) {

    /** Explicit spendable balances only. Active backing-lock principal is a separate genesis allocation and is never synthesized as a
      * spendable stipend. Invalid or duplicate balance entries fail closed instead of being silently dropped/overwritten.
      */
    def initialBalanceMap: Map[Address, Balance] =
      validatedGenesisAllocation.fold(message => throw new IllegalArgumentException(message), _._1)

    def validatedGenesisAllocation: Either[String, (Map[Address, Balance], BigInt)] =
      for {
        balances <- initialBalances.zipWithIndex.foldLeft[Either[String, Map[Address, Balance]]](Right(Map.empty)) {
          case (accE, (entry, index)) =>
            for {
              acc <- accE
              refinedAddress <- refineV[DAGAddressRefined](entry.address).left
                .map(error => s"Invalid genesis balance address at index $index: $error")
              address = Address(refinedAddress)
              refinedBalance <- refineV[NonNegative](entry.balance).left
                .map(error => s"Invalid genesis balance at index $index: $error")
              _ <- Either.cond(
                !acc.contains(address),
                (),
                s"Duplicate genesis balance address at index $index: ${address.value.value}"
              )
            } yield acc.updated(address, Balance(refinedBalance))
        }
        spendable = balances.valuesIterator.foldLeft(BigInt(0))((sum, balance) => sum + balance.value.value)
        backing =
          delegatedStakes.iterator
            .map(_.signedBackingTokenLock.amount.value.value)
            .++(nodeCollaterals.iterator.map(_.signedBackingTokenLock.amount.value.value))
            .foldLeft(BigInt(0))(_ + _)
        allocation = spendable + backing
        _ <- Either.cond(
          allocation <= BigInt(Long.MaxValue),
          (),
          s"Genesis allocation exceeds the protocol Long domain: $allocation"
        )
      } yield balances -> allocation
  }

  object L0GenesisData {
    implicit val encoder: Encoder[L0GenesisData] = deriveEncoder[L0GenesisData]
    implicit val decoder: Decoder[L0GenesisData] = deriveDecoder[L0GenesisData]

    def canonicalJson(data: L0GenesisData): Json = data.asJson
  }

  /** Per-metagraph cl1 genesis fixture. Single-metagraph for Tier-1; the L0/L1 split mirrors Cardano's per-era split, and the metagraphId
    * binds this fixture to a specific metagraph address (set at metagraph-creation time).
    */
  case class Cl1GenesisData(
    metagraphId: String,
    activationOrdinal: Long,
    balances: List[L0GenesisBalance]
  )

  object Cl1GenesisData {
    implicit val encoder: Encoder[Cl1GenesisData] = deriveEncoder[Cl1GenesisData]
    implicit val decoder: Decoder[Cl1GenesisData] = deriveDecoder[Cl1GenesisData]
  }
}
