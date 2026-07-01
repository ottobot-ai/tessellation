package io.constellationnetwork.node.shared.domain.genesis

import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.UpdateDelegatedStake
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral

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

  /** Operator-set entry: per-validator identity. `peerId` is the canonical sortition unit (per `project_consensus_epoch_staggering`).
    * `vrfPublicKey` and `kesPublicKey` are reserved for §1.2 KES wire-in and §1.3 VRF reroll; they are nullable in Tier-1 (the generator
    * emits null — the existing ECDSA key in `nodes/N/key.p12` is the operator credential at boot).
    */
  case class L0GenesisOperator(
    peerId: String,
    address: String,
    vrfPublicKey: Option[String],
    kesPublicKey: Option[String]
  )

  object L0GenesisOperator {
    implicit val encoder: Encoder[L0GenesisOperator] = deriveEncoder[L0GenesisOperator]
    implicit val decoder: Decoder[L0GenesisOperator] = deriveDecoder[L0GenesisOperator]
  }

  /** Genesis-seeded delegated-stake record. Tier-1 design: the fixture file is BYTE-DETERMINISTIC across regenerations with the same seed,
    * but ECDSA signatures are NOT deterministic in this codebase (`Signing.signData` uses a non-seeded `SecureRandom`). Resolution: the
    * fixture embeds the RAW unsigned event plus the synthetic delegator's private-key hex. The loader signs at LOAD time. Each load
    * produces a valid `Signed[UpdateDelegatedStake.Create]` whose signature differs by RNG but verifies against the same public key.
    *
    * Trade-off: the runtime in-memory `Signed[...]` is not byte-equal across cluster restarts. For Tier-1 (genesis-seeded validator stake
    * distribution) this is acceptable — the `activeDelegatedStakes` map is keyed by `(address, ordinal)` not by signature bytes, and the
    * VRF stake-weighting reads `amount` (not the signature). Tier-2 cross-client byte-portability is explicitly out of scope.
    */
  case class L0GenesisDelegatedStake(
    event: UpdateDelegatedStake.Create,
    delegatorPrivateKeyHex: String,
    createdAt: Long,
    rewards: Long
  )

  object L0GenesisDelegatedStake {
    implicit val encoder: Encoder[L0GenesisDelegatedStake] = deriveEncoder[L0GenesisDelegatedStake]
    implicit val decoder: Decoder[L0GenesisDelegatedStake] = deriveDecoder[L0GenesisDelegatedStake]
  }

  /** Genesis-seeded node-collateral record. Same shape as `L0GenesisDelegatedStake` minus rewards (collateral records do not accumulate
    * rewards by themselves).
    */
  case class L0GenesisNodeCollateral(
    event: UpdateNodeCollateral.Create,
    ownerPrivateKeyHex: String,
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
  /** Per-operator KES master-VK registration cert embedded in L0 genesis. Slice 3 of §1.2 KES wiring.
    *
    *   - `peerId` — hex-encoded operator PeerId (matches `L0GenesisOperator.peerId`).
    *   - `kesVk` — hex-encoded `VerificationKeyKesProduct.value` (the period-0 root of the super × sub Merkle tree, 32 bytes for the
    *     Blake2b-256 default).
    *   - `kesVkStep` — `VerificationKeyKesProduct.step`. Always `0` for a freshly-generated master VK at genesis time; carried explicitly
    *     so reconstruction of the `VerificationKeyKesProduct` doesn't have to assume a fixed step.
    *   - `longTermSig` — hex-encoded SHA512withECDSA signature of `kesVk` raw bytes (after hex-decode) under the operator's long-term
    *     Ed25519 private key. This is the registration binding: any receiver who knows the operator's long-term public key (from the
    *     seedlist / `L0GenesisOperator`) can verify the binding without ever seeing the KES SK.
    *
    * Optional in the L0 genesis JSON for backward compatibility with fixtures generated before §1.2 Slice 3 (they parse with the field
    * absent and `KesRegistry.empty` is used downstream).
    */
  case class L0GenesisKesRegistration(
    peerId: String,
    kesVk: String,
    kesVkStep: Int,
    longTermSig: String,
    // Eta-period offset for this operator's KES tree. Genesis operators register at offset=0
    // (their tree's step 0 == global eta period 0). Mid-life joiners (Slice 10 #179) use a
    // positive offset matching the global eta period at registration activation. See
    // KesRegistryEntry doc for verifier semantics.
    offset: Long
  )

  object L0GenesisKesRegistration {
    implicit val encoder: Encoder[L0GenesisKesRegistration] = deriveEncoder[L0GenesisKesRegistration]
    implicit val decoder: Decoder[L0GenesisKesRegistration] = deriveDecoder[L0GenesisKesRegistration]
  }

  case class L0GenesisData(
    _meta: L0GenesisMeta,
    networkMagic: String,
    activationOrdinal: Long,
    startingEpochProgress: Long,
    protocolParams: L0GenesisProtocolParams,
    operators: List[L0GenesisOperator],
    delegatedStakes: List[L0GenesisDelegatedStake],
    nodeCollaterals: List[L0GenesisNodeCollateral],
    initialBalances: List[L0GenesisBalance],
    // §1.2 Slice 3: per-operator KES master VK registration. Optional so existing Tier-1 fixtures
    // (8-node-uniform-stake.json etc) still parse — they pre-date this field and consume the
    // `KesRegistry.empty` default downstream.
    kesRegistrations: Option[List[L0GenesisKesRegistration]] = None
  ) {

    /** Build a deterministic balance map for `GlobalSnapshot.mkGenesis`. Combines `initialBalances` with the delegator/owner addresses from
      * stake records so the signer addresses actually have non-zero balances (avoids "signer address has no balance" surprises downstream).
      */
    def initialBalanceMap: Map[Address, Balance] = {
      def parseAddr(s: String): Option[Address] =
        refineV[DAGAddressRefined](s).toOption.map(Address(_))
      def parseBalance(v: Long): Option[Balance] =
        refineV[NonNegative](v).toOption.map(Balance(_))

      val explicit = initialBalances.flatMap { b =>
        for {
          addr <- parseAddr(b.address)
          bal <- parseBalance(b.balance)
        } yield addr -> bal
      }.toMap

      // Stake-signer addresses get a 1-DAG stipend if not already in `initialBalances` so the
      // address is present in the genesis balance map. Stake records carry the actual amount via
      // the signed event; this stipend is just "address must exist".
      val stipend = Balance(refineV[NonNegative](1L).toOption.get)
      val signerAddrs =
        delegatedStakes.map(_.event.source.value.value) ++ nodeCollaterals.map(_.event.source.value.value)
      val signerBalances = signerAddrs
        .flatMap(parseAddr)
        .map { addr =>
          explicit.get(addr) match {
            case Some(b) => addr -> b
            case None    => addr -> stipend
          }
        }
        .toMap

      explicit ++ signerBalances
    }
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
