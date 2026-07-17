package io.constellationnetwork.node.shared.infrastructure.genesis

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.node.shared.domain.economics.StakeBackingValidator
import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistryEntry, OperatorConsensusKeyRegistry}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, UpdateDelegatedStake}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GenesisOperatorConsensusKey}
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, UpdateNodeCollateral}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.signature.Signature
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.serde.codecs.instances.GenesisOperatorConsensusKeyCodec.immutableCodec

import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.refineV
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Encoder

/** Validates and installs public, fully signed genesis economic bundles. Genesis loading never receives or reconstructs an economic owner
  * private key. Every node installs the exact signed bytes committed in the canonical genesis file.
  */
object L0GenesisLoader {

  private val PeerIdLength = 64
  private val KesMasterVerificationKeyLength = 32

  private def invalidEconomicGenesis(message: String): IllegalArgumentException =
    new IllegalArgumentException(s"Invalid canonical L0 genesis economic bundle: $message")

  private def requireEconomic[F[_]: Async](condition: Boolean, message: => String): F[Unit] =
    Async[F].raiseError[Unit](invalidEconomicGenesis(message)).unlessA(condition)

  private def nonNegative[F[_]: Async](label: String, value: Long): F[NonNegLong] =
    refineV[NonNegative](value)
      .leftMap(error => invalidEconomicGenesis(s"$label must be non-negative: $error"))
      .liftTo[F]
      .map(refined => NonNegLong.unsafeFrom(refined.value))

  private def validateSignedBySource[F[_]: Async: Hasher: SecurityProvider, A: Encoder](
    label: String,
    signed: Signed[A],
    source: Address
  ): F[Id] =
    for {
      _ <- requireEconomic[F](signed.proofs.size === 1, s"$label must have exactly one owner signature")
      proofId = signed.proofs.head.id
      signer <- proofId.toAddress.adaptError {
        case _ =>
          invalidEconomicGenesis(s"$label carries a malformed signer identity")
      }
      _ <- requireEconomic[F](
        signer === source,
        s"$label signer ${signer.value.value} does not match source ${source.value.value}"
      )
      valid <- signed.hasValidSignature[F].adaptError {
        case _ =>
          invalidEconomicGenesis(s"$label signature is malformed")
      }
      _ <- requireEconomic[F](valid, s"$label signature is invalid")
    } yield proofId

  private def validateBackingLock[F[_]: Async: Hasher: SecurityProvider](
    label: String,
    source: Address,
    exactAmount: Long,
    expectedRef: Hash,
    signedLock: Signed[TokenLock]
  ): F[Id] = {
    val lock = signedLock.value

    for {
      proofId <- validateSignedBySource[F, TokenLock](s"$label backing token lock", signedLock, source)
      _ <- requireEconomic[F](lock.source === source, s"$label backing token-lock source mismatch")
      _ <- requireEconomic[F](
        lock.amount.value.value === exactAmount,
        s"$label backing token-lock amount ${lock.amount.value.value} does not exactly equal event amount $exactAmount"
      )
      _ <- requireEconomic[F](lock.fee.value.value === 0L, s"$label backing token-lock fee must be zero")
      _ <- requireEconomic[F](lock.parent === TokenLockReference.empty, s"$label backing token-lock parent must be native empty")
      _ <- requireEconomic[F](lock.currencyId.isEmpty, s"$label backing token lock must be native")
      _ <- requireEconomic[F](lock.unlockEpoch.isEmpty, s"$label backing token lock must be indefinite")
      _ <- requireEconomic[F](lock.replaceTokenLockRef.isEmpty, s"$label backing token lock cannot replace another lock")
      computedRef <- TokenLockReference.of[F](signedLock)
      _ <- requireEconomic[F](
        computedRef.hash === expectedRef,
        s"$label event tokenLockRef does not equal the canonical backing token-lock hash"
      )
    } yield proofId
  }

  private def validateStake[F[_]: Async: Hasher: SecurityProvider](
    index: Int,
    stake: L0GenesisDelegatedStake,
    operators: Set[PeerId],
    activationOrdinal: Long
  ): F[(Address, DelegatedStakeRecord, Signed[TokenLock])] = {
    val label = s"delegatedStakes[$index]"
    val event = stake.signedEvent.value

    for {
      eventProofId <- validateSignedBySource[F, UpdateDelegatedStake.Create](s"$label event", stake.signedEvent, event.source)
      _ <- requireEconomic[F](event.amount.value.value > 0L, s"$label amount must be positive")
      _ <- requireEconomic[F](event.fee.value.value === 0L, s"$label event fee must be zero")
      _ <- requireEconomic[F](
        event.parent === io.constellationnetwork.schema.delegatedStake.DelegatedStakeReference.empty,
        s"$label event parent must be empty"
      )
      _ <- requireEconomic[F](operators(event.nodeId), s"$label targets an operator outside the canonical genesis registry")
      lockProofId <- validateBackingLock[F](
        label,
        event.source,
        event.amount.value.value,
        event.tokenLockRef,
        stake.signedBackingTokenLock
      )
      _ <- requireEconomic[F](eventProofId === lockProofId, s"$label event and backing lock must have the exact same signer")
      createdAt <- nonNegative[F](s"$label.createdAt", stake.createdAt)
      _ <- requireEconomic[F](
        stake.createdAt === activationOrdinal,
        s"$label.createdAt must equal activationOrdinal $activationOrdinal"
      )
      _ <- requireEconomic[F](stake.rewards === 0L, s"$label.rewards must be zero at genesis")
      rewards <- nonNegative[F](s"$label.rewards", stake.rewards)
      record = DelegatedStakeRecord(stake.signedEvent, SnapshotOrdinal(createdAt), Amount(rewards))
    } yield (event.source, record, stake.signedBackingTokenLock)
  }

  private def validateCollateral[F[_]: Async: Hasher: SecurityProvider](
    index: Int,
    collateral: L0GenesisNodeCollateral,
    operators: Set[PeerId],
    activationOrdinal: Long
  ): F[(Address, NodeCollateralRecord, Signed[TokenLock])] = {
    val label = s"nodeCollaterals[$index]"
    val event = collateral.signedEvent.value

    for {
      eventProofId <- validateSignedBySource[F, UpdateNodeCollateral.Create](s"$label event", collateral.signedEvent, event.source)
      _ <- requireEconomic[F](event.amount.value.value > 0L, s"$label amount must be positive")
      _ <- requireEconomic[F](event.fee.value.value === 0L, s"$label event fee must be zero")
      _ <- requireEconomic[F](
        event.parent === io.constellationnetwork.schema.nodeCollateral.NodeCollateralReference.empty,
        s"$label event parent must be empty"
      )
      _ <- requireEconomic[F](operators(event.nodeId), s"$label targets an operator outside the canonical genesis registry")
      lockProofId <- validateBackingLock[F](
        label,
        event.source,
        event.amount.value.value,
        event.tokenLockRef,
        collateral.signedBackingTokenLock
      )
      _ <- requireEconomic[F](eventProofId === lockProofId, s"$label event and backing lock must have the exact same signer")
      createdAt <- nonNegative[F](s"$label.createdAt", collateral.createdAt)
      _ <- requireEconomic[F](
        collateral.createdAt === activationOrdinal,
        s"$label.createdAt must equal activationOrdinal $activationOrdinal"
      )
      record = NodeCollateralRecord(collateral.signedEvent, SnapshotOrdinal(createdAt))
    } yield (event.source, record, collateral.signedBackingTokenLock)
  }

  /** Augment a base `GlobalSnapshotInfo` (from `GlobalSnapshotInfoV1.toGlobalSnapshotInfo`) with the delegated-stake records,
    * node-collateral records, exact backing locks, and balances declared in an L0 genesis fixture.
    */
  def augmentSnapshotInfo[F[_]: Async: HasherSelector: SecurityProvider](
    base: GlobalSnapshotInfo,
    data: L0GenesisData
  ): F[GlobalSnapshotInfo] =
    requireEconomic[F](
      data.activationOrdinal === SnapshotOrdinal.MinValue.value.value,
      s"activationOrdinal must equal the greenfield genesis ordinal ${SnapshotOrdinal.MinValue.value.value}"
    ) >> nonNegative[F]("activationOrdinal", data.activationOrdinal).flatMap { activationOrdinal =>
      HasherSelector[F].forOrdinal(SnapshotOrdinal(activationOrdinal).next) { selectedHasher =>
        Async[F]
          .raiseError[Unit](
            invalidEconomicGenesis(
              s"the public signed-bundle schema is greenfield-current-hash-only, but first live ordinal " +
                s"${SnapshotOrdinal(activationOrdinal).next.value.value} selects ${selectedHasher.getLogic(SnapshotOrdinal(activationOrdinal).next)}"
            )
          )
          .unlessA(selectedHasher.getLogic(SnapshotOrdinal(activationOrdinal).next) == JsonHash) >> {
          implicit val hasher: Hasher[F] = selectedHasher
          augmentSnapshotInfoAtActivationHasher(base, data)
        }
      }
    }

  private def augmentSnapshotInfoAtActivationHasher[F[_]: Async: Hasher: SecurityProvider](
    base: GlobalSnapshotInfo,
    data: L0GenesisData
  ): F[GlobalSnapshotInfo] =
    for {
      genesisOperatorKeys <- buildGenesisOperatorKeys[F](data)
      allocation <- data.validatedGenesisAllocation
        .leftMap(invalidEconomicGenesis)
        .liftTo[F]
      (initialBalances, _) = allocation
      stakeTriples <- data.delegatedStakes.zipWithIndex.traverse {
        case (stake, index) =>
          validateStake[F](index, stake, genesisOperatorKeys.keySet, data.activationOrdinal)
      }
      collateralTriples <- data.nodeCollaterals.zipWithIndex.traverse {
        case (collateral, index) =>
          validateCollateral[F](index, collateral, genesisOperatorKeys.keySet, data.activationOrdinal)
      }
      duplicateStakeSources =
        stakeTriples.groupBy(_._1).collect { case (source, records) if records.sizeCompare(1) > 0 => source }.toList
      _ <- requireEconomic[F](
        duplicateStakeSources.isEmpty,
        s"multiple empty-parent delegated-stake records for source(s): ${duplicateStakeSources.map(_.value.value).sorted.mkString(",")}"
      )
      duplicateCollateralSources =
        collateralTriples.groupBy(_._1).collect { case (source, records) if records.sizeCompare(1) > 0 => source }.toList
      _ <- requireEconomic[F](
        duplicateCollateralSources.isEmpty,
        s"multiple empty-parent node-collateral records for source(s): ${duplicateCollateralSources.map(_.value.value).sorted.mkString(",")}"
      )
      allTriples = stakeTriples ++ collateralTriples
      duplicateBackingSources =
        allTriples.groupBy(_._1).collect { case (source, records) if records.sizeCompare(1) > 0 => source }.toList
      _ <- requireEconomic[F](
        duplicateBackingSources.isEmpty,
        s"multiple parallel empty-parent backing locks for source(s): ${duplicateBackingSources.map(_.value.value).sorted.mkString(",")}"
      )
      stakeMap = SortedMap.from(
        stakeTriples
          .groupBy(_._1)
          .view
          .mapValues(_.map(_._2).to(SortedSet))
      )
      collMap = SortedMap.from(
        collateralTriples
          .groupBy(_._1)
          .view
          .mapValues(_.map(_._2).to(SortedSet))
      )
      activeLocks = SortedMap.from(
        allTriples
          .groupBy(_._1)
          .view
          .mapValues(_.map(_._3).to(SortedSet))
      )
      lastTokenLockRefs <- allTriples.traverse {
        case (source, _, lock) =>
          TokenLockReference.of[F](lock).map(source -> _)
      }.map(pairs => SortedMap.from[Address, TokenLockReference](pairs))
      _ <- StakeBackingValidator.validateActiveState[F](activeLocks, stakeMap, collMap)
      mergedBalances = initialBalances.foldLeft(base.balances) {
        case (acc, (address, balance)) =>
          acc.updated(address, balance)
      }
    } yield
      base.copy(
        balances = mergedBalances,
        activeTokenLocks = Some(activeLocks),
        lastTokenLockRefs = Some(lastTokenLockRefs),
        activeDelegatedStakes = Some(stakeMap),
        activeNodeCollaterals = Some(collMap),
        genesisOperatorKeys = genesisOperatorKeys
      )

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
      _ <- Async[F]
        .raiseError[Unit](
          invalidGenesis(
            s"activationOrdinal must equal the greenfield genesis ordinal ${SnapshotOrdinal.MinValue.value.value}"
          )
        )
        .unlessA(data.activationOrdinal === SnapshotOrdinal.MinValue.value.value)
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
