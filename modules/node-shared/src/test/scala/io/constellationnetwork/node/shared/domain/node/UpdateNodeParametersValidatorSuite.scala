package io.constellationnetwork.node.shared.domain.node

import cats.data.NonEmptySet
import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits.{catsSyntaxOptionId, catsSyntaxValidatedIdBinCompat0}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.node.UpdateNodeParametersValidator.InvalidSigned
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.node._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.SignedValidator.{InvalidSignatures, NotSignedExclusivelyByAddressOwner}
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.unpRecordImmutableCodec
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.types.numeric.PosInt
import weaver.MutableIOSuite

object UpdateNodeParametersValidatorSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (j, h, sp)

  def testUpdateNodeParameters(source: Address, name: String = "name", description: String = "description"): UpdateNodeParameters =
    UpdateNodeParameters(
      source = source,
      delegatedStakeRewardParameters = DelegatedStakeRewardParameters(
        rewardFraction = RewardFraction(5_000_000)
      ),
      nodeMetadataParameters = NodeMetadataParameters(
        name = name,
        description = description
      ),
      parent = UpdateNodeParametersReference.empty
    )

  test("should succeed when the node parameters are signed correctly and the reward value is at the lower bound") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source)
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(Valid(signedUpdateNodeParameters), result)
  }

  test("should succeed when the node parameters are signed correctly and the reward value is at the upper bound") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source).copy(delegatedStakeRewardParameters =
        DelegatedStakeRewardParameters(
          rewardFraction = RewardFraction(10_000_000)
        )
      )
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(Valid(signedUpdateNodeParameters), result)
  }

  test("should succeed when the node parameters are signed correctly and the reward value is within the allowed range") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source).copy(delegatedStakeRewardParameters =
        DelegatedStakeRewardParameters(
          rewardFraction = RewardFraction(8_000_000)
        )
      )
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(Valid(signedUpdateNodeParameters), result)
  }

  test("should fail when the node is absent in seed list") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source)
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set.empty)
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.NodeNotInSeedList(peerId).invalidNec, result)
  }

  test("should fail when the signature is wrong") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair1 <- KeyPairGenerator.makeKeyPair[IO]
      keyPair2 <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair1.getPublic.toAddress
      peerId1 = PeerId.fromId(keyPair1.getPublic.toId)
      peerId2 = PeerId.fromId(keyPair2.getPublic.toId)
      signedUpdateNodeParameters <- forAsyncHasher(testUpdateNodeParameters(source), keyPair1).map(signed =>
        signed.copy(proofs =
          NonEmptySet.fromSetUnsafe(
            SortedSet(signed.proofs.head.copy(id = keyPair2.getPublic.toId))
          )
        )
      )
      validator = mkValidator(Set(peerId1, peerId2))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield
      expect.all(result match {
        case Invalid(errors) =>
          errors.exists {
            case InvalidSigned(InvalidSignatures(signedUpdateNodeParameters.proofs)) => true
            case InvalidSigned(NotSignedExclusivelyByAddressOwner)                   => true
            case _                                                                   => false
          }
        case _ => false
      })
  }

  test("should fail when the reward value is too high") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTtestUpdateNodeParameters = testUpdateNodeParameters(source).copy(delegatedStakeRewardParameters =
        DelegatedStakeRewardParameters(
          rewardFraction = RewardFraction(10_000_001)
        )
      )
      signedUpdateNodeParameters <- forAsyncHasher(invalidTtestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidRewardValue(10_000_001).invalidNec, result)
  }

  test("should fail when the reward value is too low") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTtestUpdateNodeParameters = testUpdateNodeParameters(source).copy(delegatedStakeRewardParameters =
        DelegatedStakeRewardParameters(
          rewardFraction = RewardFraction(4_999_999)
        )
      )
      signedUpdateNodeParameters <- forAsyncHasher(invalidTtestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidRewardValue(4_999_999).invalidNec, result)
  }

  test("should succeed when lastRef is empty and the global context is empty") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source)
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(Valid(signedUpdateNodeParameters), result)
  }

  test("should fail when lastRef is not empty and the global context is empty") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      lastRef = UpdateNodeParametersReference(UpdateNodeParametersReference.empty.ordinal.next, UpdateNodeParametersReference.empty.hash)
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTestUpdateNodeParameters = testUpdateNodeParameters(source).copy(parent = lastRef)
      signedUpdateNodeParameters <- forAsyncHasher(invalidTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidParent(lastRef).invalidNec, result)
  }

  test("should succeed when lastRef is not empty and the global context contains the parent") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      parent = testUpdateNodeParameters(source)
      signedParent <- forAsyncHasher(parent, keyPair)
      context = mkGlobalContext(SortedMap(signedParent.proofs.head.id -> (signedParent, SnapshotOrdinal.MinValue)))
      lastRef <- h.hash(parent).map(hash => UpdateNodeParametersReference(parent.ordinal, hash))
      validTestUpdateNodeParameters = testUpdateNodeParameters(source).copy(parent = lastRef)
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, context)
    } yield expect.same(Valid(signedUpdateNodeParameters), result)
  }

  test("should fail when lastRef is not empty and the global context does not contain the parent") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      parent = testUpdateNodeParameters(source)
      signedParent <- forAsyncHasher(parent, keyPair)
      context = mkGlobalContext(SortedMap(signedParent.proofs.head.id -> (signedParent, SnapshotOrdinal.MinValue)))
      lastRef <- h.hash(parent).map(hash => UpdateNodeParametersReference(parent.ordinal.next, hash))
      invalidTestUpdateNodeParameters = testUpdateNodeParameters(source).copy(parent = lastRef)
      signedUpdateNodeParameters <- forAsyncHasher(invalidTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, context)
    } yield expect.same(UpdateNodeParametersValidator.InvalidParent(lastRef).invalidNec, result)
  }

  test("should fail when the metadata name is greater than config 'maxMetadataFieldsChars'") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source)
      invalidName =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus efficitur, nisl eu volutpat feugiat, urna sapien pretium justo, ac mollis nulla."
      updatedParametersInvalidName = validTestUpdateNodeParameters.copy(nodeMetadataParameters =
        NodeMetadataParameters(
          name = invalidName,
          description = "VALID DESCRIPTION"
        )
      )
      signedUpdateNodeParameters <- forAsyncHasher(updatedParametersInvalidName, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.TooLargeName(invalidName, 140).invalidNec, result)
  }

  test("should fail when the metadata description is greater than config 'maxMetadataFieldsChars'") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source)
      invalidDescription =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus efficitur, nisl eu volutpat feugiat, urna sapien pretium justo, ac mollis nulla."
      updatedParametersInvalidName = validTestUpdateNodeParameters.copy(nodeMetadataParameters =
        NodeMetadataParameters(
          name = "Valid Name",
          description = invalidDescription
        )
      )
      signedUpdateNodeParameters <- forAsyncHasher(updatedParametersInvalidName, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.TooLargeDescription(invalidDescription, 140).invalidNec, result)
  }

  test("should succeed when name and description contains printable ASCII characters") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "Simple Node Name", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(Valid(signedUpdateNodeParameters), result)
  }

  test("should succeed when name contains alphanumeric and punctuation") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "Node-123_Test!@#$%", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(Valid(signedUpdateNodeParameters), result)
  }

  test("should succeed when name contains HTML special characters") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "Node <Name> & More", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(Valid(signedUpdateNodeParameters), result)
  }

  test("should fail when name contains ASCII characters mixed with non-ASCII") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "Name όνομα", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidName("Name όνομα").invalidNec, result)
  }

  test("should fail when name is empty") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(invalidTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidName("").invalidNec, result)
  }

  test("should fail when description is empty") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "Valid name", description = "")
      signedUpdateNodeParameters <- forAsyncHasher(invalidTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidDescription("").invalidNec, result)
  }

  test("should fail when name contains only control characters") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "\t\n\r", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(invalidTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidName("\t\n\r").invalidNec, result)
  }

  test("should fail when name contains only DEL character") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "\u007F", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(invalidTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidName("\u007F").invalidNec, result)
  }

  test("should fail when name contains only non-ASCII characters") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "όνομα", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(invalidTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidName("όνομα").invalidNec, result)
  }

  test("should fail when description contains only non-ASCII characters") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "Valid name", description = "περιγραφή")
      signedUpdateNodeParameters <- forAsyncHasher(invalidTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidDescription("περιγραφή").invalidNec, result)
  }

  test("should fail when name contains only null character") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      invalidTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "\u0000", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(invalidTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidName("\u0000").invalidNec, result)
  }

  test("should succeed when name starts with control character but contains printable ASCII") { res =>
    implicit val (json, h, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peerId = PeerId.fromId(keyPair.getPublic.toId)
      validTestUpdateNodeParameters = testUpdateNodeParameters(source, name = "\tNode Name", description = "Valid description")
      signedUpdateNodeParameters <- forAsyncHasher(validTestUpdateNodeParameters, keyPair)
      validator = mkValidator(Set(peerId))
      result <- validator.validate(signedUpdateNodeParameters, mkGlobalContext())
    } yield expect.same(UpdateNodeParametersValidator.InvalidName("\tNode Name").invalidNec, result)
  }

  private def mkValidator(peersList: Set[PeerId])(
    implicit S: SecurityProvider[IO],
    J: JsonSerializer[IO],
    H: Hasher[IO]
  ): UpdateNodeParametersValidator[IO] = {
    val seedList = peersList.map(peerId => SeedlistEntry(peerId, None, None, None, None))
    val signedValidator = SignedValidator.make[IO]
    UpdateNodeParametersValidator.make[IO](
      signedValidator,
      RewardFraction(5_000_000),
      RewardFraction(10_000_000),
      PosInt(140),
      seedList.some
    )
  }

  def mkGlobalContext(updateNodeParameters: SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)] = SortedMap.empty) =
    GlobalSnapshotInfo.empty.copy(updateNodeParameters = Some(updateNodeParameters))

  test("mpt path: validateParent agrees with legacy when MPT state matches lastSnapshotContext") { res =>
    implicit val (json, h, sp) = res
    val seedList = Set.empty[SeedlistEntry]
    val signedValidator = SignedValidator.make[IO]

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      id = keyPair.getPublic.toId

      // Existing params in prior state
      existingParams = testUpdateNodeParameters(source, name = "prior", description = "prior")
      signedExisting <- forAsyncHasher(existingParams, keyPair)
      priorRef <- UpdateNodeParametersReference.of(signedExisting)

      // New params that reference the existing as parent
      newParams = testUpdateNodeParameters(source).copy(parent = priorRef)
      signedNew <- forAsyncHasher(newParams, keyPair)

      priorMap = SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)](
        id -> ((signedExisting, SnapshotOrdinal.MinValue))
      )
      context = mkGlobalContext(priorMap)

      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      key <- GlobalStateKey.updateNodeParametersKey[IO](id)
      _ <- mptStore.insert[(Signed[UpdateNodeParameters], SnapshotOrdinal)](key, (signedExisting, SnapshotOrdinal.MinValue))

      legacyValidator = UpdateNodeParametersValidator.make[IO](
        signedValidator,
        RewardFraction(5_000_000),
        RewardFraction(10_000_000),
        PosInt(140),
        seedList.some,
        mptStore = Some(mptStore),
        shouldUseMptStore = false
      )
      mptValidator = UpdateNodeParametersValidator.make[IO](
        signedValidator,
        RewardFraction(5_000_000),
        RewardFraction(10_000_000),
        PosInt(140),
        seedList.some,
        mptStore = Some(mptStore),
        shouldUseMptStore = true
      )

      legacyResult <- legacyValidator.validate(signedNew, context)
      mptResult <- mptValidator.validate(signedNew, context)
    } yield expect(legacyResult.isValid == mptResult.isValid)
  }
}
