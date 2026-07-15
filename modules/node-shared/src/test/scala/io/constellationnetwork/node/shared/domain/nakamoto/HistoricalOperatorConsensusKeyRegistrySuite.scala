package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptySet
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot, StakeDistribution}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, HasherSelector}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object HistoricalOperatorConsensusKeyRegistrySuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], HasherSelector[IO])

  def sharedResource: Resource[IO, Res] =
    JsonSerializer.forAsync[IO].asResource.map { implicit json =>
      val hasher = Hasher.forJson[IO]
      (json, hasher, HasherSelector.forSyncAlwaysCurrent[IO](hasher))
    }

  private val R = 10L

  private def peer(byte: String): PeerId = PeerId(Hex(byte * 64))
  private def hash(byte: String): Hash = Hash(byte * 32)
  private def ordinal(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))

  private def genesisKeys(operator: PeerId, kesByte: Byte = 0x10, vrfByte: Byte = 0x20): OperatorConsensusKeys =
    OperatorConsensusKeys(
      operator,
      KesRegistryEntry(VerificationKeyKesProduct(Array.fill(32)(kesByte), 0), 0L),
      VrfPublicKey.fromBytes(Array.fill(32)(vrfByte)),
      EtaPeriod.Zero,
      None
    )

  private def registry(entries: (PeerId, OperatorConsensusKeys)*): OperatorConsensusKeyRegistry[IO] =
    OperatorConsensusKeyRegistry.make[IO](entries.toMap)

  private def record(
    operator: PeerId,
    acceptedAt: Long,
    effectiveAt: Long,
    kesByte: String,
    vrfByte: String,
    registrationParent: Hash,
    registrationOrdinal: Long = 1L,
    parent: KesRegistrationReference = KesRegistrationReference.empty
  ): KesRegistrationRecord = {
    val cert = KesRegistrationCert(
      operatorPeerId = operator,
      kesMasterVK = Hex(kesByte * 32),
      kesMasterVKStep = 0,
      offset = effectiveAt,
      vrfPublicKey = Hex(vrfByte * 32),
      effectiveFromPeriod = EtaPeriod(effectiveAt),
      registrationParentHash = registrationParent,
      ordinal = KesRegistrationOrdinal(NonNegLong.unsafeFrom(registrationOrdinal)),
      parent = parent
    )
    val proof = SignatureProof(Id(operator.value), Signature(Hex("7f" * 64)))
    KesRegistrationRecord(Signed(cert, NonEmptySet.one(proof)), ordinal(acceptedAt))
  }

  private def reference(record: KesRegistrationRecord)(implicit hasher: Hasher[IO]): IO[KesRegistrationReference] =
    KesRegistrationReference.of[IO](record.event)

  private def view(
    parentHash: Hash,
    parentOrdinal: Long,
    histories: SortedMap[PeerId, SortedSet[KesRegistrationRecord]] = SortedMap.empty,
    historicalStakes: SortedMap[EtaPeriod, HistoricalStakeSnapshot] = SortedMap.empty,
    pointerOverrides: Option[SortedMap[PeerId, KesRegistrationReference]] = None
  )(implicit hasher: Hasher[IO]): IO[HistoricalOperatorRegistryView] =
    pointerOverrides
      .fold(
        histories.toList.traverse {
          case (operator, records) =>
            reference(records.last).map(operator -> _)
        }.map(_.to(SortedMap))
      )(_.pure[IO])
      .map { pointers =>
        HistoricalOperatorRegistryView(
          parentHash,
          ordinal(parentOrdinal),
          GlobalSnapshotInfo.empty.copy(
            historicalStakeSnapshots = historicalStakes,
            kesRegistrationCerts = histories,
            lastKesRegistrationRefs = pointers
          )
        )
      }

  private def viewSource(
    available: Map[Hash, HistoricalOperatorRegistryView]
  ): HistoricalOperatorRegistryViewSource[IO] = new HistoricalOperatorRegistryViewSource[IO] {
    def get(candidateParent: Hash): IO[Option[HistoricalOperatorRegistryView]] = IO.pure(available.get(candidateParent))
  }

  private def rosterSource(
    available: Map[(Hash, EtaPeriod), CanonicalOperatorRosterSnapshot]
  ): CanonicalOperatorRosterSource[IO] = new CanonicalOperatorRosterSource[IO] {
    def get(candidateParent: Hash, period: EtaPeriod): IO[Option[CanonicalOperatorRosterSnapshot]] =
      IO.pure(available.get(candidateParent -> period))
  }

  private def resolver(
    genesis: OperatorConsensusKeyRegistry[IO],
    views: Map[Hash, HistoricalOperatorRegistryView],
    genesisOperators: SortedSet[PeerId] = SortedSet.empty,
    genesisStakes: StakeDistribution = StakeDistribution.Empty,
    rosters: CanonicalOperatorRosterSource[IO] = CanonicalOperatorRosterSource.unavailable[IO]
  )(implicit selector: HasherSelector[IO]): HistoricalOperatorConsensusKeyRegistry[IO] =
    HistoricalOperatorConsensusKeyRegistry.make[IO](
      genesis,
      CanonicalGenesisOperatorPopulation(genesisOperators, genesisStakes),
      viewSource(views),
      rosters,
      R
    )

  test("missing exact candidate-parent history returns unavailable and never falls back to genesis") { res =>
    implicit val (_, _, selector) = res
    val operator = peer("01")
    val missing = hash("aa")
    val subject = resolver(registry(operator -> genesisKeys(operator)), Map.empty)

    subject.activeKeysAt(operator, missing, EtaPeriod.Zero).map { result =>
      expect(result == Left(CandidateParentRegistryUnavailable(missing)))
    }
  }

  test("sibling candidate branches resolve their own preregistered key rotation") { res =>
    implicit val (_, hasher, selector) = res
    val operator = peer("01")
    val parentA = hash("aa")
    val parentB = hash("bb")
    val rotationA = record(operator, 9L, 2L, "31", "41", hash("a1"))
    val rotationB = record(operator, 9L, 2L, "32", "42", hash("b1"))

    for {
      viewA <- view(parentA, 25L, SortedMap(operator -> SortedSet(rotationA)))
      viewB <- view(parentB, 25L, SortedMap(operator -> SortedSet(rotationB)))
      subject = resolver(registry(operator -> genesisKeys(operator)), Map(parentA -> viewA, parentB -> viewB))
      atA <- subject.activeKeysAt(operator, parentA, EtaPeriod(2L))
      atB <- subject.activeKeysAt(operator, parentB, EtaPeriod(2L))
    } yield
      expect(atA.toOption.flatten.exists(_.vrfPublicKey.toBytes.sameElements(Hex("41" * 32).toBytes))) &&
        expect(atB.toOption.flatten.exists(_.vrfPublicKey.toBytes.sameElements(Hex("42" * 32).toBytes)))
  }

  test("N-2 cutoff holds rotations pending and changes both KES and VRF atomically at the boundary") { res =>
    implicit val (_, hasher, selector) = res
    val operator = peer("01")
    val parentHash = hash("aa")
    val first = record(operator, 9L, 2L, "31", "41", hash("a1"))

    for {
      firstRef <- reference(first)
      second = record(operator, 19L, 3L, "32", "42", hash("a2"), 2L, firstRef)
      parentView <- view(parentHash, 35L, SortedMap(operator -> SortedSet(first, second)))
      subject = resolver(registry(operator -> genesisKeys(operator)), Map(parentHash -> parentView))
      periodOne <- subject.activeKeysAt(operator, parentHash, EtaPeriod(1L))
      periodTwo <- subject.activeKeysAt(operator, parentHash, EtaPeriod(2L))
      periodThree <- subject.activeKeysAt(operator, parentHash, EtaPeriod(3L))
    } yield
      expect(periodOne.toOption.flatten.exists(_.registration.isEmpty)) &&
        expect(periodTwo.toOption.flatten.exists(_.kes.vk.value.sameElements(Hex("31" * 32).toBytes))) &&
        expect(periodTwo.toOption.flatten.exists(_.vrfPublicKey.toBytes.sameElements(Hex("41" * 32).toBytes))) &&
        expect(periodThree.toOption.flatten.exists(_.kes.vk.value.sameElements(Hex("32" * 32).toBytes))) &&
        expect(periodThree.toOption.flatten.exists(_.vrfPublicKey.toBytes.sameElements(Hex("42" * 32).toBytes)))
  }

  test("a backdated rotation in rooted history corrupts the whole parent view") { res =>
    implicit val (_, hasher, selector) = res
    val operator = peer("01")
    val parentHash = hash("aa")
    // Accepted in period 2, so period 3 is too early: minimum activation is period 4.
    val backdated = record(operator, 29L, 3L, "31", "41", hash("a1"))

    for {
      parentView <- view(parentHash, 35L, SortedMap(operator -> SortedSet(backdated)))
      subject = resolver(registry(operator -> genesisKeys(operator)), Map(parentHash -> parentView))
      result <- subject.activeKeysAt(operator, parentHash, EtaPeriod(3L))
    } yield expect(result.swap.exists(_.isInstanceOf[CorruptHistoricalOperatorRegistry]))
  }

  test("records outside the exact pointer-selected chain fail closed") { res =>
    implicit val (_, hasher, selector) = res
    val operator = peer("01")
    val parentHash = hash("aa")
    val selected = record(operator, 9L, 2L, "31", "41", hash("a1"))
    val orphan = record(operator, 8L, 2L, "32", "42", hash("a2"))

    for {
      selectedRef <- reference(selected)
      parentView <- view(
        parentHash,
        25L,
        SortedMap(operator -> SortedSet(selected, orphan)),
        pointerOverrides = Some(SortedMap(operator -> selectedRef))
      )
      subject = resolver(registry(operator -> genesisKeys(operator)), Map(parentHash -> parentView))
      result <- subject.activeKeyPairsAt(parentHash, EtaPeriod(2L))
    } yield expect(result.swap.exists(_.isInstanceOf[CorruptHistoricalOperatorRegistry]))
  }

  test("eligible population intersects active pairs with separately authenticated N-2 roster and stakes") { res =>
    implicit val (_, hasher, selector) = res
    val anchored = peer("01")
    val keyOnly = peer("02")
    val parentHash = hash("aa")
    val keyOnlyRegistration = record(keyOnly, 9L, 2L, "32", "42", hash("a1"))
    val stakes = StakeDistribution(SortedMap(anchored -> BigInt(100), keyOnly -> BigInt(100)))
    val historical = SortedMap(EtaPeriod.Zero -> HistoricalStakeSnapshot(stakes, hash("ee")))
    val roster = CanonicalOperatorRosterSnapshot(parentHash, EtaPeriod.Zero, SortedSet(anchored))

    for {
      parentView <- view(parentHash, 25L, SortedMap(keyOnly -> SortedSet(keyOnlyRegistration)), historical)
      subject = resolver(
        registry(anchored -> genesisKeys(anchored)),
        Map(parentHash -> parentView),
        rosters = rosterSource(Map((parentHash -> EtaPeriod.Zero) -> roster))
      )
      activePairs <- subject.activeKeyPairsAt(parentHash, EtaPeriod(2L))
      eligible <- subject.eligibleOperatorsAt(parentHash, EtaPeriod(2L))
    } yield
      expect(activePairs.toOption.exists(_.keys.keySet == Set(anchored, keyOnly))) &&
        expect(eligible.toOption.exists(_.operators.keySet == Set(anchored))) &&
        expect(eligible.toOption.exists(_.stakes.stakes == SortedMap(anchored -> BigInt(100))))
  }

  test("rooted stake without a canonical N-2 operator roster is unavailable, not inferred from key owners") { res =>
    implicit val (_, hasher, selector) = res
    val operator = peer("01")
    val parentHash = hash("aa")
    val stakes = StakeDistribution(SortedMap(operator -> BigInt(100)))
    val historical = SortedMap(EtaPeriod.Zero -> HistoricalStakeSnapshot(stakes, hash("ee")))

    for {
      parentView <- view(parentHash, 25L, historicalStakes = historical)
      subject = resolver(registry(operator -> genesisKeys(operator)), Map(parentHash -> parentView))
      result <- subject.eligibleOperatorsAt(parentHash, EtaPeriod(2L))
    } yield expect(result == Left(CanonicalOperatorRosterUnavailable(parentHash, EtaPeriod.Zero)))
  }
}
