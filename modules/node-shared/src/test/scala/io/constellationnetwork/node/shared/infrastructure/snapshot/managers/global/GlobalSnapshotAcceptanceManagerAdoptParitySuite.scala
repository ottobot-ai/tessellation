package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.security.KeyPair

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.domain.statechannel._
import io.constellationnetwork.node.shared.infrastructure.snapshot.CurrencySnapshotContextFunctions
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Axis 1a (#259) — adopt-path byte-exactness + stall-bypass parity.
  *
  * Proves the load-bearing #259 claim that the GSAM adopt path is equivalent to re-execution for the SAME binaries, while bypassing the
  * chain-link check that causes the token-lock stall. The two GSAM code paths are exercised over a REAL
  * [[GlobalSnapshotStateChannelEventsProcessor]] (real `GlobalSnapshotStateChannelAcceptanceManager` chain-link; permissive validator):
  *
  *   - '''Legacy / standard path''' = `processor.process(...)` — runs `onlyPossibleReferences` (chain-link) then the currency derivation.
  *     This is exactly what GSAM runs on the raw `scEvents`.
  *   - '''Adopt path''' = `processor.processCurrencySnapshots(adopted) + calculateLastCurrencySnapshots` — the EXACT tail GSAM's
  *     `deriveAdoptedCurrencyState` runs, with the chain-link stage replaced by the committee-attested `adoptedScSnapshots`. The
  *     `assembleAdoptResult` helper below replicates GSAM's replication line-for-line so the test and production share one derivation.
  *
  * Two scenarios:
  *   1. '''Stall vs advance''': a genesis binary whose `lastSnapshotHash = Hash.empty` does NOT chain to gl0's FROZEN tip (a non-empty
  *      stored hash). The legacy chain-link path yields an empty `accepted` for that MG (the #259 stall — the binary is `returned`); the
  *      adopt path yields `accepted(mg) == includedSnapshots(mg)` and the currency state advances.
  *   1. '''Happy-path equivalence''': the same genesis binary against a FRESH gl0 tip (`Hash.empty`) chains successfully under BOTH paths,
  *      and the two `StateChannelAcceptanceResult`s are byte-equal — confirming the adopt path introduces NO divergence on the path the
  *      legacy code already handled.
  *
  * A genesis full `CurrencySnapshot` binary (the `Left` case) is used deliberately: `processCurrencySnapshots` derives its state via the
  * pure `deserialize[Signed[CurrencySnapshot]]` branch and never calls `applyCurrencySnapshot`, so a no-op `CurrencySnapshotContextFunctions`
  * stub suffices and the derivation stays a pure function of the binary (the byte-identity premise).
  */
object GlobalSnapshotAcceptanceManagerAdoptParitySuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))
  private val frozenTip: Hash = Hash("ff" * 32) // a non-empty stored gl0 tip the genesis binary cannot chain to (lastSnapshotHash=empty)

  /** Build the REAL processor: real chain-link `GlobalSnapshotStateChannelAcceptanceManager` (pull/purge delay 0 ⇒ first-sight is
    * immediately pullable), a permissive `StateChannelValidator` (every output valid), a no-op `CurrencySnapshotContextFunctions` (only
    * reached for 2nd+ incrementals — never for a genesis full snapshot), `FeeCalculator` with empty config (`isFeeRequired = false`), and a
    * `GlobalStateReader` over an empty in-memory MptStore (only consulted for non-genesis fee lookups, which the genesis path skips).
    */
  private def mkProcessor(
    implicit h: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[GlobalSnapshotStateChannelEventsProcessor[IO]] = {
    val validator = new StateChannelValidator[IO] {
      def validate(
        output: StateChannelOutput,
        globalOrdinal: SnapshotOrdinal,
        snapshotFeesInfo: SnapshotFeesInfo
      )(implicit hasher: Hasher[IO]) = IO.pure(output.validNec)
      def validateHistorical(
        output: StateChannelOutput,
        globalOrdinal: SnapshotOrdinal,
        snapshotFeesInfo: SnapshotFeesInfo
      )(implicit hasher: Hasher[IO]) = IO.pure(output.validNec)
    }

    // Genesis full-snapshot binaries never reach `applyCurrencySnapshot`; this stub is therefore unreachable in this suite.
    val noopContextFns: CurrencySnapshotContextFunctions[IO] = new CurrencySnapshotContextFunctions[IO] {
      def createContext(
        context: CurrencySnapshotContext,
        lastArtifact: Signed[CurrencyIncrementalSnapshot],
        signedArtifact: Signed[CurrencyIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[CurrencySnapshotContext] =
        IO.raiseError(new RuntimeException("noop CurrencySnapshotContextFunctions should not be reached for genesis binaries"))
    }

    for {
      manager <- GlobalSnapshotStateChannelAcceptanceManager.make[IO](None, pullDelay = NonNegLong.MinValue, purgeDelay = NonNegLong.MinValue)
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      reader = io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(mptStore)
      feeCalculator = FeeCalculator.make[IO](SortedMap.empty)
      processor = GlobalSnapshotStateChannelEventsProcessor.make[IO](validator, manager, noopContextFns, feeCalculator, reader)
    } yield processor
  }

  /** Build a genesis full `CurrencySnapshot` SC binary for `mgKeyPair` (lastSnapshotHash = Hash.empty). Mirrors
    * `ShardCommitteeReExecutionSuite.mkCurrencyGenesisBinary` — the exact shape `processCurrencySnapshots` decodes.
    */
  private def mkCurrencyGenesisBinary(
    mgKeyPair: KeyPair
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], j: JsonSerializer[IO]): IO[Signed[StateChannelSnapshotBinary]] = {
    val genesis: CurrencySnapshot = CurrencySnapshot.mkGenesis(Map.empty, None, None)
    for {
      signedGenesis <- forAsyncHasher(genesis, mgKeyPair)
      contentBytes <- JsonSerializer[IO].serialize(signedGenesis)
      binary = StateChannelSnapshotBinary(Hash.empty, contentBytes, SnapshotFee.MinValue)
      signedBinary <- forAsyncHasher(binary, mgKeyPair)
    } yield signedBinary
  }

  /** Exercises the EXACT production adopt assembly — GSAM's `deriveAdoptedCurrencyState` calls the SAME shared
    * `GlobalSnapshotStateChannelEventsProcessor.assembleAcceptanceResult` over the `processCurrencySnapshots` output. No hand-copied
    * replica (N1): the test and production share one assembly method, so they cannot drift.
    */
  private def assembleAdoptResult(
    processor: GlobalSnapshotStateChannelEventsProcessor[IO],
    currentBalances: SortedMap[Address, Balance],
    priorLastCurrencySnapshots: SortedMap[Address, CurrencySnapshotWithState],
    adoptedScSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  )(implicit h: Hasher[IO]): IO[StateChannelAcceptanceResult] =
    processor
      .processCurrencySnapshots(ordinal, currentBalances, priorLastCurrencySnapshots, adoptedScSnapshots, _ => None.pure[IO])
      .map(accepted => processor.assembleAcceptanceResult(accepted, priorLastCurrencySnapshots, Set.empty[StateChannelOutput]))

  // ===========================================================================
  // Scenario 1 — STALL vs ADVANCE: frozen gl0 tip ⇒ legacy chain-link stalls; adopt advances
  // ===========================================================================

  test("frozen gl0 tip: legacy chain-link yields empty accepted (the #259 stall); adopt path adopts the binary + advances") { res =>
    implicit val (h, sp, j) = res
    for {
      processor <- mkProcessor
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = PublicKeyOps(mgKeyPair.getPublic).toAddress
      binary <- mkCurrencyGenesisBinary(mgKeyPair)

      scOutput = StateChannelOutput(mgAddr, binary)
      adopted = SortedMap(mgAddr -> NonEmptyList.of(binary))(Address.OrderingInstance)

      // gl0's stored tip for this MG is FROZEN at a non-empty hash that the genesis binary (lastSnapshotHash = empty) cannot chain to.
      frozenPriorHashes = SortedMap(mgAddr -> frozenTip)(Address.OrderingInstance)

      // LEGACY path: process() runs onlyPossibleReferences against the frozen tip ⇒ binary rejected, accepted is empty (the stall).
      legacy <- processor.process(
        ordinal,
        SortedMap.empty[Address, Balance],
        frozenPriorHashes,
        SortedMap.empty[Address, CurrencySnapshotWithState],
        List(scOutput),
        StateChannelValidationType.Full,
        _ => None.pure[IO]
      )

      // ADOPT path: bypass chain-link, derive currency state directly from the committee-attested binary.
      adopt <- assembleAdoptResult(processor, SortedMap.empty, SortedMap.empty, adopted)
    } yield
      expect.all(
        // Legacy stalls: the MG's binary did NOT make it into `accepted`, and was `returned` to the metagraph.
        !legacy.accepted.contains(mgAddr),
        legacy.returned.contains(scOutput),
        !legacy.calculatedCurrencyState.contains(mgAddr),
        // Adopt advances: the binary is accepted verbatim and the currency state moves forward.
        adopt.accepted.get(mgAddr) == Some(NonEmptyList.of(binary)),
        adopt.calculatedCurrencyState.contains(mgAddr),
        adopt.incomingCurrencySnapshotsWithState.contains(mgAddr)
      )
  }

  // ===========================================================================
  // Scenario 2 — HAPPY-PATH EQUIVALENCE: fresh gl0 tip ⇒ both paths produce byte-equal results
  // ===========================================================================

  test("fresh gl0 tip: legacy chain-link and adopt path produce byte-identical StateChannelAcceptanceResult") { res =>
    implicit val (h, sp, j) = res
    for {
      processor <- mkProcessor
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = PublicKeyOps(mgKeyPair.getPublic).toAddress
      binary <- mkCurrencyGenesisBinary(mgKeyPair)

      scOutput = StateChannelOutput(mgAddr, binary)
      adopted = SortedMap(mgAddr -> NonEmptyList.of(binary))(Address.OrderingInstance)

      // Fresh tip: no prior hash for this MG ⇒ defaults to Hash.empty ⇒ the genesis binary chains under the legacy path.
      legacy <- processor.process(
        ordinal,
        SortedMap.empty[Address, Balance],
        SortedMap.empty[Address, Hash],
        SortedMap.empty[Address, CurrencySnapshotWithState],
        List(scOutput),
        StateChannelValidationType.Full,
        _ => None.pure[IO]
      )

      adopt <- assembleAdoptResult(processor, SortedMap.empty, SortedMap.empty, adopted)
    } yield
      expect.all(
        // Sanity: the legacy path DID accept (so this is a genuine happy-path comparison, not "both empty").
        legacy.accepted.get(mgAddr) == Some(NonEmptyList.of(binary)),
        // The whole result is byte-equal across the two paths — adopt introduces no divergence on the chain-linkable case.
        legacy.accepted == adopt.accepted,
        legacy.calculatedCurrencyState == adopt.calculatedCurrencyState,
        legacy.incomingCurrencySnapshotsWithState == adopt.incomingCurrencySnapshotsWithState,
        legacy.balanceUpdate == adopt.balanceUpdate,
        // `returned` differs only in that the adopt path can never return (committee-accepted); on the happy path both are empty.
        legacy.returned.isEmpty && adopt.returned.isEmpty
      )
  }
}
