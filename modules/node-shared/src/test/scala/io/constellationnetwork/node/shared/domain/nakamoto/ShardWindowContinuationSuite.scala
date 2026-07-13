package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.syntax.eq._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object ShardWindowContinuationSuite extends SimpleIOSuite {

  private val proof = SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))

  private def bin(parent: Hash): Signed[StateChannelSnapshotBinary] =
    Signed(
      StateChannelSnapshotBinary(parent, Array[Byte](0), SnapshotFee(NonNegLong.unsafeFrom(0L))),
      NonEmptySet.of(proof)
    )

  private def deepCopy(binary: Signed[StateChannelSnapshotBinary]): Signed[StateChannelSnapshotBinary] =
    Signed(binary.value.copy(content = binary.value.content.clone()), binary.proofs)

  private def h(c: String): Hash = Hash(c * 64)
  private val A = h("a")
  private val B = h("b")
  private val C = h("c")
  private val X = h("e")
  private val W = h("f")
  private val mgA = Address.fromBytes("mg-a".getBytes("UTF-8"))
  private val mgB = Address.fromBytes("mg-b".getBytes("UTF-8"))
  private val shard0 = ShardId.unsafeApply(0)
  private val shard1 = ShardId.unsafeApply(1)

  private val committeeSignature = CommitteeMemberSignature(
    peerId = io.constellationnetwork.schema.peer.PeerId(Hex("33" * 64)),
    vrfProof = Hex(""),
    ed25519Sig = Hex(""),
    kesProductSig = Hex(""),
    kesTreeStep = 0
  )

  private def checkpoint(
    shardId: ShardId,
    windows: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  ): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardId,
      parentCheckpointHash = Hash.empty,
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = SnapshotOrdinal.MinValue,
      slot = Slot.MinValue,
      derivedStateDelta = ShardDerivedStateDelta(
        perMetagraphMptRoots = SortedMap.from(windows.keys.map(_ -> W)),
        includedSnapshots = windows
      ),
      committeeSignatures = NonEmptyList.of(committeeSignature),
      epoch = EtaPeriod(0L)
    )

  pureTest("Continue: a window binary continues GL0's tip at the head") {
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(A), bin(B)), W, A) ==
        ShardWindowContinuation.Continue(0)
    )
  }

  pureTest("Continue: a window binary continues GL0's tip after an already-adopted prefix") {
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(A), bin(B), bin(C)), W, B) ==
        ShardWindowContinuation.Continue(1)
    )
  }

  pureTest("unseeded metagraph: a genesis window continues Hash.empty") {
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(Hash.empty), bin(A)), W, Hash.empty) ==
        ShardWindowContinuation.Continue(0)
    )
  }

  pureTest("sibling lineage: genesis-rooted window is deferred when it does not continue the committed tip") {
    val sibling = NonEmptyList.of(bin(Hash.empty), bin(A), bin(B), bin(C))
    expect(ShardWindowContinuation.classify(sibling, W, X) == ShardWindowContinuation.Defer)
  }

  pureTest("AlreadyAdopted: a window whose tail is GL0's tip is a no-op") {
    val T = h("d")
    val fullyAdopted = NonEmptyList.of(bin(Hash.empty), bin(A))
    expect(ShardWindowContinuation.classify(fullyAdopted, T, T) == ShardWindowContinuation.AlreadyAdopted)
  }

  pureTest("Continue takes precedence over AlreadyAdopted") {
    val T = h("d")
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(T), bin(A)), T, T) ==
        ShardWindowContinuation.Continue(0)
    )
  }

  pureTest("successor window continues the tip at which its predecessor ended") {
    val T = h("d")
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(T), bin(A)), W, T) ==
        ShardWindowContinuation.Continue(0)
    )
  }

  pureTest("missing ancestor: a mid-rooted window that does not continue the tip is deferred") {
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(A), bin(B)), W, X) == ShardWindowContinuation.Defer
    )
  }

  test("windowTipHashF uses the same value-hash as GL0's state-channel tip setter") {
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val nel = NonEmptyList.of(bin(Hash.empty), bin(A))
      for {
        viaHelper <- ShardWindowContinuation.windowTipHashF[IO](nel)
        viaTipSetter <- nel.last.toHashed[IO].map(_.hash)
      } yield expect(viaHelper === viaTipSetter)
    }
  }

  test("atomic continuation rejects a multi-MG checkpoint when any segment is deferred") {
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val cp = checkpoint(
        shard0,
        SortedMap(
          mgA -> NonEmptyList.of(bin(A)),
          mgB -> NonEmptyList.of(bin(B))
        )
      )

      ShardWindowContinuation
        .isAtomicallyContinuableF[IO](shard0, cp, SortedMap(mgA -> A, mgB -> X))
        .map(result => expect(!result))
    }
  }

  test("Phase-2 proof requires every continuing segment's exact accepted suffix") {
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val aWindow = NonEmptyList.of(bin(A), bin(B))
      val bWindow = NonEmptyList.of(bin(B))
      val cp = checkpoint(shard0, SortedMap(mgA -> aWindow, mgB -> bWindow))
      val tips = SortedMap(mgA -> A, mgB -> B)

      for {
        complete <- ShardWindowContinuation.wasFullyAppliedF[IO](
          shard0,
          cp,
          tips,
          SortedMap(mgA -> aWindow, mgB -> bWindow)
        )
        missing <- ShardWindowContinuation.wasFullyAppliedF[IO](
          shard0,
          cp,
          tips,
          SortedMap(mgA -> aWindow)
        )
        shortened <- ShardWindowContinuation.wasFullyAppliedF[IO](
          shard0,
          cp,
          tips,
          SortedMap(mgA -> NonEmptyList.of(aWindow.head), mgB -> bWindow)
        )
      } yield expect(complete) && expect(!missing) && expect(!shortened)
    }
  }

  test("Phase-2 proof rejects an outer shard-map key mismatch") {
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val window = NonEmptyList.of(bin(A))
      val cp = checkpoint(shard0, SortedMap(mgA -> window))

      ShardWindowContinuation
        .wasFullyAppliedF[IO](shard1, cp, SortedMap(mgA -> A), SortedMap(mgA -> window))
        .map(result => expect(!result))
    }
  }

  test("Phase-2 proof compares canonical bytes across independently decoded array instances") {
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val window = NonEmptyList.of(bin(A), bin(B))
      val independentlyDecoded = window.map(deepCopy)
      val cp = checkpoint(shard0, SortedMap(mgA -> window))

      ShardWindowContinuation
        .wasFullyAppliedF[IO](shard0, cp, SortedMap(mgA -> A), SortedMap(mgA -> independentlyDecoded))
        .map(result => expect(result) && expect(window.head.value.content ne independentlyDecoded.head.value.content))
    }
  }

  test("an already-applied segment can accompany one exact new segment") {
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val already = NonEmptyList.of(bin(Hash.empty), bin(A))
      val continuing = NonEmptyList.of(bin(B))
      val cp = checkpoint(shard0, SortedMap(mgA -> already, mgB -> continuing))

      for {
        alreadyTip <- ShardWindowContinuation.windowTipHashF[IO](already)
        result <- ShardWindowContinuation.wasFullyAppliedF[IO](
          shard0,
          cp,
          SortedMap(mgA -> alreadyTip, mgB -> B),
          SortedMap(mgB -> continuing)
        )
      } yield expect(result)
    }
  }
}
