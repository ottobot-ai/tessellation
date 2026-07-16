package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.Functor
import cats.data.NonEmptyList
import cats.syntax.functor._

import scala.collection.immutable.SortedMap
import scala.util.control.NoStackTrace

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{
  ExactReplayHistoryBatch,
  ExactReplayHistoryFailure,
  ExactReplayHistorySession
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.Phase2ReferencePolicy.ExactCanonicalAncestor
import io.constellationnetwork.node.shared.domain.snapshot.finality.Phase2UseScope.CurrencySnapshotReplay
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendAction
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.{MptImageDigest, MptImageReceipt}
import io.constellationnetwork.security.signature.Signed

/** DARK IDENTITY-COMPOSITION SCAFFOLD ONLY.
  *
  * None of the package-owned inputs below currently has a production issuer. Their package-visible constructors exist for the pure model
  * suite and future dedicated verifiers; constructor access or identifier equality does not prove image semantics, field-32 witness parity,
  * or value derivation. Consequently this file is private to the finality package, has no live caller, and must not be wired to replay,
  * signing, inclusion, slashing, serving, or mutation until ROOT-008/010 and the Phase-2 issuer gates land.
  */

/** Three distinct field-32 preimage states. In particular, absent and present-empty are not interchangeable. */
private[finality] sealed abstract class Field32ReplayWitnessState private[finality] ()

private[finality] object Field32ReplayWitnessState {
  final class Absent private[finality] () extends Field32ReplayWitnessState
  final class PresentEmpty private[finality] () extends Field32ReplayWitnessState
  final class PresentNonEmpty private[finality] (val entryCount: Int) extends Field32ReplayWitnessState
}

/** Target identity of one immutable verified image. This dark slice deliberately has no live issuer or image verifier. */
private[finality] final class CandidateExactImageReceipt private[finality] (
  private[finality] val scope: CurrencySnapshotReplay,
  private[finality] val receipt: MptImageReceipt
)

/** Target semantic-reproduction receipt for the whole image. Identifier agreement alone is not semantic verification. */
private[finality] final class CandidateWholeImageSemanticReceipt private[finality] (
  private[finality] val scope: CurrencySnapshotReplay,
  private[finality] val target: GlobalSnapshotStateRef,
  private[finality] val imageDigest: MptImageDigest,
  private[finality] val imageGeneration: Long,
  private[finality] val receipt: ScopedArtifactRef
)

/** Target field-32 replay-witness receipt. Its population and preimage digests are unproved until ROOT-010 supplies the issuer. */
private[finality] final class CandidateField32ReplayWitnessReceipt private[finality] (
  private[finality] val scope: CurrencySnapshotReplay,
  private[finality] val target: GlobalSnapshotStateRef,
  private[finality] val imageDigest: MptImageDigest,
  private[finality] val imageGeneration: Long,
  private[finality] val semanticReceipt: ScopedArtifactRef,
  private[finality] val state: Field32ReplayWitnessState,
  private[finality] val preimageDigest: Hash,
  private[finality] val operatorPopulationDigest: Hash
)

/** Currency replay projection at one exact global target.
  *
  * The maps may contain global balances and multiple metagraphs because framework replay can require those reads. A future issuer must
  * derive the complete required projection internally from the verified image; this model proves neither derivation nor namespace scope.
  */
private[finality] final class CandidateRootedCurrencyValues private[finality] (
  private[finality] val scope: CurrencySnapshotReplay,
  private[finality] val target: GlobalSnapshotStateRef,
  private[finality] val imageDigest: MptImageDigest,
  private[finality] val imageGeneration: Long,
  private[finality] val semanticReceipt: ScopedArtifactRef,
  val balances: SortedMap[Address, Balance],
  val lastCurrencySnapshots: CandidateCurrencyReplayView.LastCurrencySnapshots,
  val metagraphSyncData: Option[SortedMap[Address, MetagraphSyncDataInfo]]
)

/** One exact structural-history result plus SpendActions derived from those signed artifacts only. */
private[finality] sealed abstract class CandidateCurrencyReplayArtifacts private[finality] (
  val exact: ExactReplayHistoryBatch,
  val spendActionsOldestFirst: Vector[
    (GlobalSnapshotStateRef, Option[SortedMap[Address, List[SpendAction]]])
  ]
)

private[finality] sealed abstract class CandidateCurrencyReplayViewFailure(message: String)
    extends RuntimeException(message)
    with NoStackTrace
    with Product
    with Serializable

private[finality] object CandidateCurrencyReplayViewFailure {
  sealed trait Component extends Product with Serializable

  object Component {
    case object Lease extends Component
    case object History extends Component
    case object ExactImage extends Component
    case object WholeImageSemanticReceipt extends Component
    case object Field32ReplayWitness extends Component
    case object RootedCurrencyValues extends Component
  }

  final case class WrongLeaseScope(observed: Phase2UseScope)
      extends CandidateCurrencyReplayViewFailure(s"Currency replay requires CurrencySnapshotReplay, got $observed")

  final case class WrongPolicy(observed: Phase2ReferencePolicy)
      extends CandidateCurrencyReplayViewFailure(s"Currency replay requires exact-canonical-ancestor policy, got $observed")

  final case class ScopeMismatch(component: Component, expected: CurrencySnapshotReplay, observed: CurrencySnapshotReplay)
      extends CandidateCurrencyReplayViewFailure(s"$component replay scope mismatch: expected=$expected observed=$observed")

  final case class TargetMismatch(component: Component, expected: GlobalSnapshotStateRef, observed: GlobalSnapshotStateRef)
      extends CandidateCurrencyReplayViewFailure(s"$component replay target mismatch: expected=$expected observed=$observed")

  final case class ImageDigestMismatch(component: Component, expected: MptImageDigest, observed: MptImageDigest)
      extends CandidateCurrencyReplayViewFailure(s"$component image digest mismatch: expected=$expected observed=$observed")

  final case class ImageGenerationMismatch(component: Component, expected: Long, observed: Long)
      extends CandidateCurrencyReplayViewFailure(s"$component image generation mismatch: expected=$expected observed=$observed")

  final case class ExactImageReceiptMismatch(expected: MptImageReceipt, observed: MptImageReceipt)
      extends CandidateCurrencyReplayViewFailure(s"Exact image receipt mismatch: expected=$expected observed=$observed")

  final case class SemanticReceiptMismatch(component: Component, expected: ScopedArtifactRef, observed: ScopedArtifactRef)
      extends CandidateCurrencyReplayViewFailure(s"$component semantic receipt mismatch: expected=$expected observed=$observed")

  final case class ReleasedImageMissing(target: GlobalSnapshotStateRef)
      extends CandidateCurrencyReplayViewFailure(s"Phase-2 release has no exact image for $target")

  final case class InvalidGeneration(component: Component, observed: Long)
      extends CandidateCurrencyReplayViewFailure(s"$component image generation must be non-negative, got $observed")

  final case class InvalidDigest(component: Component, field: String, observed: Hash)
      extends CandidateCurrencyReplayViewFailure(s"$component has invalid $field digest '${observed.value}'")

  final case class InvalidField32State(detail: String)
      extends CandidateCurrencyReplayViewFailure(s"Invalid field-32 replay-witness state: $detail")
}

/** Candidate-scoped, exact replay inputs. This capability cannot be built from structural history alone.
  *
  * It is deliberately dark: no live runtime constructs it, it has no codec, and it cannot sign, commit, or decide finality. Construction
  * requires a current [[CanonicalPhase2Lease]] and exact identity agreement across the released image, semantic reproduction, field-32
  * witness, and decoded rooted values.
  */
private[finality] sealed abstract class CandidateCurrencyReplayView[F[_]] private[finality] () {
  def scope: CurrencySnapshotReplay
  def target: GlobalSnapshotStateRef
  def balances: SortedMap[Address, Balance]
  def lastCurrencySnapshots: CandidateCurrencyReplayView.LastCurrencySnapshots
  def metagraphSyncData: Option[SortedMap[Address, MetagraphSyncDataInfo]]

  def resolveOrdinals(
    targets: NonEmptyList[SnapshotOrdinal]
  ): F[Either[ExactReplayHistoryFailure, CandidateCurrencyReplayArtifacts]]

  def resolveExact(
    targets: NonEmptyList[GlobalSnapshotStateRef]
  ): F[Either[ExactReplayHistoryFailure, CandidateCurrencyReplayArtifacts]]
}

private[finality] object CandidateCurrencyReplayView {
  type LastCurrencySnapshots = SortedMap[Address, (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]

  private final class IssuedArtifacts(batch: ExactReplayHistoryBatch)
      extends CandidateCurrencyReplayArtifacts(
        batch,
        batch.artifactsOldestFirst.map { artifact =>
          artifact.reference -> artifact.snapshot.signed.value.spendActions
        }
      )

  private final class IssuedView[F[_]: Functor](
    val scope: CurrencySnapshotReplay,
    val target: GlobalSnapshotStateRef,
    history: ExactReplayHistorySession[F],
    values: CandidateRootedCurrencyValues
  ) extends CandidateCurrencyReplayView[F] {
    val balances: SortedMap[Address, Balance] = values.balances
    val lastCurrencySnapshots: LastCurrencySnapshots = values.lastCurrencySnapshots
    val metagraphSyncData: Option[SortedMap[Address, MetagraphSyncDataInfo]] = values.metagraphSyncData

    def resolveOrdinals(
      targets: NonEmptyList[SnapshotOrdinal]
    ): F[Either[ExactReplayHistoryFailure, CandidateCurrencyReplayArtifacts]] =
      history.resolveOrdinals(targets).map(_.map(new IssuedArtifacts(_)))

    def resolveExact(
      targets: NonEmptyList[GlobalSnapshotStateRef]
    ): F[Either[ExactReplayHistoryFailure, CandidateCurrencyReplayArtifacts]] =
      history.resolveExact(targets).map(_.map(new IssuedArtifacts(_)))
  }

  import CandidateCurrencyReplayViewFailure.Component._
  import CandidateCurrencyReplayViewFailure._

  /** Validate only the identity closure represented by these dark model inputs.
    *
    * This is intentionally not named or typed as an issuer: it does not derive any receipt or value from retained bytes and therefore
    * cannot establish the semantic predicates required by ROOT-008/010.
    */
  private[finality] def composeIdentityModel[F[_]: Functor](
    lease: CanonicalPhase2Lease,
    history: ExactReplayHistorySession[F],
    exactImage: CandidateExactImageReceipt,
    semantic: CandidateWholeImageSemanticReceipt,
    field32: CandidateField32ReplayWitnessReceipt,
    values: CandidateRootedCurrencyValues
  ): Either[CandidateCurrencyReplayViewFailure, CandidateCurrencyReplayView[F]] =
    for {
      scope <- lease.scope match {
        case replay: CurrencySnapshotReplay => Right(replay)
        case other                          => Left(WrongLeaseScope(other): CandidateCurrencyReplayViewFailure)
      }
      _ <- requirePolicy(lease.policy)
      _ <- requireTarget(History, lease.target, history.anchor)
      releasedImage <- lease.released.payload.receipt.activePublication.image
        .toRight(ReleasedImageMissing(lease.target): CandidateCurrencyReplayViewFailure)
      _ <- requireScope(ExactImage, scope, exactImage.scope)
      _ <- requireTarget(ExactImage, lease.target, exactImage.receipt.anchor)
      _ <- requireExactImage(releasedImage, exactImage.receipt)
      _ <- requireGeneration(ExactImage, exactImage.receipt.generation)
      _ <- requireDigest(ExactImage, "image", exactImage.receipt.digest.value)
      _ <- requireScope(WholeImageSemanticReceipt, scope, semantic.scope)
      _ <- requireTarget(WholeImageSemanticReceipt, lease.target, semantic.target)
      _ <- requireImageIdentity(WholeImageSemanticReceipt, exactImage.receipt, semantic.imageDigest, semantic.imageGeneration)
      _ <- requireSemanticReceipt(
        WholeImageSemanticReceipt,
        lease.released.payload.receipt.semanticReceipt,
        semantic.receipt
      )
      _ <- requireScope(Field32ReplayWitness, scope, field32.scope)
      _ <- requireTarget(Field32ReplayWitness, lease.target, field32.target)
      _ <- requireImageIdentity(Field32ReplayWitness, exactImage.receipt, field32.imageDigest, field32.imageGeneration)
      _ <- requireSemanticReceipt(Field32ReplayWitness, semantic.receipt, field32.semanticReceipt)
      _ <- validateField32(field32)
      _ <- requireScope(RootedCurrencyValues, scope, values.scope)
      _ <- requireTarget(RootedCurrencyValues, lease.target, values.target)
      _ <- requireImageIdentity(RootedCurrencyValues, exactImage.receipt, values.imageDigest, values.imageGeneration)
      _ <- requireSemanticReceipt(RootedCurrencyValues, semantic.receipt, values.semanticReceipt)
    } yield new IssuedView(scope, lease.target, history, values)

  private def requirePolicy(policy: Phase2ReferencePolicy): Either[CandidateCurrencyReplayViewFailure, Unit] =
    Either.cond(policy == ExactCanonicalAncestor, (), WrongPolicy(policy))

  private def requireScope(
    component: Component,
    expected: CurrencySnapshotReplay,
    observed: CurrencySnapshotReplay
  ): Either[CandidateCurrencyReplayViewFailure, Unit] =
    Either.cond(observed == expected, (), ScopeMismatch(component, expected, observed))

  private def requireTarget(
    component: Component,
    expected: GlobalSnapshotStateRef,
    observed: GlobalSnapshotStateRef
  ): Either[CandidateCurrencyReplayViewFailure, Unit] =
    Either.cond(observed == expected, (), TargetMismatch(component, expected, observed))

  private def requireExactImage(
    expected: MptImageReceipt,
    observed: MptImageReceipt
  ): Either[CandidateCurrencyReplayViewFailure, Unit] =
    Either.cond(
      observed == expected,
      (),
      if (observed.generation != expected.generation)
        ImageGenerationMismatch(ExactImage, expected.generation, observed.generation)
      else if (observed.digest != expected.digest)
        ImageDigestMismatch(ExactImage, expected.digest, observed.digest)
      else if (observed.anchor != expected.anchor)
        TargetMismatch(ExactImage, expected.anchor, observed.anchor)
      else ExactImageReceiptMismatch(expected, observed)
    )

  private def requireImageIdentity(
    component: Component,
    expected: MptImageReceipt,
    observedDigest: MptImageDigest,
    observedGeneration: Long
  ): Either[CandidateCurrencyReplayViewFailure, Unit] =
    if (observedGeneration != expected.generation)
      Left(ImageGenerationMismatch(component, expected.generation, observedGeneration))
    else if (observedDigest != expected.digest)
      Left(ImageDigestMismatch(component, expected.digest, observedDigest))
    else Right(())

  private def requireSemanticReceipt(
    component: Component,
    expected: ScopedArtifactRef,
    observed: ScopedArtifactRef
  ): Either[CandidateCurrencyReplayViewFailure, Unit] =
    Either.cond(observed == expected, (), SemanticReceiptMismatch(component, expected, observed))

  private def requireGeneration(
    component: Component,
    generation: Long
  ): Either[CandidateCurrencyReplayViewFailure, Unit] =
    Either.cond(generation >= 0L, (), InvalidGeneration(component, generation))

  private def requireDigest(
    component: Component,
    field: String,
    digest: Hash
  ): Either[CandidateCurrencyReplayViewFailure, Unit] =
    Either.cond(isCanonicalHash(digest), (), InvalidDigest(component, field, digest))

  private def validateField32(
    receipt: CandidateField32ReplayWitnessReceipt
  ): Either[CandidateCurrencyReplayViewFailure, Unit] =
    for {
      _ <- requireDigest(Field32ReplayWitness, "preimage", receipt.preimageDigest)
      _ <- requireDigest(Field32ReplayWitness, "operator-population", receipt.operatorPopulationDigest)
      _ <- receipt.state match {
        case _: Field32ReplayWitnessState.Absent       => Right(())
        case _: Field32ReplayWitnessState.PresentEmpty => Right(())
        case state: Field32ReplayWitnessState.PresentNonEmpty =>
          Either.cond(state.entryCount > 0, (), InvalidField32State(s"present-nonempty entryCount=${state.entryCount}"))
      }
    } yield ()

  private def isCanonicalHash(hash: Hash): Boolean =
    hash != Hash.empty && hash.value.length == 64 && hash.value.forall(character =>
      character >= '0' && character <= '9' || character >= 'a' && character <= 'f'
    )
}
