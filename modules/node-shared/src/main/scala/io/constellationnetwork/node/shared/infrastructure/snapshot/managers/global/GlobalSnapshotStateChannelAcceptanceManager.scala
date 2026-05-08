package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.Eval
import cats.data.{NonEmptyChain, NonEmptyList, NonEmptySet}
import cats.effect.kernel.{Async, Ref}
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.list._
import cats.syntax.traverse._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary}
import io.constellationnetwork.syntax.sortedCollection._

import _root_.cats.kernel.Order
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong

trait GlobalSnapshotStateChannelAcceptanceManager[F[_]] {
  def accept(
    ordinal: SnapshotOrdinal,
    priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
    events: List[StateChannelOutput]
  )(
    implicit hasher: Hasher[F]
  ): F[
    (
      SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
      Set[StateChannelOutput]
    )
  ]
}

object GlobalSnapshotStateChannelAcceptanceManager {
  def make[F[_]: Async](
    stateChannelAllowanceLists: Option[Map[Address, NonEmptySet[PeerId]]],
    pullDelay: NonNegLong = NonNegLong.MinValue,
    purgeDelay: NonNegLong = NonNegLong.MinValue
  ): F[GlobalSnapshotStateChannelAcceptanceManager[F]] =
    // First-sight registry keyed by `(address, parent.lastSnapshotHash)`. Tracks
    // (firstSeenOrdinal, set of binary hashes seen so far at that key). The set membership
    // ensures that under fork-recovery, when a freshly-built canonical binary arrives at a
    // parent slot whose orphan-period predecessor was cached AND aged past purgeDelay, the
    // new binary gets its own first-sight registration instead of being silently dropped via
    // the orphan's stale `seenAt`. (#113) Without this, ml0 re-builds at parent=H but gl0
    // says "already saw key (addr, H) at X (>5 ords ago), purge" and the canonical binary
    // never advances gl0's metagraph view. The race-aging semantics for honest concurrent
    // siblings (multiple binaries sharing the same parent in the same window) is preserved
    // because they all share the SAME first-sight ord — they enter the set together at first
    // sight, and all become pullable when shouldPull triggers.
    Ref.of[F, Map[(Address, Hash), (Long, Set[Hash])]](Map.empty).map { firstSeenKeysForOrdinalR =>
      new GlobalSnapshotStateChannelAcceptanceManager[F] {

        def accept(
          ordinal: SnapshotOrdinal,
          priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
          events: List[StateChannelOutput]
        )(
          implicit hasher: Hasher[F]
        ): F[
          (
            SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
            Set[StateChannelOutput]
          )
        ] =
          events
            .groupBy(_.address)
            .toList
            .traverse {
              case (address, outputs) =>
                acceptForAddress(
                  ordinal,
                  stateChannelAllowanceLists.flatMap(_.get(address))
                )(
                  priorLastStateChannelSnapshotHashes.getOrElse(address, Hash.empty),
                  outputs
                ).map {
                  case (accepted, returned) => (accepted.map(address -> _), returned.toSet)
                }
            }
            .flatTap { _ =>
              firstSeenKeysForOrdinalR.update(_.filterNot { case (_, (seenAt, _)) => shouldPurge(seenAt, ordinal) })
            }
            .map(_.unzip)
            .map {
              case (accepted, returned) => (accepted.flatMap(_.toList).toMap.toSortedMap, returned.toSet.flatten)
            }

        private def acceptForAddress(
          ordinal: SnapshotOrdinal,
          allowedPeers: Option[NonEmptySet[PeerId]]
        )(lastHash: Hash, outputs: List[StateChannelOutput])(implicit hasher: Hasher[F]) = for {
          outputsWithHashes <- outputs.traverse(stateChannelOutputWithHashes)
          (notAllowed, allowed) <- allowedForProcessing(ordinal, outputsWithHashes).map(_.partitionMap(identity))
          (impossibleCandidates, possibleCandidates) = onlyPossibleReferences(lastHash, allowed.flatten).partitionMap(identity)
          toReturn = notAllowed.flatten.map(_.output) ++ impossibleCandidates.map(_.output)
          toAdd = selectStateChannels(allowedPeers)(lastHash, possibleCandidates)
        } yield (toAdd, toReturn)

        private def allowedForProcessing(ordinal: SnapshotOrdinal, withHashes: List[StateChannelOutputWithHash]) =
          withHashes.groupBy(o => (o.output.address, o.output.snapshotBinary.lastSnapshotHash)).toList.traverse {
            case (key, outputs) =>
              val incomingHashes = outputs.map(_.hash).toSet
              firstSeenKeysForOrdinalR.modify { current =>
                current.get(key) match {
                  case Some((seenAt, knownHashes)) if shouldPurge(seenAt, ordinal) =>
                    val novelHashes = incomingHashes -- knownHashes
                    if (novelHashes.nonEmpty) {
                      // (#113) New binary content arrived at this parent-slot AFTER the cached
                      // entry aged out. Re-register at this ordinal so the rebuilt canonical
                      // binary doesn't get silently purged. Old binaries that already aged
                      // remain blocked because they're still in `knownHashes`.
                      val refreshed = (ordinal.value.value, knownHashes ++ incomingHashes)
                      val novelOutputs = outputs.filter(o => novelHashes.contains(o.hash))
                      val verdict =
                        if (shouldPull(ordinal.value, ordinal)) Right(novelOutputs)
                        else Left(novelOutputs)
                      (current.updated(key, refreshed), verdict)
                    } else
                      (current, Left(List.empty))
                  case Some((seenAt, knownHashes)) if shouldPull(seenAt, ordinal) =>
                    val merged = (seenAt, knownHashes ++ incomingHashes)
                    (current.updated(key, merged), Right(outputs))
                  case Some((seenAt, knownHashes)) =>
                    val merged = (seenAt, knownHashes ++ incomingHashes)
                    (current.updated(key, merged), Left(outputs))
                  case None if shouldPull(ordinal.value, ordinal) =>
                    (current.updated(key, (ordinal.value.value, incomingHashes)), Right(outputs))
                  case None =>
                    (current.updated(key, (ordinal.value.value, incomingHashes)), Left(outputs))
                }
              }
          }

        private def onlyPossibleReferences(
          lastHashReference: Hash,
          outputs: List[StateChannelOutputWithHash]
        ): List[Either[StateChannelOutputWithHash, StateChannelOutputWithHash]] = {
          val references = lastHashReference :: outputs.map(_.hash)

          outputs.map { o =>
            val hasReference = references.contains(o.output.snapshotBinary.value.lastSnapshotHash)

            Either.cond(hasReference, o, o)
          }
        }

        private def selectStateChannels(
          allowedPeers: Option[NonEmptySet[PeerId]]
        )(lastHash: Hash, stateChannels: List[StateChannelOutputWithHash]) = {
          val lastHashForStateChannel = stateChannels.groupByNec(_.output.snapshotBinary.lastSnapshotHash)

          def unfold(lastHash: Hash): Eval[List[StateChannelOutputWithHash]] =
            lastHashForStateChannel
              .get(lastHash)
              .map(pickMajority(allowedPeers))
              .map { go =>
                for {
                  head <- Eval.now(go)
                  tail <- unfold(go.hash)
                } yield head :: tail
              }
              .getOrElse(Eval.now(List.empty))

          unfold(lastHash).value.toNel.map(_.map(_.output.snapshotBinary).reverse)
        }

        private def pickMajority(allowedPeers: Option[NonEmptySet[PeerId]])(outputs: NonEmptyChain[StateChannelOutputWithHash]) =
          (pickMajorityByNumberOfSignatures(filterWithAllowedPeers(allowedPeers)) _)
            .andThen(pickMajorityByNumberOfSignatures(_.toSortedSet))(outputs)
            .groupBy(_.hash)
            .mapBoth((hash, o) => ((o.length, hash), o.sortBy(_.proofsHash).head))(Order.reverse(implicitly[Order[(Long, Hash)]]))
            .head
            ._2

        private def pickMajorityByNumberOfSignatures(
          filterSignatures: NonEmptySet[SignatureProof] => SortedSet[SignatureProof]
        )(outputs: NonEmptyChain[StateChannelOutputWithHash]) =
          outputs.tail
            .foldLeft(NonEmptyChain(outputs.head)) {
              case (acc, o) if filterSignatures(acc.head.proofs).size < filterSignatures(o.proofs).size  => NonEmptyChain(o)
              case (acc, o) if filterSignatures(acc.head.proofs).size == filterSignatures(o.proofs).size => acc.append(o)
              case (acc, _)                                                                              => acc
            }

        private def filterWithAllowedPeers(
          allowedPeers: Option[NonEmptySet[PeerId]]
        )(signatures: NonEmptySet[SignatureProof]): SortedSet[SignatureProof] =
          signatures.filter(signature => allowedPeers.map(allowed => allowed.contains(signature.id.toPeerId)).getOrElse(true))

        private def stateChannelOutputWithHashes(output: StateChannelOutput)(implicit hasher: Hasher[F]) =
          output.snapshotBinary.toHashed.map(hashed => StateChannelOutputWithHash(output, hashed.hash, hashed.proofsHash))

        private def shouldPurge(seenAt: Long, ordinal: SnapshotOrdinal): Boolean =
          seenAt <= ordinal.value - pullDelay - purgeDelay

        private def shouldPull(seenAt: Long, ordinal: SnapshotOrdinal): Boolean =
          seenAt <= ordinal.value - pullDelay

      }
    }

  private case class StateChannelOutputWithHash(output: StateChannelOutput, hash: Hash, proofsHash: ProofsHash) {
    def proofs = output.snapshotBinary.proofs
  }

}
