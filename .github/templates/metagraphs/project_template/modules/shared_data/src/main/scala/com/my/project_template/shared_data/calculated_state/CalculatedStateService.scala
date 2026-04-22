package com.my.project_template.shared_data.calculated_state

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash

import com.my.project_template.shared_data.types.Types.UsageUpdateCalculatedState
import io.circe.syntax.EncoderOps

trait CalculatedStateService[F[_]] {
  def getCalculatedState: F[CalculatedState]

  def setCalculatedState(
    snapshotOrdinal: SnapshotOrdinal,
    state: UsageUpdateCalculatedState
  ): F[Boolean]

  def hashCalculatedState(
    state: UsageUpdateCalculatedState
  ): F[Hash]
}

object CalculatedStateService {
  def make[F[_]: Async]: F[CalculatedStateService[F]] =
    Ref.of[F, CalculatedState](CalculatedState.empty).map { stateRef =>
      new CalculatedStateService[F] {
        override def getCalculatedState: F[CalculatedState] = stateRef.get

        override def setCalculatedState(
          snapshotOrdinal: SnapshotOrdinal,
          state: UsageUpdateCalculatedState
        ): F[Boolean] =
          stateRef.modify { currentState =>
            val devices = currentState.state.devices ++ state.devices
            CalculatedState(snapshotOrdinal, UsageUpdateCalculatedState(devices)) -> true
          }

        override def hashCalculatedState(
          state: UsageUpdateCalculatedState
        ): F[Hash] = Async[F].delay {
          // SHA-256 of the canonical JSON encoding, rendered as 64-char lowercase hex.
          // This field feeds `DataApplicationPart.calculatedStateProof: Hash`, which is
          // written into the MPT via a strict 64-hex-char scodec codec; returning the raw
          // JSON string here (as the template previously did) passes circe-JSON MPT
          // storage but blows up on the typed-scodec storage path.
          val digest = MessageDigest.getInstance("SHA-256").digest(state.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
          Hash(digest.map("%02x".format(_)).mkString)
        }
      }
    }
}
