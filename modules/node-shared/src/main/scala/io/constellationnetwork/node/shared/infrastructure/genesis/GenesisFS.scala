package io.constellationnetwork.node.shared.infrastructure.genesis

import java.nio.file.NoSuchFileException

import cats.effect.Async
import cats.syntax.all._

import scala.util.control.NoStackTrace

import io.constellationnetwork.ext.json._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.genesis.types.{GenesisCSVAccount, L0GenesisData}
import io.constellationnetwork.node.shared.domain.genesis.{GenesisFS, types}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.snapshot.FullSnapshot
import io.constellationnetwork.security.signature.Signed

import fs2.data.csv._
import fs2.io.file.{Files, Path}
import fs2.{Stream, text}
import io.circe.parser.{decode => circeDecode}
import io.circe.{Decoder, Encoder}

object GenesisFS {

  case class GenesisFileNotFound(path: Path, fileType: String) extends NoStackTrace {
    override def getMessage: String = s"Genesis $fileType file not found at $path"
  }

  def make[F[_]: Async: JsonSerializer, S <: FullSnapshot[_, _]: Encoder: Decoder]: GenesisFS[F, S] = new GenesisFS[F, S] {
    def write(genesis: Signed[S], identifier: Address, path: Path): F[Unit] = {
      // Ensure directory exists before writing
      val ensureDir = Files.forAsync[F].createDirectories(path)

      val writeGenesis = Stream
        .evalSeq(genesis.toBinaryF.map(_.toSeq))
        .through(Files.forAsync[F].writeAll(path / "genesis.snapshot"))

      val writeIdentifier = Stream
        .emit(identifier.value.value)
        .through(text.utf8.encode)
        .through(Files.forAsync[F].writeAll(path / "genesis.address"))

      ensureDir >> writeGenesis.merge(writeIdentifier).compile.drain
    }

    def loadBalances(path: Path): F[Set[types.GenesisAccount]] =
      Files
        .forAsync[F]
        .readAll(path)
        .through(text.utf8.decode)
        .through(
          decodeWithoutHeaders[GenesisCSVAccount]()
        )
        .map(_.toGenesisAccount)
        .map(_.leftMap(new RuntimeException(_)))
        .rethrow
        .compile
        .toList
        .map(_.toSet)
        .adaptError {
          case _: NoSuchFileException => GenesisFileNotFound(path, "balances")
        }

    def loadSignedGenesis(path: Path): F[Signed[S]] =
      Files
        .forAsync[F]
        .readAll(path)
        .compile
        .toList
        .map(_.toArray)
        .flatMap(_.fromBinaryF[Signed[S]])
        .adaptError {
          case _: NoSuchFileException => GenesisFileNotFound(path, "snapshot")
        }

    // Tier-1 test-vector loader. Mirrors the byte-stream pattern of `loadSignedGenesis` but the
    // payload is plain UTF-8 JSON (no Brotli) for human-reviewability of the committed fixtures.
    // Uses `circeDecode` directly rather than `JsonSerializer.deserialize` because the latter is
    // for the Brotli-compressed wire path; fixture files are reviewed and diff'd in PRs.
    def loadL0Genesis(path: Path): F[L0GenesisData] =
      Files
        .forAsync[F]
        .readAll(path)
        .through(text.utf8.decode)
        .compile
        .string
        .flatMap { jsonStr =>
          circeDecode[L0GenesisData](jsonStr) match {
            case Right(data) => Async[F].pure(data)
            case Left(err) =>
              Async[F].raiseError[L0GenesisData](
                new RuntimeException(s"Failed to decode L0GenesisData at $path: ${err.getMessage}")
              )
          }
        }
        .adaptError {
          case _: NoSuchFileException => GenesisFileNotFound(path, "l0-genesis")
        }
  }
}
