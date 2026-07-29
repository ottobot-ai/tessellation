package io.constellationnetwork.node.shared.domain.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import io.constellationnetwork.schema.GlobalIncrementalSnapshot
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotCodecs._
import io.constellationnetwork.serde.implicits._

import scodec.bits.ByteVector
import weaver.FunSuite

/** RED gates for the owner-ratified O22 ordinal-zero launch contract.
  *
  * These checks specify the final active path. Absence of fraud-proof authority is not the target: launch artifacts carry bounded canonical
  * `InvalidStateProofEvidenceV1`, every candidate is adjudicated atomically against its exact parent, field 34 records the resulting
  * signer-specific audit/exclusion state, and economic policy comes from rooted consensus state.
  *
  * Source checks are temporary engineering interlocks. Promote them to behavioral codec, oracle, restart, and multi-node tests with the
  * production implementation; do not preserve source spelling as a protocol API.
  */
object O22FinalLaunchWireRedSuite extends FunSuite {

  private val snapshotSchemaPath =
    "modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalIncrementalSnapshot.scala"
  private val snapshotCodecPath =
    "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/GlobalSnapshotCodecs.scala"
  private val consensusFunctionsPath =
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusFunctions.scala"
  private val acceptanceManagerPath =
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala"
  private val slashManagerPath =
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofSlashManager.scala"
  private val accumulatorPath =
    "modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala"

  private val provisionalSnapshotBytes = ByteVector.fromValidHex(
    "000000000000000a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100400101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010100000000111111111111111111111111111111111111111111111111111111111111111122222222222222222222222222222222222222222222222222222222222222223333333333333333333333333333333333333333333333333333333333333333000000000000000000000000000000000000000000000000000005302e302e31000000000000"
  )

  private val consensusHashSchema: Regex =
    """ConsensusHashSchema\s*\[\s*GlobalIncrementalSnapshot\s*\]""".r
  private val finalEvidenceType: Regex =
    """\bInvalidStateProofEvidenceV1\b""".r
  private val provisionalEvidenceType: Regex =
    """\bInvalidStateProofEvidence\b(?!V1)""".r

  test("O22-WIRE-RED-001: retired JSON cannot supply the GL0 snapshot consensus digest") {
    val consensusFunctions = source(consensusFunctionsPath)
    val production = productionScalaSources()

    expect.all(
      consensusHashSchema.findFirstIn(production).nonEmpty,
      !consensusFunctions.contains("Hasher.forJson")
    )
  }

  test("O22-WIRE-RED-002: provisional and malformed Scodec bytes cannot decode as the launch snapshot") {
    val provisional = provisionalSnapshotBytes.fromImmutableBytes[GlobalIncrementalSnapshot]
    val trailing = (provisionalSnapshotBytes ++ ByteVector(0x00)).fromImmutableBytes[GlobalIncrementalSnapshot]

    expect.all(
      provisional.isLeft,
      trailing.isLeft
    )
  }

  test("O22-WIRE-RED-003: the launch snapshot carries one bounded canonical InvalidStateProofEvidenceV1 collection") {
    val snapshotSchema = source(snapshotSchemaPath)
    val snapshotCodec = source(snapshotCodecPath)
    val slashingSources = productionSourcesUnder(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/slashing",
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs"
    )
    val normalized = slashingSources.toLowerCase(java.util.Locale.ROOT)

    expect.all(
      finalEvidenceType.findFirstIn(snapshotSchema).nonEmpty,
      finalEvidenceType.findFirstIn(snapshotCodec).nonEmpty,
      finalEvidenceType.findFirstIn(slashingSources).nonEmpty,
      provisionalEvidenceType.findFirstIn(snapshotSchema).isEmpty,
      provisionalEvidenceType.findFirstIn(snapshotCodec).isEmpty,
      boundedConcept(normalized, "count", "entries"),
      boundedConcept(normalized, "encodedbytes", "evidencebytes", "bytes"),
      boundedConcept(normalized, "replaywork", "replaybudget"),
      boundedConcept(normalized, "historicalproof", "historyproof"),
      boundedConcept(normalized, "economiceffect", "economiclimit"),
      normalized.contains("canonical")
    )
  }

  test("O22-WIRE-RED-004: adjudication is five-way, candidate-atomic, and field-34 integrated") {
    val slashingSources = productionSourcesUnder(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/slashing"
    )
    val acceptance = source(acceptanceManagerPath)
    val accumulator = source(accumulatorPath)
    val verdicts = List(
      "Upheld",
      "EvidenceInvalid",
      "NotUpheld",
      "HistoryUnavailable",
      "NoNewlyCulpableSigner"
    )

    expect.all(
      verdicts.forall(verdict => word(verdict).findFirstIn(slashingSources).nonEmpty),
      !acceptance.contains("fraudProofs.toList.flatTraverse"),
      !acceptance.contains("case Left(_)       => Nil"),
      acceptance.contains("InvalidStateProofSlashRecordV1"),
      accumulator.contains("InvalidStateProofSlashRecordV1"),
      accumulator.contains("slashRecords"),
      acceptance.contains("StateChangesAccumulator")
    )
  }

  test("O22-WIRE-RED-005: rooted policy, not local HOCON, controls adjudication and field-34 economics") {
    val acceptance = source(acceptanceManagerPath)
    val slashManager = source(slashManagerPath)
    val consensusCriticalLocalInputs = List(
      "invaliditySlashingConfig.watchtowerEnabled",
      "config.slashFraction",
      "config.bountyFraction",
      "config.cooldownEpochs",
      "slashFraction: Ratio",
      "bountyFraction: Ratio",
      "cooldownEpochs: Long"
    )

    expect.all(
      consensusCriticalLocalInputs.forall(input => !acceptance.contains(input)),
      consensusCriticalLocalInputs.forall(input => !slashManager.contains(input))
    )
  }

  private lazy val repositoryRoot: Path = {
    @tailrec
    def find(path: Path): Path =
      if (Files.isRegularFile(path.resolve("build.sbt"))) path
      else
        Option(path.getParent) match {
          case Some(parent) => find(parent)
          case None         => throw new IllegalStateException(s"Unable to locate repository root from ${sys.props("user.dir")}")
        }

    find(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
  }

  private def source(path: String): String =
    Files.readString(repositoryRoot.resolve(path), StandardCharsets.UTF_8)

  private def productionScalaSources(): String =
    productionSourcesUnder(
      "modules/shared/src/main/scala",
      "modules/node-shared/src/main/scala",
      "modules/dag-l0/src/main/scala"
    )

  private def productionSourcesUnder(paths: String*): String =
    paths.iterator
      .flatMap { relative =>
        val root = repositoryRoot.resolve(relative)
        if (!Files.isDirectory(root)) Iterator.empty
        else {
          val stream = Files.walk(root)
          try stream.iterator().asScala.filter(path => Files.isRegularFile(path) && path.toString.endsWith(".scala")).toList.iterator
          finally stream.close()
        }
      }
      .toList
      .sortBy(_.toString)
      .map(path => Files.readString(path, StandardCharsets.UTF_8))
      .mkString("\n")

  private def boundedConcept(source: String, words: String*): Boolean =
    words.exists { word =>
      source.contains(s"max$word") ||
      source.contains(s"maximum$word") ||
      source.contains(s"bounded$word")
    }

  private def word(value: String): Regex =
    ("""(?s)\b""" + Regex.quote(value) + """\b""").r
}
