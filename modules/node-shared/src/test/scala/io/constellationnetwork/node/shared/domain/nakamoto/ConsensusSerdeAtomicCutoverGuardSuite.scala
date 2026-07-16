package io.constellationnetwork.node.shared.domain.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import weaver.SimpleIOSuite

/** Inverse cutover fuse for consensus byte authority.
  *
  * This suite freezes the reviewed JSON/Kryo signing, hashing, state-channel, and persistence surfaces while the Scodec migration is
  * incomplete. It rejects a piecemeal runtime activation: consensus must not begin selecting Scodec or directly extracting immutable bytes
  * until the complete atomic cutover removes/replaces the legacy inventory in one reviewed change.
  *
  * Passing this suite does NOT activate Scodec, prove byte equivalence, or complete SER-005. It proves only that the known authority
  * boundary has not changed without updating this fuse.
  */
object ConsensusSerdeAtomicCutoverGuardSuite extends SimpleIOSuite {

  private final case class Source(path: String, contents: String)
  private final case class Reviewed(count: Int, rationale: String)
  private final case class Marker(label: String, pattern: Regex, expected: Int, rationale: String)

  private val hasherPath =
    "modules/shared/src/main/scala/io/constellationnetwork/security/Hasher.scala"
  private val appPath =
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/app/TessellationIOApp.scala"
  private val dagL0MainPath =
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/Main.scala"
  private val snapshotLeaderPath =
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala"
  private val stateChannelServicePath =
    "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/services/StateChannelSnapshotService.scala"
  private val stateChannelProcessorPath =
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelEventsProcessor.scala"
  private val dataApplicationTraversePath =
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/dataApplication/DataApplicationTraverse.scala"
  private val currencySnapshotProcessorPath =
    "modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/domain/snapshot/programs/CurrencySnapshotProcessor.scala"
  private val localFileSystemStoragePath =
    "modules/shared/src/main/scala/io/constellationnetwork/storage/LocalFileSystemStorage.scala"
  private val snapshotStoragePath =
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotLocalFileSystemStorage.scala"
  private val snapshotInfoStoragePath =
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotInfoLocalFileSystemStorage.scala"
  private val combinedStoragePath =
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/CombinedSnapshotCheckpointFileSystemStorage.scala"
  private val snapshotDownloadStoragePath =
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/SnapshotDownloadStorage.scala"

  private val globalSnapshotSigning =
    """Signed\s*\.\s*forAsyncHasher\s*\[\s*[^,\]]+\s*,\s*Global(?:Incremental)?Snapshot\s*\]""".r

  /** Exact legacy byte-authority markers. These are liabilities to remove atomically, not APIs approved for expansion.
    */
  private val reviewedLegacyMarkers: Map[String, List[Marker]] = Map(
    hasherPath -> List(
      Marker("JSON hash selection", """case\s+JsonHash\s*=>\s*hasherJson""".r, 1, "ordinal-selected legacy snapshot hashing"),
      Marker("Kryo hash selection", """case\s+KryoHash\s*=>\s*hasherKryo""".r, 1, "ordinal-selected historical snapshot hashing"),
      Marker("JSON current hasher", """def\s+getCurrent\s*=\s*hasherJson""".r, 2, "current signing/hashing remains JSON-backed")
    ),
    appPath -> List(
      Marker("runtime HasherSelector construction", """HasherSelector\s*\.\s*forSync""".r, 1, "application-scoped JSON/Kryo hash selector"),
      Marker("runtime state-proof selector", """GlobalStateProofSelector\s*\(""".r, 1, "independent legacy state-proof era selector"),
      Marker("Kryo hash boundary input", """\blastKryoHashOrdinal\b""".r, 1, "configuration input for legacy hash selection")
    ),
    dagL0MainPath -> List(
      Marker("global snapshot signing", globalSnapshotSigning, 2, "genesis and first incremental signing use AsyncHasher")
    ),
    snapshotLeaderPath -> List(
      Marker("global snapshot signing", globalSnapshotSigning, 1, "leader signs the produced incremental through AsyncHasher")
    ),
    stateChannelServicePath -> List(
      Marker(
        "currency snapshot JSON payload",
        """(?s)JsonSerializer\s*\[\s*F\s*\]\s*\.\s*serialize\s*\(\s*snapshot\s*\)""".r,
        2,
        "currency genesis/incremental state-channel payload bytes are JSON"
      ),
      Marker(
        "state-channel binary signing",
        """\.\s*sign\s*\(\s*keyPair\s*\)""".r,
        2,
        "outer state-channel signatures bind the currently serialized binary"
      )
    ),
    stateChannelProcessorPath -> List(
      Marker(
        "GL0 state-channel JSON decode",
        """(?s)JsonSerializer\s*\[\s*F\s*\]\s*\.\s*deserialize\s*\[\s*A\s*\]\s*\(\s*binary\s*\.\s*value\s*\.\s*content\s*\)""".r,
        1,
        "GL0 generic state-channel content decode remains JSON"
      )
    ),
    dataApplicationTraversePath -> List(
      Marker(
        "node-shared currency incremental JSON decode",
        """(?s)JsonSerializer\s*\[\s*F\s*\]\s*\.\s*deserialize\s*\[\s*Signed\s*\[\s*CurrencyIncrementalSnapshot\s*\]\s*\]\s*\(\s*binary\s*\.\s*content\s*\)""".r,
        1,
        "framework currency payload traversal remains JSON"
      )
    ),
    currencySnapshotProcessorPath -> List(
      Marker(
        "CL1 currency incremental JSON decode",
        """(?s)JsonSerializer\s*\[\s*F\s*\]\s*\.\s*deserialize\s*\[\s*Signed\s*\[\s*CurrencyIncrementalSnapshot\s*\]\s*\]\s*\(\s*binary\s*\.\s*content\s*\)""".r,
        1,
        "CL1 downstream currency snapshot processing remains JSON"
      )
    ),
    localFileSystemStoragePath -> List(
      Marker(
        "generic snapshot JSON read",
        """JsonSerializer\s*\[\s*F\s*\]\s*\.\s*deserialize\s*\[\s*A\s*\]\s*\(\s*bytes\s*\)""".r,
        1,
        "primary local persistence read is JSON"
      ),
      Marker(
        "generic snapshot JSON write",
        """JsonSerializer\s*\[\s*F\s*\]\s*\.\s*serialize\s*\(\s*a\s*\)""".r,
        1,
        "local persistence writes remain JSON"
      )
    ),
    snapshotStoragePath -> List(
      Marker(
        "snapshot Kryo fallback",
        """(?s)KryoSerializer\s*\[\s*F\s*\]\s*\.\s*deserialize""".r,
        3,
        "historical global/global-incremental/currency-incremental disk fallback"
      )
    ),
    snapshotInfoStoragePath -> List(
      Marker(
        "snapshot-info Kryo fallback",
        """(?s)KryoSerializer\s*\[\s*F\s*\]\s*\.\s*deserialize""".r,
        3,
        "historical global/global-v2/currency snapshot-info disk fallback"
      )
    ),
    combinedStoragePath -> List(
      Marker(
        "combined checkpoint JSON write",
        """printer\s*\.\s*unsafePrintToAppendable\s*\([^)]*\.asJson\s*,""".r,
        2,
        "combined snapshot and state checkpoint writes remain JSON"
      )
    ),
    snapshotDownloadStoragePath -> List(
      Marker(
        "download hash-era selection",
        """hashSelect\s*\.\s*select\s*\(\s*ordinal\s*\)""".r,
        2,
        "download proof/state selection still branches over JSON and Kryo history"
      )
    )
  )

  private val checkpointPreimageHash =
    """(?s)\b(?:Hasher\s*\[\s*F\s*\]|hasher)\s*\.\s*hash\s*\([^)]*\bsigningPreimage\b""".r

  private val reviewedCheckpointPreimageHashes: Map[String, Reviewed] = Map(
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala" ->
      Reviewed(1, "GL0 checkpoint acceptance derives the canonical checkpoint hash"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala" ->
      Reviewed(1, "catch-up derives the checkpoint hash before branch integration"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardChainStore.scala" ->
      Reviewed(1, "the shard store derives its checkpoint identifier"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardCheckpointProducerDutyValidator.scala" ->
      Reviewed(1, "producer-duty validation authenticates the exact parent"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardSubtreeProofService.scala" ->
      Reviewed(2, "subtree proof construction and verification bind checkpoint identifiers"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofValidator.scala" ->
      Reviewed(1, "fraud-proof validation binds the disputed checkpoint"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationValidator.scala" ->
      Reviewed(4, "equivocation validation compares both checkpoint children in two paths"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointChainStoreRecovery.scala" ->
      Reviewed(2, "recovery authenticates the received and stored checkpoint identifiers"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointProducer.scala" ->
      Reviewed(4, "production, held-candidate comparison, emission, and signed recovery derive checkpoint identifiers"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/WatchtowerFraudProofEmitter.scala" ->
      Reviewed(2, "watchtower binds both the disputed checkpoint and its fraud-proof preimage"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ShardCheckpointGl0AcceptanceManager.scala" ->
      Reviewed(2, "checkpoint validation derives identifiers on both acceptance paths")
  )

  private val evidencePreimageHash =
    """Hasher\s*\[\s*F\s*\]\s*\.\s*hash\s*\(\s*preimage\s*\)""".r

  private val reviewedEvidencePreimageHashes: Map[String, Reviewed] = Map(
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofValidator.scala" ->
      Reviewed(1, "fraud-proof signature verification hashes its explicit evidence preimage"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/SlashableEvidenceValidator.scala" ->
      Reviewed(2, "slashable evidence signing and verification hash the explicit evidence preimage")
  )

  private val ordinalHasherSelection =
    """(?:HasherSelector\s*\[\s*F\s*\]|\bselector)\s*\.\s*(?:forOrdinal|getForOrdinal)\s*\(""".r

  /** Every live call that lets a snapshot ordinal choose JSON versus Kryo bytes.
    *
    * These calls must disappear in the same change that installs the branch-bound active-era service. Moving one call to current JSON or
    * Scodec while another still selects by ordinal is not a migration.
    */
  private val reviewedOrdinalHasherSelections: Map[String, Reviewed] = Map(
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoChainStore.scala" ->
      Reviewed(1, "exact branch walking checks the stored snapshot against ordinal-selected legacy hash authority"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoChainStoreExactReplayHistorySource.scala" ->
      Reviewed(1, "exact replay-history verification compares current and ordinal-selected legacy hash authority"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/TowerCatchupCoordinator.scala" ->
      Reviewed(1, "tower catch-up rehashes a historical snapshot with ordinal-selected legacy bytes"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/HistoricalOperatorConsensusKeyRegistry.scala" ->
      Reviewed(1, "historical KES registration references use the accepting ordinal's legacy hash authority"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/GlobalSnapshotContextFunctions.scala" ->
      Reviewed(2, "snapshot context reconstruction selects legacy bytes for parent tips and parent identity"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala" ->
      Reviewed(1, "global acceptance selects the candidate ordinal's legacy hashing authority")
  )

  private val currentHasherPromotion =
    """(?s)HasherSelector\s*\[\s*F\s*\]\s*\.\s*withCurrent\s*\{.{0,512}?\bmovePersistedToTmp\s*\(""".r

  private val reviewedCurrentHasherPromotions: Map[String, Reviewed] = Map(
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotLocalFileSystemStorage.scala" ->
      Reviewed(1, "snapshot disk promotion validates under the current JSON-backed hasher authority")
  )

  private val directHashSelect =
    """\bhashSelect\s*\.\s*select\s*\(""".r

  private val reviewedDirectHashSelections: Map[String, Reviewed] = Map(
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/SnapshotDownloadStorage.scala" ->
      Reviewed(2, "download state/info reconstruction directly branches between JSON and Kryo by ordinal"),
    hasherPath ->
      Reviewed(1, "HasherSelector itself converts the local ordinal boundary into consensus byte authority")
  )

  private val directStateProofSelection =
    """\b(?:globalStateProofSelector|stateProofSelector|selector)\s*\.\s*select\s*\(\s*ordinal\s*\)""".r

  private val reviewedDirectStateProofSelections: Map[String, Reviewed] = Map(
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala" ->
      Reviewed(1, "acceptance branches state-proof validation on a locally selected ordinal format"),
    "modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala" ->
      Reviewed(2, "state-proof construction directly selects legacy Merkle versus MPT format by ordinal")
  )

  private val legacySnapshotProjection =
    """\b(?:CurrencyIncrementalSnapshotV1\s*\.\s*fromCurrencyIncrementalSnapshot|CurrencySnapshotInfoV1\s*\.\s*fromCurrencySnapshotInfo|GlobalIncrementalSnapshotV1\s*\.\s*fromGlobalIncrementalSnapshot|GlobalSnapshotInfoV2\s*\.\s*fromGlobalSnapshotInfo|GlobalSnapshotStateProofV1\s*\.\s*fromGlobalSnapshotStateProof|CurrencySnapshotStateProofV1\s*\.\s*fromCurrencySnapshotStateProof)|(?<!FollowVerifyCore)\.\s*(?:toGlobalSnapshotInfo|toCurrencySnapshotInfo|toCurrencyIncrementalSnapshot)\b""".r

  /** Projections that erase active fields or resurrect legacy full-state shapes on live paths.
    *
    * Schema conversion helpers are included because leaving one reachable after the active cutover would preserve a second byte/state
    * interpretation even if the top-level selector disappeared.
    */
  private val reviewedLegacySnapshotProjections: Map[String, Reviewed] = Map(
    "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/programs/Genesis.scala" ->
      Reviewed(1, "ML0 genesis converts the retained full currency snapshot into runtime state"),
    "modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/domain/snapshot/programs/CurrencySnapshotProcessor.scala" ->
      Reviewed(1, "currency follower processing converts a retained legacy full snapshot shape"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/Main.scala" ->
      Reviewed(1, "GL0 genesis wiring converts the current full snapshot into runtime state"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/domain/snapshot/programs/Download.scala" ->
      Reviewed(1, "GL0 download converts a retained full snapshot shape"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusFunctions.scala" ->
      Reviewed(1, "GL0 proposal construction hashes a field-erasing GlobalIncrementalSnapshotV1 projection"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotTraverse.scala" ->
      Reviewed(2, "rollback traversal converts full state and projects current state into the legacy proof shape"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/SnapshotDownloadStorage.scala" ->
      Reviewed(2, "download storage converts Kryo-era state and carries a legacy/current disjunction"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoChainStore.scala" ->
      Reviewed(1, "chain-store reconstruction converts a retained full snapshot shape"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/overlay/AcceptanceMptStateChanges.scala" ->
      Reviewed(1, "MPT state-change extraction converts a retained full currency snapshot info shape"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotValidator.scala" ->
      Reviewed(2, "currency signature validation retries after projecting away and then restoring active snapshot fields"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala" ->
      Reviewed(2, "global acceptance projects current incrementals and info into the legacy Merkle commitment shape"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelEventsProcessor.scala" ->
      Reviewed(1, "state-channel processing converts a retained full currency snapshot into runtime info"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotInfoLocalFileSystemStorage.scala" ->
      Reviewed(4, "snapshot-info persistence projects and promotes retained legacy shapes"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotLocalFileSystemStorage.scala" ->
      Reviewed(1, "snapshot persistence promotes retained V1 currency incrementals"),
    "modules/shared/src/main/scala/io/constellationnetwork/currency/dataApplication/storage/TraverseLocalFileSystemTempStorage.scala" ->
      Reviewed(1, "data-application temp traversal promotes a retained V1 currency incremental"),
    "modules/shared/src/main/scala/io/constellationnetwork/currency/schema/currency.scala" ->
      Reviewed(1, "the retained full currency snapshot projects its state proof into V1"),
    "modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalIncrementalSnapshot.scala" ->
      Reviewed(2, "global incremental V1 conversion projects both state proof and retained full state"),
    "modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshot.scala" ->
      Reviewed(1, "full snapshot helpers define the current runtime-state conversion"),
    "modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala" ->
      Reviewed(5, "the current state schema retains legacy state-proof, currency incremental, and currency-info projections"),
    "modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala" ->
      Reviewed(5, "MPT construction converts retained full currency snapshot info shapes"),
    hasherPath ->
      Reviewed(2, "Kryo hashing projects current global and currency state into field-erasing legacy shapes")
  )

  private val liveKryoDeserialize =
    """\bKryoSerializer\s*(?:\[[^]]+\])?\s*\.\s*deserialize""".r

  private val reviewedLiveKryoDeserializers: Map[String, Reviewed] = Map(
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotInfoLocalFileSystemStorage.scala" ->
      Reviewed(3, "snapshot-info disk reads probe retained Kryo formats"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotLocalFileSystemStorage.scala" ->
      Reviewed(3, "snapshot disk reads probe retained Kryo formats"),
    "modules/shared/src/main/scala/io/constellationnetwork/currency/dataApplication/storage/TraverseLocalFileSystemTempStorage.scala" ->
      Reviewed(1, "data-application temp traversal promotes a Kryo fallback"),
    "modules/shared/src/main/scala/io/constellationnetwork/ext/kryo.scala" ->
      Reviewed(2, "generic Kryo syntax exposes live deserialize entry points")
  )

  private val mptJsonCommitmentHash =
    """(?s)\bHasher\s*\[\s*F\s*\]\s*\.\s*prefixedHash\s*\(\s*[^,\n]+\.asJson\s*,""".r

  private val reviewedMptJsonCommitmentHashes: Map[String, Reviewed] = Map(
    "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/MerklePatriciaNode.scala" ->
      Reviewed(3, "live leaf, branch, and extension construction hashes JSON commitments"),
    "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/prover/MerklePatriciaAbsenceProver.scala" ->
      Reviewed(3, "live absence proof construction reproduces JSON commitment hashes"),
    "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/prover/MerklePatriciaSingleInclusionProver.scala" ->
      Reviewed(3, "live inclusion proof construction reproduces JSON commitment hashes"),
    "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/verifier/MerklePatriciaAbsenceVerifier.scala" ->
      Reviewed(5, "live absence verification reproduces JSON commitment hashes on every proof shape"),
    "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/verifier/MerklePatriciaBatchInclusionVerifier.scala" ->
      Reviewed(3, "live batch inclusion verification reproduces JSON commitment hashes"),
    "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/verifier/MerklePatriciaInclusionVerifier.scala" ->
      Reviewed(3, "live inclusion verification reproduces JSON commitment hashes"),
    "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/verifier/MerklePatriciaRangeVerifier.scala" ->
      Reviewed(3, "live range verification reproduces JSON commitment hashes")
  )

  private val directImmutableBytes = """\b(?:immutableBytes|fromImmutableBytes)\b""".r

  private val reviewedDirectImmutableBytes: Map[String, Reviewed] = Map(
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala" ->
      Reviewed(
        1,
        "encodes an InvalidStateProofSlashedReader MPT value for replay comparison; it is not artifact/signing byte activation"
      ),
    "modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala" ->
      Reviewed(
        7,
        "encodes typed MPT leaf values on the already-live MPT state path; it does not select artifact/signature/hash preimages"
      )
  )

  private val signedImplementationPath =
    "modules/shared/src/main/scala/io/constellationnetwork/security/signature/Signed.scala"

  private val protocolEraIdentityPath =
    "modules/shared/src/main/scala/io/constellationnetwork/schema/era/ProtocolEraId.scala"
  private val protocolEraCodecPath =
    "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/ProtocolEraIdCodec.scala"

  private val mptCommitmentScodecV1CodecPath =
    "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/MerklePatriciaCommitmentScodecV1Codec.scala"

  private val reviewedProtocolEraIdentityPaths: Set[String] =
    Set(protocolEraIdentityPath, protocolEraCodecPath)

  private val protocolEraIdentityReference =
    """\bProtocolEraId\b|\bio\.constellationnetwork\.schema\.era\b""".r

  private val mptCommitmentScodecV1Reference =
    """\bMerklePatriciaCommitmentScodecV1Codec\b|\b(?:ImmutableCodec|Codec)\s*\[\s*MerklePatriciaCommitment\s*\]""".r

  private val consensusSensitivePaths: Set[String] =
    List(
      reviewedLegacyMarkers.keySet,
      reviewedCheckpointPreimageHashes.keySet,
      reviewedEvidencePreimageHashes.keySet,
      reviewedOrdinalHasherSelections.keySet,
      reviewedCurrentHasherPromotions.keySet,
      reviewedDirectHashSelections.keySet,
      reviewedDirectStateProofSelections.keySet,
      reviewedLegacySnapshotProjections.keySet,
      reviewedLiveKryoDeserializers.keySet,
      reviewedMptJsonCommitmentHashes.keySet,
      reviewedDirectImmutableBytes.keySet
    ).foldLeft(Set(signedImplementationPath))(_ union _)

  private val forbiddenRuntimeActivations: List[(String, Regex)] = List(
    "ScodecHash runtime selection" -> """\bScodecHash\b""".r,
    "Hasher.forScodec runtime selection" -> """\bHasher\s*\.\s*forScodec\b""".r
  )

  test("runtime Scodec activation remains absent") {
    productionSources.map { sources =>
      val violations = sources.flatMap { source =>
        activationLabels(source).toList.sorted.map(label => s"${source.path}: $label")
      }

      if (violations.isEmpty) success
      else failure(s"partial Scodec activation crossed the atomic-cutover fuse: ${violations.mkString(", ")}")
    }
  }

  test("ProtocolEraId remains dark outside its reviewed identity and codec sources") {
    productionSources.map { sources =>
      val actualPaths = sources.collect {
        case source if protocolEraIdentityReference.findFirstIn(withoutScalaComments(source.contents)).nonEmpty => source.path
      }.toSet
      val unexpected = actualPaths -- reviewedProtocolEraIdentityPaths
      val missing = reviewedProtocolEraIdentityPaths -- actualPaths

      if (unexpected.isEmpty && missing.isEmpty) success
      else
        failure(
          s"ProtocolEraId production reachability changed: unexpected=${unexpected.toList.sorted.mkString(",")} " +
            s"missing=${missing.toList.sorted.mkString(",")}"
        )
    }
  }

  test("MPT commitment ScodecV1 bytes remain dark outside their standalone codec source") {
    productionSources.map { sources =>
      val actualPaths = sources.collect {
        case source if mptCommitmentScodecV1Reference.findFirstIn(withoutScalaComments(source.contents)).nonEmpty => source.path
      }.toSet
      val expectedPaths = Set(mptCommitmentScodecV1CodecPath)
      val unexpected = actualPaths -- expectedPaths
      val missing = expectedPaths -- actualPaths

      if (unexpected.isEmpty && missing.isEmpty) success
      else
        failure(
          s"MPT commitment ScodecV1 production reachability changed: unexpected=${unexpected.toList.sorted.mkString(",")} " +
            s"missing=${missing.toList.sorted.mkString(",")}"
        )
    }
  }

  test("legacy consensus byte-authority markers remain an exact atomic-cutover inventory") {
    productionSources.map { sources =>
      val mismatches = reviewedMarkerMismatches(sources, reviewedLegacyMarkers)

      if (mismatches.isEmpty) success
      else failure(s"legacy byte-authority inventory changed: ${mismatches.mkString(", ")}")
    }
  }

  test("checkpoint and evidence preimage hashing remains an exact reviewed inventory") {
    productionSources.map { sources =>
      val checkpointMismatches =
        exactInventoryMismatches(sources, checkpointPreimageHash, reviewedCheckpointPreimageHashes, "checkpoint preimage hash")
      val evidenceMismatches =
        exactInventoryMismatches(sources, evidencePreimageHash, reviewedEvidencePreimageHashes, "evidence preimage hash")
      val mismatches = checkpointMismatches ++ evidenceMismatches

      if (mismatches.isEmpty) success
      else failure(s"reviewed consensus preimage inventory changed: ${mismatches.mkString(", ")}")
    }
  }

  test("ordinal-selected hash and state-proof authority remains an exact atomic-cutover inventory") {
    productionSources.map { sources =>
      val mismatches = List(
        exactInventoryMismatches(sources, ordinalHasherSelection, reviewedOrdinalHasherSelections, "ordinal hasher selection"),
        exactInventoryMismatches(sources, currentHasherPromotion, reviewedCurrentHasherPromotions, "current-hasher promotion"),
        exactInventoryMismatches(sources, directHashSelect, reviewedDirectHashSelections, "direct hash selection"),
        exactInventoryMismatches(
          sources,
          directStateProofSelection,
          reviewedDirectStateProofSelections,
          "direct state-proof selection"
        )
      ).flatten

      if (mismatches.isEmpty) success
      else failure(s"ordinal-selected consensus authority inventory changed: ${mismatches.mkString(", ")}")
    }
  }

  test("legacy snapshot projections and Kryo promotion remain an exact atomic-cutover inventory") {
    productionSources.map { sources =>
      val mismatches = List(
        exactInventoryMismatches(sources, legacySnapshotProjection, reviewedLegacySnapshotProjections, "legacy snapshot projection"),
        exactInventoryMismatches(sources, liveKryoDeserialize, reviewedLiveKryoDeserializers, "live Kryo deserialize")
      ).flatten

      if (mismatches.isEmpty) success
      else failure(s"legacy projection/promotion inventory changed: ${mismatches.mkString(", ")}")
    }
  }

  test("live MPT commitment hashing remains an exact JSON authority inventory") {
    productionSources.map { sources =>
      val mismatches =
        exactInventoryMismatches(sources, mptJsonCommitmentHash, reviewedMptJsonCommitmentHashes, "MPT JSON commitment hash")

      if (mismatches.isEmpty) success
      else failure(s"live MPT commitment byte authority changed: ${mismatches.mkString(", ")}")
    }
  }

  test("sensitive consensus surfaces have no unreviewed direct immutable-byte activation") {
    productionSources.map { sources =>
      val byPath = sources.map(source => source.path -> source).toMap
      val mismatches = consensusSensitivePaths.toList.sorted.flatMap { path =>
        byPath.get(path) match {
          case None => List(s"missing sensitive source=$path")
          case Some(source) =>
            val actual = directImmutableBytes.findAllIn(withoutScalaComments(source.contents)).size
            val expected = reviewedDirectImmutableBytes.get(path).fold(0)(_.count)
            if (actual == expected) Nil else List(s"$path expected=$expected actual=$actual")
        }
      }
      val staleAllowances = reviewedDirectImmutableBytes.keySet -- consensusSensitivePaths
      val emptyRationales = reviewedDirectImmutableBytes.collect {
        case (path, Reviewed(_, rationale)) if rationale.trim.isEmpty => path
      }.toList.sorted
      val allMismatches = mismatches ++
        staleAllowances.toList.sorted.map(path => s"allowance is not sensitive=$path") ++
        emptyRationales.map(path => s"empty allowance rationale=$path")

      if (allMismatches.isEmpty) success
      else failure(s"direct immutable-byte inventory changed: ${allMismatches.mkString(", ")}")
    }
  }

  pureTest("activation parser distinguishes comments, ordinary codecs, and unsafe activation") {
    val sensitivePath =
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala"
    val commentOnly = Source(
      sensitivePath,
      """// Hasher.forScodec is not active
        |/* ScodecHash */
        |Hasher[F].hash(checkpoint.signingPreimage)
        |""".stripMargin
    )
    val ordinaryCodec = Source(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/ExampleCodec.scala",
      "codec.immutableBytes(value)"
    )
    val partialActivation = Source(
      sensitivePath,
      """val mode = ScodecHash
        |val hasher = Hasher.forScodec[F]
        |checkpoint.signingPreimage.immutableBytes
        |""".stripMargin
    )

    expect(activationLabels(commentOnly).isEmpty) &&
    expect(activationLabels(ordinaryCodec).isEmpty) &&
    expect(
      activationLabels(partialActivation) == Set(
        "ScodecHash runtime selection",
        "Hasher.forScodec runtime selection",
        "direct immutable-byte activation"
      )
    )
  }

  pureTest("exact inventory parser rejects missing, added, and unreviewed authority sites") {
    val pattern = """authority\s*\(""".r
    val reviewed = Map("A.scala" -> Reviewed(2, "synthetic exact-count fixture"))
    val exact = List(Source("A.scala", "authority(x); /* authority(comment) */ authority(y)"))
    val missing = List(Source("A.scala", "authority(x)"))
    val added = List(Source("A.scala", "authority(x); authority(y); authority(z)"))
    val unreviewed = exact :+ Source("B.scala", "authority(z)")

    expect(exactInventoryMismatches(exact, pattern, reviewed, "synthetic").isEmpty) &&
    expect(exactInventoryMismatches(missing, pattern, reviewed, "synthetic").nonEmpty) &&
    expect(exactInventoryMismatches(added, pattern, reviewed, "synthetic").nonEmpty) &&
    expect(exactInventoryMismatches(unreviewed, pattern, reviewed, "synthetic").nonEmpty)
  }

  private def activationLabels(source: Source): Set[String] = {
    val stripped = withoutScalaComments(source.contents)
    val runtimeLabels =
      forbiddenRuntimeActivations.collect {
        case (label, pattern) if pattern.findFirstIn(stripped).nonEmpty => label
      }.toSet
    val directImmutableCount = directImmutableBytes.findAllIn(stripped).size
    val reviewedDirectImmutableCount = reviewedDirectImmutableBytes.get(source.path).fold(0)(_.count)
    val immutableLabel =
      if (consensusSensitivePaths.contains(source.path) && directImmutableCount > reviewedDirectImmutableCount)
        Set("direct immutable-byte activation")
      else Set.empty[String]

    runtimeLabels ++ immutableLabel
  }

  private def reviewedMarkerMismatches(
    sources: List[Source],
    reviewed: Map[String, List[Marker]]
  ): List[String] = {
    val byPath = sources.map(source => source.path -> source).toMap

    reviewed.toList.sortBy(_._1).flatMap {
      case (path, markers) =>
        byPath.get(path) match {
          case None => List(s"missing reviewed source=$path")
          case Some(source) =>
            val stripped = withoutScalaComments(source.contents)
            markers.flatMap { marker =>
              val actual = marker.pattern.findAllIn(stripped).size
              val countMismatch =
                if (actual == marker.expected) Nil
                else List(s"$path ${marker.label} expected=${marker.expected} actual=$actual")
              val metadataMismatch =
                if (marker.label.trim.nonEmpty && marker.rationale.trim.nonEmpty) Nil
                else List(s"$path has an empty marker label/rationale")
              countMismatch ++ metadataMismatch
            }
        }
    }
  }

  private def exactInventoryMismatches(
    sources: List[Source],
    pattern: Regex,
    reviewed: Map[String, Reviewed],
    label: String
  ): List[String] = {
    val actualCounts = sources.iterator
      .map(source => source.path -> pattern.findAllIn(withoutScalaComments(source.contents)).size)
      .filter { case (_, count) => count > 0 }
      .toMap
    val expectedCounts = reviewed.view.mapValues(_.count).toMap
    val unexpected = actualCounts.keySet -- expectedCounts.keySet
    val stale = expectedCounts.keySet -- actualCounts.keySet
    val changed = actualCounts.keySet.intersect(expectedCounts.keySet).toList.sorted.collect {
      case path if actualCounts(path) != expectedCounts(path) =>
        s"$label $path expected=${expectedCounts(path)} actual=${actualCounts(path)}"
    }
    val emptyRationales = reviewed.collect {
      case (path, Reviewed(_, rationale)) if rationale.trim.isEmpty => path
    }.toList.sorted

    unexpected.toList.sorted.map(path => s"unexpected $label=$path count=${actualCounts(path)}") ++
      stale.toList.sorted.map(path => s"stale $label=$path") ++
      changed ++
      emptyRationales.map(path => s"empty $label rationale=$path")
  }

  /** Removes line and nested block comments without treating comment delimiters inside literals as syntax. Literal contents remain
    * searchable so reflective/string-based activation still crosses the fuse.
    */
  private def withoutScalaComments(input: String): String = {
    val out = new StringBuilder(input.length)
    var index = 0
    var blockDepth = 0
    var inLineComment = false
    var inString = false
    var inTripleString = false
    var inCharacter = false
    var escaped = false

    while (index < input.length) {
      val current = input.charAt(index)
      val next = if (index + 1 < input.length) input.charAt(index + 1) else '\u0000'

      if (inLineComment) {
        if (current == '\n') {
          inLineComment = false
          out.append(current)
        }
        index += 1
      } else if (blockDepth > 0) {
        if (current == '/' && next == '*') {
          blockDepth += 1
          index += 2
        } else if (current == '*' && next == '/') {
          blockDepth -= 1
          index += 2
        } else {
          if (current == '\n') out.append(current)
          index += 1
        }
      } else if (inTripleString) {
        if (current == '"' && input.startsWith("\"\"\"", index)) {
          out.append("\"\"\"")
          index += 3
          inTripleString = false
        } else {
          out.append(current)
          index += 1
        }
      } else if (inString) {
        out.append(current)
        index += 1
        if (escaped) escaped = false
        else if (current == '\\') escaped = true
        else if (current == '"') inString = false
      } else if (inCharacter) {
        out.append(current)
        index += 1
        if (escaped) escaped = false
        else if (current == '\\') escaped = true
        else if (current == '\'') inCharacter = false
      } else if (current == '/' && next == '/') {
        inLineComment = true
        index += 2
      } else if (current == '/' && next == '*') {
        blockDepth = 1
        index += 2
      } else if (current == '"' && input.startsWith("\"\"\"", index)) {
        out.append("\"\"\"")
        index += 3
        inTripleString = true
      } else {
        out.append(current)
        index += 1
        if (current == '"') inString = true
        else if (current == '\'') inCharacter = true
      }
    }

    out.result()
  }

  private def productionSources: IO[List[Source]] =
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      val stream = Files.walk(root.resolve("modules"))
      try
        stream
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .filter(_.getFileName.toString.endsWith(".scala"))
          .filter(_.toString.replace('\\', '/').contains("/src/main/scala/"))
          .map { path =>
            val relative = root.relativize(path).toString.replace('\\', '/')
            Source(relative, new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
          }
          .toList
      finally stream.close()
    }

  @annotation.tailrec
  private def repositoryRoot(candidate: Path): Path =
    if (Files.isDirectory(candidate.resolve("modules/node-shared")) && Files.isRegularFile(candidate.resolve("build.sbt"))) candidate
    else
      Option(candidate.getParent) match {
        case Some(parent) => repositoryRoot(parent)
        case None         => throw new IllegalStateException(s"Unable to locate repository root from ${sys.props("user.dir")}")
      }
}
