package io.constellationnetwork.schema.consensus

/** Semantic authority claimed by a consensus artifact or signature domain.
  *
  * This is inventory metadata only. It does not encode bytes, authorize signing, or activate a protocol path.
  */
sealed trait ArtifactAuthority extends Product with Serializable {
  def semanticLabel: String
}

object ArtifactAuthority {
  case object StateValidity extends ArtifactAuthority {
    val semanticLabel: String = "state-validity"
  }

  case object FinalityQualification extends ArtifactAuthority {
    val semanticLabel: String = "finality-qualification"
  }

  case object SourceAuthorization extends ArtifactAuthority {
    val semanticLabel: String = "source-authorization"
  }

  case object Eligibility extends ArtifactAuthority {
    val semanticLabel: String = "eligibility"
  }

  case object CustodyAvailability extends ArtifactAuthority {
    val semanticLabel: String = "custody-availability"
  }

  case object ObjectiveEvidence extends ArtifactAuthority {
    val semanticLabel: String = "objective-evidence"
  }

  case object Commitment extends ArtifactAuthority {
    val semanticLabel: String = "commitment"
  }

  case object LocalDurability extends ArtifactAuthority {
    val semanticLabel: String = "local-durability"
  }

  case object MigrationGenesis extends ArtifactAuthority {
    val semanticLabel: String = "migration-genesis"
  }

  val all: List[ArtifactAuthority] = List(
    StateValidity,
    FinalityQualification,
    SourceAuthorization,
    Eligibility,
    CustodyAvailability,
    ObjectiveEvidence,
    Commitment,
    LocalDurability,
    MigrationGenesis
  )
}
