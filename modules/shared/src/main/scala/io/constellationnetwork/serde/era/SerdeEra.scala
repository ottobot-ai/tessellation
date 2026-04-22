package io.constellationnetwork.serde.era

/** A codec era — the dispatch key for choosing which serde family applies at a given snapshot ordinal.
  *
  * Sealed ADT so additions are reviewed (no stringly-typed eras, no open extension in consumer code).
  *
  * New eras: add a case here, wire it up in the `EraCodecRegistry` loader, and extend `hash-eras` in `application.conf`. No other code
  * should pattern-match on `SerdeEra` — all dispatch goes through the registry.
  */
sealed trait SerdeEra extends Product with Serializable {
  def name: String
}

object SerdeEra {

  /** Legacy era — Kryo bytes were written by pre-3.x nodes. Read-only via `legacy.KryoBridge`.
    */
  case object Kryo extends SerdeEra { val name = "kryo" }

  /** Legacy era — circe JSON bytes, the default for 3.x pre-scodec snapshots. Read-only via `legacy.JsonBridge`.
    */
  case object Json extends SerdeEra { val name = "json" }

  /** Modern era — hand-written scodec canonical codecs. Only writable era. Content-addressable hashing uses scodec-era bytes.
    */
  case object Scodec extends SerdeEra { val name = "scodec" }

  val all: List[SerdeEra] = List(Kryo, Json, Scodec)

  def fromName(n: String): Either[String, SerdeEra] =
    all.find(_.name == n).toRight(s"Unknown serde era '$n'; valid: ${all.map(_.name).mkString(", ")}")
}
