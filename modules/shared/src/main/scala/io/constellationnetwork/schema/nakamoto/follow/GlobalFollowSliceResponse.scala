package io.constellationnetwork.schema.nakamoto.follow

import cats.Eq

import io.constellationnetwork.schema.SnapshotOrdinal

import io.circe._
import io.circe.syntax._

/** gl0 → gl1 follow-slice TRANSPORT payload (Axis 2 — see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * What gl0 serves over HTTP for the latest-finalized consumed-field slice, and what a gl1 follower fetches. Per the locked "Transfer model"
  * (design point 1): the `MptOverlay` cannot read value-bytes at an arbitrary historical ordinal, so there is no per-ordinal historical-diff
  * fetch. Instead gl1 fetches the LATEST-FINALIZED full slice (as a [[ConsumedFieldDelta]] delta-from-empty — every consumed-field entry an
  * upsert, removals empty, projected from gl0's finalized `GlobalSnapshotInfo` by
  * [[io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowSliceService.latestSlice]]) and recompute-matches it against the signed
  * snapshot's `stateProof.<field>Proof` roots (field-root equality).
  *
  *   - `ordinal` — the finalized global ordinal `slice` was read at. The follower uses it to pick the matching signed snapshot to recompute
  *     the field roots against; the cryptographic anchor is that snapshot's roots, not this number.
  *   - `slice` — the full consumed-field slice at `ordinal`, expressed as an Address-keyed [[ConsumedFieldDelta]] of all-upserts. The follower's
  *     verifier forward-hashes each `(Address, value)` to its MPT leaf the way gl0's writer does (`toHex(hypergraph(field, address))` +
  *     `immutableBytes(value)`), reproducing gl0's leaf digests and therefore gl0's subtree roots.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`): fresh payload for the follow transport, no compat ceremony. Field
  * order is the protocol contract once the route + client are load-bearing — add a field = bump the case class explicitly, same discipline as
  * [[GlobalFollowProof]].
  */
final case class GlobalFollowSliceResponse(
  ordinal: SnapshotOrdinal,
  slice: ConsumedFieldDelta
)

object GlobalFollowSliceResponse {

  // Both members already carry full circe codecs (`SnapshotOrdinal` in its companion; `ConsumedFieldDelta`
  // in `FollowVerifyCore`'s file). Encode the envelope by hand — same explicit-object style as
  // `GlobalFollowProof` — so the wire field names are an intentional, stable contract rather than a
  // derivation artifact.
  implicit val encoder: Encoder[GlobalFollowSliceResponse] = (r: GlobalFollowSliceResponse) =>
    Json.obj(
      "ordinal" -> r.ordinal.asJson,
      "slice" -> r.slice.asJson
    )

  implicit val decoder: Decoder[GlobalFollowSliceResponse] = (c: HCursor) =>
    for {
      ordinal <- c.downField("ordinal").as[SnapshotOrdinal]
      slice <- c.downField("slice").as[ConsumedFieldDelta]
    } yield GlobalFollowSliceResponse(ordinal, slice)

  // Structural Eq via the canonical JSON encoding (byte-stable: `SnapshotOrdinal` is a number, `slice`'s Eq
  // is already JSON-based and uses sorted maps). For test assertions / dedup, never control flow — same
  // approach as `GlobalFollowProof` / `ConsumedFieldDelta`.
  implicit val eq: Eq[GlobalFollowSliceResponse] = Eq.by[GlobalFollowSliceResponse, Json](_.asJson)
}
