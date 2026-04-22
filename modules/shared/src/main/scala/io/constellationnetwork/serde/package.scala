package io.constellationnetwork

/** Serde typeclass layer.
  *
  * Three typeclasses, one per purpose (do NOT mix):
  *
  *   - [[serde.ImmutableCodec]] — canonical bytes, round-trippable, frozen forever per type. Also serves as signing input (hash and
  *     signature both derive from these bytes; no separate `Signable` typeclass).
  *   - [[serde.Persistable]] — disk bytes, may be compressed. Evolves via reindex.
  *   - [[serde.Transmittable]] — node-to-node wire bytes. Frozen within protocol version.
  *
  * Typeclass layer is intentionally not a subtype hierarchy. Each is a separate implicit, so the compiler refuses to pass wire bytes to a
  * signing function, for example. That enforcement is the whole point — it makes the Cosmos/Ethereum-style "signed over wire bytes" class
  * of consensus bug a compile error.
  *
  * Import `serde.implicits._` to get all syntax at once.
  *
  * Era coexistence — historical JSON / Kryo bytes are decoded via [[serde.legacy.LegacyBridgeSerde]]; those bridges are READ-ONLY by
  * design. No implicit conversion maps a legacy decoder back into the write path.
  *
  * See `.workspace/serde-design-notes.md` for the full design rationale.
  */
package object serde {

  /** Single import: `import io.constellationnetwork.serde.implicits._`. Brings `.immutableBytes`, `.persistedBytes`, `.transmittableBytes`
    * and their inverse decode syntax into scope at once.
    */
  object implicits extends ImmutableCodec.ImmutableCodecSyntax with Persistable.PersistableSyntax with Transmittable.TransmittableSyntax
}
