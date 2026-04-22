package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HexContentCodec.{codec => hexCodec}

import scodec.Codec

/** Canonical scodec codec for `PeerId` — single-field wrapper around `Hex`.
  *
  * Same wire layout as `Id` (both wrap `Hex`) — length-prefixed raw bytes, not ASCII hex on the wire. Differs only in Scala-type identity.
  */
object PeerIdCodec {

  implicit val codec: Codec[PeerId] =
    hexCodec.xmap[PeerId](h => PeerId(h), (p: PeerId) => p.value: Hex)

  implicit val immutableCodec: ImmutableCodec[PeerId] = ImmutableCodec.fromScodecCodec(codec)
}
