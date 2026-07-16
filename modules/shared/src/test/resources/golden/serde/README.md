# Golden vectors for the serde layer

Byte-level canary tests. Each file freezes the canonical ScodecV1 encoding of a
specific value of a specific consensus type.

## Policy

- **Never regenerate.** If a codec change would produce different bytes for the
  same Scala value, the golden test fails and that's the point — the refactor
  needs to be explicit (a new version type or ratified protocol era), not a silent mutation
  that invalidates every historical signature in the chain.
- **Add, don't mutate.** A ratified future protocol version adds a new type and
  golden file. The greenfield runtime does not retain undeployed compatibility
  decoders.
- **Hand-authored Scala values.** The expected value is committed alongside the
  golden bytes in the test file as `val sample: T = T(...)`. If the golden
  looks weird, the reviewer has the source value in plain Scala to cross-check.

## File naming

```
<TypeName>-scodec-v<N>.hex
```

- `<TypeName>`: the Scala type whose bytes are captured, e.g. `Balance`.
- `scodec`: the only encoding family in the greenfield runtime.
- `v<N>`: codec version. Bumped only when a new version of the type is introduced
  (e.g. a `BalanceV2` refactor ships its own `Balance-scodec-v2.hex`).

## File format

- Single-line lowercase hex, no whitespace, no leading `0x`, newline terminator.
- Decode with `scodec.bits.ByteVector.fromHexDescriptive` in the test.
- Example: `0123456789abcdef` for a `Balance` holding `0x0123456789ABCDEFL`.

## Versioning policy

- Adding a field to `Balance` is forbidden. A ratified future transition
  introduces `BalanceV2` as a distinct Scala type with its own codec and
  `Balance-scodec-v2.hex` golden.
- Upstream-v4 source fixtures are migration inputs, not active runtime codec
  eras. Their parser belongs in the isolated read-only importer and produces a
  verified ScodecV1 genesis manifest.
