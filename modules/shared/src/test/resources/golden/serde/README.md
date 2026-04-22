# Golden vectors for the serde layer

Byte-level canary tests. Each file freezes the canonical encoding of a specific
value of a specific consensus type under a specific codec era.

## Policy

- **Never regenerate.** If a codec change would produce different bytes for the
  same Scala value, the golden test fails and that's the point — the refactor
  needs to be explicit (new version type or new era), not a silent mutation
  that invalidates every historical signature in the chain.
- **Add, don't mutate.** A new codec version adds a new golden file; the old
  file stays in place so re-sync tests prove backwards-compat on the read path.
- **Hand-authored Scala values.** The expected value is committed alongside the
  golden bytes in the test file as `val sample: T = T(...)`. If the golden
  looks weird, the reviewer has the source value in plain Scala to cross-check.

## File naming

```
<TypeName>-<era>-v<N>.hex
```

- `<TypeName>`: the Scala type whose bytes are captured, e.g. `Balance`.
- `<era>`: one of `scodec`, `json`, `kryo`. Matches `SerdeEra` values.
- `v<N>`: codec version. Bumped when a new version of the type is introduced
  (e.g. a `BalanceV2` refactor ships its own `Balance-scodec-v2.hex`).

## File format

- Single-line lowercase hex, no whitespace, no leading `0x`, newline terminator.
- Decode with `scodec.bits.ByteVector.fromHexDescriptive` in the test.
- Example: `0123456789abcdef` for a `Balance` holding `0x0123456789ABCDEFL`.

## How multi-version / multi-era coexistence works

- **Same type, different scodec-era versions:** adding a field to `Balance` is
  forbidden; instead, introduce `BalanceV2` as a new Scala type with its own
  codec instance and its own `Balance-scodec-v2` style of golden. The
  `EraCodecRegistry` is NOT involved — each `Balance` vs `BalanceV2` is a
  distinct compile-time type.
- **Same type, different eras:** if for some reason a type appears both in a
  pre-scodec ordinal range (JSON/Kryo era) and in the scodec era, commit a
  golden per era. Example hypothetical: `Balance-json-v1.hex` for re-sync of
  pre-scodec ordinals, `Balance-scodec-v1.hex` for the scodec-era write path.
  The `EraCodecRegistry` dispatches which decoder to use per ordinal.
