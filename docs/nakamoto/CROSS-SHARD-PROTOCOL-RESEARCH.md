# Cross-shard interaction protocols — production-blockchain survey + Tessellation recommendation

**Status:** research document. Survey + recommendation. No code commitment.
Written 2026-05-22 against `feature/serde-typeclass-shim`.

**Scope:** survey of how production blockchains handle cross-shard / cross-chain
interactions, plus a recommendation for the cross-shard primitive Tessellation
should adopt for its hierarchical execution-sharding design. Bounded by the
existing design context in
[`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md):
Option A (VRF-sortition) + Option C (slashing) are already chosen; this
document addresses the *cross-shard data-dependency* question that arises
*after* those defences are in place.

**Out of scope:** Tessellation implementation details (this is a primitive-
shape proposal; the implementation surface is downstream); changes to the
metagraph submission pipeline (out of scope per the 2026-05-15 user
directive, see `CROSS-SHARD-MITIGATION-PROPOSAL.md` §5.2); cross-shard
NIPoPoW composition (out of scope per [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md)).

---

## §0 Problem framing

Under the chosen sharding model
([`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md)
§2.7 + `:project_sharding_direction_clarified`), each metagraph is
deterministically mapped to one shard, and shard members are the sole
re-executors of that metagraph's currency-layer transitions. gl0 verifies
state-proofs + committee signatures rather than re-executing. Per-operator
storage is bounded at `(gl0) + (one metagraph's full state) + (S × 32-byte
subtree-root stubs)`.

The cross-shard *data-dependency* question:
[`SpendActionValidator`](../../modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/SpendActionValidator.scala)
(line 13 imports `SpendAction, SpendTransaction`; line 15 imports `AllowSpend`)
inspects an `activeAllowSpends` map keyed by `(currencyId, source)` —
meaning a single validation pass touches state from multiple metagraphs.
A `SpendTransaction` originating from one metagraph may consume an
`AllowSpend` recorded by another. With execution sharded, those two
metagraphs may sit in different shards. The shard processing the
`SpendTransaction` does **not** hold the originating metagraph's full
state — only its 32-byte subtree-root stub.

So: when a tx in shard 1 requires state from shard 2 to validate, what
does shard 1's committee do? The user's suggested shape is "request
state + inclusion proof". This document surveys how production systems
solve the analogous problem, compares the candidate primitives, and
recommends a v1 shape.

A worked counter-example to keep in mind: the current `priorBalances`
merge inside `GlobalSnapshotAcceptanceManager` (and the `SpendActionValidator`
admission path it feeds) is precisely the **global re-execution** that
the sharding redesign removes. Under sharding, the work that was once
universal becomes per-shard, and the cross-shard slice — `SpendAction`s
that reference allow-spends in a different shard — is the residual
problem this document scopes.

---

## §1 Existing approaches — survey

Each subsection covers: how it works, primitives used, latency, failure
modes, slashing surface, and the key insight applicable to Tessellation.
Each claim about another blockchain is attributed; see the Sources
section at the end for canonical URLs.

### §1.1 Polkadot — XCMP / HRMP / VMP

**How it works.** Polkadot separates the message *format* (XCM — Cross-
Consensus Message format) from the message *transport* (XCMP / HRMP /
VMP). Parachains are independent execution layers; the relay chain is
the security + ordering substrate. Cross-parachain messages flow
through unidirectional channels that must be opened by mutual consent
between sender and recipient parachains (or by governance for
common-good chains) (Polkadot Wiki — XCM Transport; Polkadot Wiki —
Build HRMP Channels).

There are three transport variants:

- **VMP (Vertical Message Passing)** — relay chain ↔ parachain. Split
  into **UMP** (parachain → relay) and **DMP** (relay → parachain).
  Each parachain "must consume at least one message per candidate if
  the queue is not empty"; "there's no size cap — the relay chain
  implements spam prevention mechanisms" (Polkadot Implementers' Guide).
- **HRMP (Horizontally Relay-routed Message Passing)** — the
  currently-deployed inter-parachain transport. **All messages are
  stored in relay-chain storage**, making it a heavy but simple
  primitive. Each channel opens via `hrmp_init_open_channel` (sender,
  posts `hrmp_sender_deposit`) + `hrmp_accept_open_channel` (recipient,
  posts `hrmp_recipient_deposit`). Channel parameters: `max_capacity`
  (queue depth in messages), `max_message_size` (byte limit per
  message), `max_total_size` (total bytes queued). Polkadot's
  configuration uses `hrmpChannelMaxCapacity: 1,000` and
  `hrmpChannelMaxTotalSize: 102,400` (Polkadot Developer Docs — XCM
  Channels).
- **XCMP (Cross-Chain Message Passing)** — the long-term replacement
  for HRMP. Only **channel metadata (an MQC head hash)** sits on the
  relay chain; messages themselves travel collator-to-collator off the
  relay chain. Recipients cryptographically verify messages against
  the relay-parent's stored hash during candidate authoring (Polkadot
  Implementers' Guide — Messaging). XCMP is not fully deployed in the
  protocol's mainline production as of this writing; HRMP remains the
  operational primitive.

**Primitives used.** Per-channel MQC (Message Queue Chain) head — a
hash chain of `(prev_head, B, H(M))` where B is the relay block number
and H(M) is the message hash (Polkadot Implementers' Guide — HRMP
Pallet). Each link is an immutable audit trail of one message. The
recipient's parachain consumes by advancing its `hrmp_watermark` (the
relay block number "up to which a para has received messages"); the
pallet's `prune_hrmp` then drops messages up to that watermark from
inbound channels.

**Latency cost.** HRMP requires one relay block for the message to be
included in the sender parachain's candidate, one relay block for the
candidate's backing, and the recipient parachain's next candidate to
consume — roughly **2–3 relay blocks** end-to-end (Polkadot's relay
block time is ~6 s, so ~12–18 s per cross-parachain message under
nominal conditions). XCMP halves the on-chain footprint but inherits
the relay-finality latency for safety.

**Failure modes.** "With relayed message variants, message data is
passed via the relay chain, and piggy-backs over VMP. It is much less
scalable, and on-demand parachains in particular may not receive
messages due to excessive queue growth" (Polkadot Wiki — XCM Transport).
Parachains are explicitly allowed to *block* messages from other
parachains (a partition primitive). Full-queue rejection happens at
candidate-acceptance time via `check_outbound_hrmp`, which enforces
`max_capacity`, `max_message_size`, `max_total_size`, and
`hrmp_max_message_num_per_candidate`.

**Slashing surface.** Polkadot does not slash on cross-chain message
behaviour directly. Slashing happens at the **validator** layer for
equivocation in the relay chain (GRANDPA + BABE) and for incorrect
attestations on parachain candidates. A parachain that misbehaves
(produces invalid state transitions) is caught by relay-chain backing
+ validity-checker (ELVES) approval; its block is rejected, but the
*cross-chain* surface is not a slashing primitive.

**Key insight for Tessellation.** Polkadot's XCMP/HRMP cleanly
separates *format* (XCM — what the message says) from *transport*
(how it's delivered). The MQC head primitive — a hash-chain of
messages stored on a shared coordinator — is exactly the shape
Tessellation's gl0 could play if we adopt a receipt-based model
(§3.2). The **deposit-gated channel** model is heavier than we need
at Tessellation's scale (~1K-3K metagraphs), and the *mutual-consent
channel opening* is a UX hit our model can avoid because every shard
is in the same trust domain (overlapping operator set, single global
seedlist, single eta).

---

### §1.2 Cosmos — IBC (Inter-Blockchain Communication)

**How it works.** IBC is the canonical light-client-based cross-chain
protocol. Two independent blockchains each run a **light client** of
the other (typically Tendermint-light-client), and exchange messages
via cryptographically-verified *packets*. The light client on chain A
tracks chain B's consensus state (header hashes, validator-set
rotations, state-root commitments) and gates inbound packets on Merkle
proofs against B's verified state. The protocol is specified across
ICS (Interchain Standards) modules; the packet-and-channel layer is
ICS-004 (cosmos/ibc spec ICS-004).

The packet lifecycle has four canonical operations (ICS-004 spec):

1. **`sendPacket(capability, sourcePort, sourceChannel, timeoutHeight,
   timeoutTimestamp, data)`** — sender chain stores a packet commitment
   `hash(hash(data), timeoutHeight, timeoutTimestamp)` at
   `packetCommitmentPath` and increments `nextSequenceSend`. No proof
   required (local).
2. **`recvPacket(packet, proof, proofHeight, relayer)`** — receiver
   chain verifies `verifyPacketData()` (single-hop) or
   `verifyMultihopMembership()` (multi-hop) proving the commitment
   exists on source. Stores `nextSequenceRecv` (ordered channel) or a
   `SUCCESSFUL_RECEIPT` at `packetReceiptPath` (unordered).
3. **`writeAcknowledgement(packet, acknowledgement)`** — receiver
   chain stores `hash(acknowledgement)` at `packetAcknowledgementPath`.
   No proof required (local).
4. **`acknowledgePacket(packet, acknowledgement, proof, proofHeight,
   relayer)`** — sender chain verifies `verifyPacketAcknowledgement()`
   and deletes the packet commitment.

A fifth path covers `timeoutPacket(...)`: sender chain verifies
`verifyPacketReceiptAbsence()` (unordered) or `verifyNextSequenceRecv()`
(ordered) proving the receiver did not consume the packet by the
timeout, deletes the commitment, and refunds.

**Primitives used.** Merkle inclusion proofs against the counterparty
chain's verified state-root. Two channel-ordering modes
("**ORDERED**", "**UNORDERED**", plus "**ORDERED_ALLOW_TIMEOUT**" for
the variant that doesn't close on timeout). A persistent **light
client** on each side tracks the other chain's validator-set
rotations.

**Relayer role.** Critically, IBC relayers are **off-chain
permissionless actors** — not consensus participants. They observe
events on the sending chain (via the `sendPacket` event log),
construct Merkle proofs, and submit `recvPacket` / `acknowledgePacket`
/ `timeoutPacket` transactions on the destination chain (ICS-018
relayer spec). The protocol's safety properties — exactly-once
delivery, deliver-or-timeout — hold **even under fully Byzantine
relayers**; "packet relay liveness depends on at least one correct,
live relayer" (search summary, IBC relayer issue discussions).

**Latency cost.** Each cross-chain packet round-trip is gated by
**counterparty finality**, which in Tendermint chains is single-block
(~6 s on Cosmos Hub). So a `sendPacket` → `recvPacket` → `ack` round
trip is **at minimum 2 counterparty blocks + 1 sender block** =
~15-25 s under nominal conditions. ICS-29 fees are paid to incentivise
relayer submission timeliness.

**Failure modes.** Out-of-order receive on ordered channels closes
the channel; receipt-after-timeout is prevented by the absence proof
required at `timeoutPacket`; double-send is prevented by the
commitment-delete on ack; in-flight packets at channel closure are
timed out via `timeoutOnClose`. The ICS-20 token-transfer application
demonstrates the atomicity model: "Execution is atomic: if any
payload fails, all state changes from that packet are rolled back"
(search summary, IBC overview).

**Slashing surface.** IBC does not slash on cross-chain message
behaviour. Slashing happens at the underlying consensus layer
(Tendermint equivocation / unavailability). However, the light-client
mechanism does encode a *fraud-detection* primitive: a **misbehaviour
detection** procedure exists where a relayer submits two conflicting
headers from the counterparty chain, and the light client is "frozen"
— blocking further packet processing until governance intervenes.
This is not slashing per se (no on-chain stake forfeiture in the
canonical spec), but it is the surface on which slashing can be
layered.

**Key insight for Tessellation.** IBC's cleanest contribution is the
**explicit lifecycle separation** of `send → recv → ack`, with each
step gated by a Merkle proof against the counterparty's state. The
explicit timeout primitive (with `packetReceiptAbsence` proof) is the
liveness backstop that lets the protocol cleanly handle "the
destination shard is wedged" without escalating to governance every
time. The **relayer-as-off-chain-actor** pattern is appealing because
the protocol's safety doesn't depend on relayer honesty — but in our
overlapping-operator model the relayer role naturally collapses into
the gl0-side aggregator. The notion that "the packet body is stored
only as a hash on the sending chain" (cf. Polkadot's MQC head) is the
right scalability primitive.

---

### §1.3 NEAR — Receipts and async cross-shard txs

**How it works.** NEAR's Nightshade design represents cross-shard
execution as **asynchronous message-passing via Receipts**. There are
no separate shard chains; "all block producers and validators build
a single blockchain called the main chain. The state is split into
shards, and each block producer downloads locally only a subset of
the state corresponding to some subset of the shards" (NEAR Nightshade
paper, summarised). A *chunk* is the per-shard subset of a block;
each block is the union of chunks across all shards (Stader Labs
explanation).

**Action receipts vs data receipts.** NEAR's NomiCon ICS-equivalent
spec defines two receipt types (Nomicon — Receipts spec):

- **`ActionReceipt`** — "a request to apply actions on the receiver's
  account. It may originate from a transaction or another
  ActionReceipt's processing." Carries `signer_id`,
  `signer_public_key`, `gas_price`, `input_data_ids` (dependency
  receipts), `output_data_receivers`, and `actions` (FunctionCall,
  Transfer, Stake, AddKey, DeleteKey, CreateAccount, DeleteAccount).
- **`DataReceipt`** — "represents the final result of contract
  execution, containing the data output." Carries `data_id` and
  `data: Option<bytes>` (None indicates failure).

Both receipt types carry universal `predecessor_id`, `receiver_id`,
`receipt_id` (CryptoHash).

**Cross-shard execution flow.** Per the Nomicon: "All cross-contract
communication in Near happens through Receipts." The runtime model:

1. A transaction submitted in shard A is converted into an
   `ActionReceipt` and applied in shard A's chunk.
2. If the receipt's `receiver_id` lives in shard B, the receipt is
   routed to shard B and stored as a **delayed receipt** in shard B's
   state. NEAR's promise system makes this asynchronous from the
   caller's perspective (Sigma Prime — NEAR Smart Contract Auditing:
   Sharding & Cross Contract Calls).
3. Shard B's next chunk applies the receipt. If the receipt's
   `actions` produce further cross-shard calls, **new ActionReceipts
   are created** and the cycle repeats.
4. If the original call expected a callback (a typical "promise" in
   NEAR contracts), the destination chunk emits a **DataReceipt**
   that's routed back to shard A; shard A applies it as the
   callback's input.

**Postponed receipts.** Per the Nomicon: a postponed `ActionReceipt`
"stored pending DataReceipt arrival, keyed by `account_id,receipt_id`"
— if a receipt's `input_data_ids` are not yet present in the
receiver's state, the runtime postpones execution and decrements a
`pending_data_receipt_count` as each dependency arrives. When the
count reaches zero, the postponed receipt fires.

**Latency cost.** Each cross-shard hop costs **at minimum 1 block**:
shard A's chunk emits the receipt → shard B's next chunk consumes
it. If the originating tx + callback pattern is used (call → result),
the round trip is **2 blocks minimum**. Multi-hop routing (when
shards aren't directly adjacent) adds further block hops; the article
(search summary) notes: "Nightshade routes the transaction through
intermediate shards if the origin and destination are not directly
adjacent, with this multi-hop routing adding a small latency
overhead." NEAR's recent (May 2025) reduction of block times to
~600 ms makes the per-hop latency cheap; finality is ~1.2 s (NEAR
blog — Blink and It's Final).

**Failure modes.** A *postponed* receipt waiting on `DataReceipt`s
that never arrive will sit in receiver state indefinitely; NEAR's
runtime imposes a "lifetime cap" via storage rent and gas
mechanisms, but the protocol does not have an explicit "timeout +
refund" path equivalent to IBC's `timeoutPacket`. If a chunk
producer fails to include a receipt that should have been routed,
the producer's chunk is rejected by the network's chunk-validation
gate. Garbage collection of completed receipts happens via standard
storage cleanup in receiver state.

**Slashing surface.** NEAR slashes for chunk-validity infractions —
"chunk-only producer" validators that produce invalid chunks are
slashed. Cross-shard receipts themselves are not a separate slashing
target; their correct routing is enforced by the chunk-validation
layer (receipts in a chunk must derive correctly from the prior
state + included txs).

**Garbage collection.** Receipts are stored as part of normal state
(in `postponed_receipts` and `delayed_receipts` maps); once executed,
they're removed in the standard state-transition. No special GC
mechanism beyond the runtime's state-storage rent.

**Key insight for Tessellation.** NEAR's receipt model is the
**purest async cross-shard primitive** in production. The
`predecessor_id` / `receiver_id` shape is exactly the routing
information our cross-metagraph references need. The **callback-via-
DataReceipt** pattern is interesting but adds a *programming model*
overhead — NEAR contract authors must explicitly partition their
logic around promise boundaries. **For Tessellation, the synchronous-
tx semantics our users expect (submit → wait → confirm) sits
uncomfortably with this model** unless we hide the asynchrony at the
gl0-aggregation boundary.

---

### §1.4 Ethereum — abandoned Phase 2 + current rollup model

**Original Phase 2 design (abandoned 2020).** Ethereum's original
sharding proposal (the "Eth2 / Serenity" Phase 2 design) used a
similar receipt-based async model to NEAR, but with substantially
more programmer-visible complexity. Vitalik's Phase 2 pre-spec
(ethresear.ch — "Phase 2 pre-spec: cross-shard mechanics") proposed:

- **Address encoding** carries the shard ID (32-byte address = version
  + shard + 29-byte shard-local address).
- **Two special contracts per shard**: `CROSS_SHARD_MESSAGE_SENDER`
  and `CROSS_SHARD_MESSAGE_RECEIVER`.
- **Asynchronous messaging via `CrossShardReceipt`** objects (target,
  value, calldata, optional init data) — exactly the NEAR
  ActionReceipt shape.
- **"Yanking"** — moving an entire contract's state (code + storage +
  balance) between shards atomically: "keeping the [version number]
  and [address in shard] the same, but changing the shard." Yanking
  was the proposed solution for cases where a synchronous cross-shard
  atomic operation was needed — yank the contract to the destination
  shard, do the operation locally, yank it back.

**Why it was hard enough to derail the design.** Vitalik's
characterisation: "making safe cross-shard transaction is impossible
in general" (ethresear.ch summary). Specific failure modes:

- **Atomicity gap.** "The proposal couldn't directly update states in
  both sender and receiver shards. Vitalik acknowledged: 'that is
  impossible to do directly, so what you would have to do is yank
  the sender into the receiving shard, perform the atomic operation,
  then yank the sender back.'" (ethresear.ch summary).
- **Cross-shard call discovery.** Multiple unsatisfactory approaches
  considered: senders submitting receive transactions (but lack
  funds on destination shard); wallet providers handling submission
  (centralisation risks); incentive mechanisms (mempool flooding
  concerns).
- **Yanking vulnerabilities.** "Attackers could yank the reserve
  contract to a third shard before the yankee could use it (the
  'train and hotel problem'), and attackers could pre-reserve
  resources without committing" (ethresear.ch summary).
- **Round-trip latency.** "The system required waiting for beacon
  chain finality before destination shard processing, creating
  inherent asynchrony that applications must navigate" (ethresear.ch
  summary).

**The pivot — rollup-centric roadmap (October 2020).** Vitalik
declared: "It seems very plausible to me that when phase 2 finally
comes, essentially no one will care about it. Everyone will have
already adapted to a rollup-centric world whether we like it or not"
(Vitalik's rollup-centric roadmap post). Ethereum reframed
sharding from *execution sharding* to *data-availability sharding* —
"the base layer focuses on just consensus and data availability
(i.e., data shards, not computation shards)" (summary). All
execution shards became layer-2 rollups.

**Current cross-rollup model.** Each rollup is its own L2 with its
own state; inter-rollup transfer happens via:

- **Canonical bridges** to L1: each rollup has a protocol-defined
  bridge contract on L1. Deposits L1 → L2 are credited by the
  rollup's sequencer within 1–15 minutes; withdrawals L2 → L1 take
  **7 days for optimistic rollups** (the fraud-proof challenge
  window) or **hours for ZK rollups** (Eco support — L2 bridging).
- **Third-party bridges** between rollups: liquidity-based bridges
  with their own validator sets (not Ethereum-secured); faster but
  weaker security.
- **Messenger contracts**: paired smart contracts on L1 and L2 that
  "abstract low-level communication details, similar to how HTTP
  libraries abstract network protocols" (Cube Exchange — Canonical
  Bridge).

The L1 fundamentally acts as a **canonical message bus** between L2s,
but the *atomicity* of cross-rollup operations is delegated to either
the bridge contracts (with their bond / fraud-proof gates) or to
off-chain market makers.

**Slashing surface.** L1 (post-Merge): proposer/attester equivocation
and inactivity leak. Rollup operators: rollup-specific (e.g., bond
slashing on optimistic rollups for invalid state transitions).
Bridges: depends on the bridge design (canonical bridges inherit L1
security; third-party bridges have their own).

**Key insight for Tessellation.** The Phase 2 history is a
*cautionary tale*: building synchronous-atomic cross-shard semantics
into a protocol is hard enough to consume an entire roadmap chapter
and ultimately be abandoned. The successful escape was to either
(a) **embrace async** at the protocol level (NEAR) or (b) **push
atomicity out** to L2 layers (Ethereum). For Tessellation, the
lesson is to design the *protocol* primitive to be the **minimum
sufficient** cross-shard surface (async-friendly, simple to verify)
and let synchronous patterns layer above it where applications need
them.

---

### §1.5 Zilliqa — Sharded execution with no shared state

**How it works.** Zilliqa was the first production sharded blockchain
(launched 2019). Its sharding model is **transaction-sharding only**;
the state is **not sharded across shards** — every node holds the
global state (Skidanov — Limitations of Zilliqa's sharding approach).
Architecture (Zilliqa whitepaper, via search summaries + Zilliqa
sharding-for-smart-contracts blog):

- **Network nodes split into "shards"** (each typically ~600 nodes,
  using PoW for sybil resistance + per-shard BLS-signed PBFT for
  intra-shard consensus).
- **DS (Directory Service) committee** — a special shard of ~600
  nodes elected via PoW. Aggregates per-shard microblocks into final
  blocks. Handles transactions that don't fit standard shard
  assignment.
- **Microblocks** — each regular shard produces a microblock of
  transactions; PBFT-finalised within the shard.
- **Final block** — the DS committee combines microblocks across
  shards into the canonical block.

**Transaction categories** (Zilliqa sharding-for-smart-contracts
post):

- **Category I [U → U]** — user-to-user payments. Assigned to a shard
  by hashing sender + receiver addresses; both must share the shard's
  final n bits.
- **Category II [U → C]** — user calls a smart contract (no further
  contract calls). Same hash-routing.
- **Category III [U → C → ... → C]** — cross-contract chained calls.
  **Cannot be parallelised across shards** — handled sequentially by
  the DS committee.

**Cross-shard messaging.** Per Skidanov: "Zilliqa prohibits parallel
execution of transactions affecting multiple shards. The protocol
restricts: 'transactions that affect more than one shard in parallel
with any other transaction.'" The DS committee processes Category III
transactions **sequentially after the regular shards complete** —
this is the explicit anti-parallelism gate. There is no cross-shard
*messaging* per se; the design assumes shards never need to talk to
each other because conflicting transactions are forced into the same
shard by address-based routing.

**Atomicity.** Atomic guarantees exist *within* a shard (PBFT) and
*within* the DS committee (PBFT). Cross-shard atomicity is achieved
by the structural sequencing — Category III transactions are simply
not concurrent with anything else.

**Slashing surface.** Zilliqa relies on PoW-cost sybil resistance
plus PBFT's byzantine tolerance. Slashing is not a primary mechanism
in the Zilliqa design.

**Limitations identified by Skidanov.** (1) "Single-shard execution
only" — full cross-shard concurrency is unavailable; (2) "Contract
concentration: popular applications monopolize shards, creating
bottlenecks as 'five top dApps will have to reside in five shards'";
(3) Without state sharding, "Zilliqa will just make another
incremental change" — the throughput ceiling is constrained by every
node still holding the global state.

**Key insight for Tessellation.** Zilliqa's design is the **cleanest
example of "avoid cross-shard messaging by forcing routing"** — the
hash-based shard assignment + same-shard requirement means cross-
shard execution simply doesn't happen for the categories where it
would be hard. **This is not viable for Tessellation** because our
metagraphs are independent business units that cannot be co-located
arbitrarily (a `SpendAction` from metagraph A naturally refers to an
`AllowSpend` from metagraph B — we cannot force-route them to the
same shard without breaking the metagraph-as-business-unit framing).
But the *DS-committee-as-cross-shard-fallback* pattern — where a
shared coordinator handles the cross-shard slice serially — is a
useful precedent for the "pull-down to gl0 leader" option in §3.4.

---

## §2 Comparative table

| System | Sync vs async | Latency per cross-shard tx | Primitive complexity | Atomicity guarantee | Slashing surface | Fits Tessellation? |
|---|---|---|---|---|---|---|
| **Polkadot HRMP** | Async (recipient consumes in its next candidate) | ~2–3 relay blocks ≈ 12–18 s | Medium — channels, deposits, watermarks, MQC head, governance for opens | Per-message delivery is atomic; cross-message atomicity is application-level | Validator equivocation (consensus layer); no XCMP-specific slashing | Partial — MQC-head primitive is reusable; channel-deposit overhead is too heavy at 1K-3K metagraphs |
| **Polkadot XCMP** | Async, but messages stored only off-chain (hash on relay) | Same as HRMP (gated by relay finality) | Higher — requires collator-to-collator transport infrastructure | Same as HRMP | Same | Partial — same primitive shape, lower on-chain cost |
| **Cosmos IBC** | Async (sendPacket → recvPacket → ack) | ~2–3 counterparty blocks ≈ 15–25 s | High — light clients, Merkle proofs, channels, relayer fees | Per-packet atomic; cross-packet not | Validator equivocation (Tendermint); IBC misbehaviour freezes light client (not slash) | Partial — lifecycle is clean, but full light-client overhead is overkill when shards share a single eta + seedlist |
| **NEAR Nightshade** | Fully async (Receipts) | ~1 block per shard hop ≈ 0.6–1.2 s post-2025 upgrade | Medium — ActionReceipt + DataReceipt + postponed-receipt machinery | Per-receipt atomic; multi-receipt atomicity is application's responsibility (callback patterns) | Chunk-validity slashing (chunk producer); receipts validated by chunk-validation gate | Strong — the receipt shape is the closest match; but requires giving up synchronous tx semantics |
| **Ethereum Phase 2 (abandoned)** | Async (CrossShardReceipt) + sync (yanking) | Multi-block, gated by beacon finality | Very high — yanking, receipt routing, two special contracts per shard | Yanking enables limited sync atomicity; receipts give async-only | Beacon-chain equivocation; no cross-shard-specific slashing in the abandoned design | Cautionary — the design's complexity led to its abandonment |
| **Ethereum rollups (current)** | Async (canonical bridge round trip) | 7 days (optimistic) / hours (ZK) for L1→L2→L1; minutes for L1→L2 deposits | Medium per rollup; high system-wide (rollups are independent execution environments) | Per-bridge atomic; cross-rollup atomicity depends on bridge | Per-rollup; L1 for canonical bridges | Weak — rollups are independent execution environments. The "L1 as message bus" pattern is more analogous to gl0's role than to per-shard execution |
| **Zilliqa** | Sync within shard; cross-shard is **prohibited / serialised** | Cross-shard category III runs serially in DS committee | Low — no cross-shard messaging at all | Cross-shard atomicity by structural sequencing (DS committee serialises) | PBFT byzantine tolerance; PoW for sybil; no explicit slashing | Weak — forcing same-shard routing breaks metagraph autonomy; but DS-committee-as-fallback is a useful pattern |

**Reading the table:**
- The async approaches (Polkadot, IBC, NEAR) all settle around the
  same primitive shape: a **commitment on the source side + a proof
  on the destination side**. They differ in *what shape the commitment
  is* (MQC head vs packet commitment vs receipt) and *how the proof
  is verified* (relay chain vs light client vs chunk validation).
- The sync approach that was tried (Ethereum Phase 2 yanking) was
  abandoned. **No production system has a clean synchronous cross-shard
  primitive at scale.**
- The "no cross-shard messaging" approach (Zilliqa) works only when
  shards can be statically partitioned with no cross-shard
  dependencies — not viable for Tessellation's metagraph model.

---

## §3 Tessellation-specific adaptation — candidate primitive shapes

Given Tessellation's constraints — overlapping operator set, shared
eta seed, KES + committee signatures already in place, MPT-based
state proofs already in place via `HistoricalMptProofService`, ~7s
gl0 snapshot cadence, metagraphs at ≥7s — the candidate cross-shard
primitives are:

### §3.1 Option I — State request + inclusion proof ("synchronous pull")

**Mechanism.** When shard 1's committee processes a tx that needs
state from shard 2:

1. Shard 1 identifies the cross-shard read at validation time
   (`SpendActionValidator` sees a `currencyId` belonging to a
   different shard).
2. Shard 1 requests the relevant state slice from shard 2's
   committee — concretely, from any honest shard 2 member, with the
   request routed via the gl0 P2P overlay.
3. Shard 2 returns the state slice + an MPT inclusion proof rooted
   at shard 2's *most recent committee-signed checkpoint* (the
   subtree-root recorded in the previous gl0 snapshot for shard 2's
   metagraph).
4. Shard 1 verifies the proof against the locally-stored subtree-
   root stub (every gl0 operator holds these stubs per §2.6 of the
   mitigation proposal); accepts the read; continues validation.

**Latency.** One round-trip from shard 1 → shard 2 → shard 1, gated
by shard 2's most recent gl0-committed checkpoint. Best case: shard
2's last checkpoint is already in gl0 → ~10–100 ms (P2P round trip
+ MPT proof verify). Worst case: shard 2's last checkpoint is
behind — shard 1 must wait for shard 2's next checkpoint to commit,
which is at most one gl0 snapshot away (~7 s).

**Atomicity.** Per-read atomic (the proof either verifies or it
doesn't). Cross-read atomicity within a tx is handled by re-running
the validation if any read fails verification. The tx itself either
admits (all proofs verified) or rejects.

**Primitives needed.** MPT inclusion proof (already present via
`HistoricalMptProofService`); committee-signed subtree root (already
present via the per-shard checkpoint that goes into the gl0
snapshot); a P2P request-response channel between shards (the gl0
overlay already supports peer-to-peer messaging).

**Where it lives architecturally.** The primitive sits **inside the
shard's validation pipeline**. The shard requests state at validation
time, blocks on the proof, then continues. From the *tx-submitter's*
perspective, the tx is processed synchronously.

**Pros.**
- **Synchronous tx semantics preserved.** Application authors and
  end-users see the same submit-and-wait pattern as today.
- **Reuses existing Tessellation primitives.** MPT proofs, committee
  signatures, P2P overlay — all already in place. No new transport
  layer.
- **Minimal protocol surface.** No new tx types, no receipt
  bookkeeping, no timeout machinery.
- **Bounded staleness.** The proof is always rooted at a gl0-
  committed checkpoint, so the read is at most ~7s stale.

**Cons.**
- **Liveness coupling to shard 2.** If no honest shard 2 member
  responds within timeout, shard 1 stalls. Mitigation: any single
  honest member can serve the proof (read is fan-out-tolerant).
- **State-request bandwidth.** Each cross-shard tx adds one request
  + one proof on the wire. Bounded but non-zero.
- **The proof is rooted at the *previous* gl0 snapshot.** If shard 2
  has accepted state changes in its current (uncommitted) snapshot
  that haven't reached gl0 yet, those changes are invisible to
  shard 1. This is the **read-after-write consistency gap**: a tx
  in shard 2 followed immediately by a dependent tx in shard 1 may
  not see the shard 2 effect for up to one gl0 snapshot.

### §3.2 Option II — Receipt-based async ("request-response over multiple rounds")

**Mechanism.** Modeled on NEAR's ActionReceipt / DataReceipt pattern
+ Cosmos IBC's send/recv/ack lifecycle:

1. Shard 1 processes a tx that needs state from shard 2. Instead of
   blocking, shard 1 emits an `InterShardReceipt` of type
   `StateRead(target_shard=2, key=..., callback=tx_continuation)`.
2. The receipt is committed in shard 1's next checkpoint, signed by
   shard 1's committee, and surfaces in the next gl0 snapshot.
3. Shard 2 sees the receipt at the next gl0 snapshot, looks up the
   requested state, and emits a `DataReceipt(reply_to=receipt_id,
   value=..., proof=...)` in its own next checkpoint.
4. Shard 1 sees the `DataReceipt` at the gl0 snapshot after that,
   resumes the tx continuation, validates with the supplied value,
   and applies (or rejects) in the round after.

**Latency.** **3 gl0 snapshots minimum** for a single cross-shard
read: shard 1 emits → gl0 → shard 2 emits → gl0 → shard 1 consumes
→ tx finalised. At 7s gl0 cadence: ~21 s per cross-shard tx,
worst-case higher with non-trivial scheduling.

**Atomicity.** Per-receipt atomic. Multi-step transactions
(callbacks within callbacks) require application-level state-machine
management — exactly NEAR's promise programming model.

**Primitives needed.** A new on-chain receipt type (`InterShardReceipt`
+ `DataReceipt`), routing via gl0 (the relay-chain analogue),
postponed-receipt bookkeeping in receiver shards, GC mechanism for
completed receipts. A *new tx-continuation primitive* — the tx must
be parked between rounds.

**Where it lives architecturally.** The primitive runs **above the
shard's validation pipeline** — the validator emits a receipt and
parks the tx; a separate runtime layer drives the receipt
machinery; the validator resumes when the reply arrives.

**Pros.**
- **No live cross-shard dependency.** Shard 1's processing rate is
  not coupled to shard 2's responsiveness — receipts are
  asynchronous.
- **Clean partition handling.** A timeout primitive (à la IBC
  `timeoutPacket`) cleanly handles "shard 2 is down" — refund the
  tx, surface the failure.
- **Generalises to multi-shard reads.** A tx that needs state from
  shards 2, 3, 4 emits three receipts and resumes when all
  `DataReceipt`s arrive (NEAR's `pending_data_receipt_count`
  pattern).

**Cons.**
- **Breaks synchronous tx semantics.** Application authors must
  handle multi-block convergence; end-users see the tx in a
  "pending" state for ~21s. This is the largest deviation from
  current Tessellation tx UX.
- **Major protocol surface.** New tx types, new validator state
  (postponed receipts), new lifecycle handling, GC machinery — all
  comparable in scope to a new MPT partition family
  (cf. `:project_mpt_primary_migration_pattern`).
- **State-staleness model.** The read is rooted at shard 2's state
  at the *moment shard 2 served the read*, not at the moment shard 1
  emitted the receipt. Consistency reasoning is harder than in the
  sync model.

### §3.3 Option III — Escrow / pre-commit ("two-phase commit at gl0")

**Mechanism.** Inspired by Cosmos IBC's commitment-and-ack model +
classical 2PC:

1. Shard 1 detects a cross-shard dependency at validation time;
   instead of accepting the tx, it admits it as **pending** and
   emits a "witness request" naming shard 2 and the required state.
2. Shard 1's pending tx pool surfaces the witness request in
   shard 1's next checkpoint.
3. Shard 2's committee, on observing the witness request via gl0,
   includes the requested state slice + proof in its own next
   checkpoint as a "witness".
4. gl0 aggregates both checkpoints in the same global snapshot. The
   witness arriving at gl0 enables shard 1's pending tx to be
   "promoted" to admitted in shard 1's checkpoint *after* gl0 has
   committed shard 2's witness.

**Latency.** **2 gl0 snapshots minimum**: shard 1 emits request +
shard 2 emits witness (in parallel, both in round N) → gl0 commits
both at round N → shard 1 finalises at round N+1. ~14 s under 7s
cadence.

**Atomicity.** **Strong atomicity** — the tx is admitted only after
gl0 has cryptographically committed both the request and the
witness. If shard 2 doesn't witness, the tx times out cleanly.

**Primitives needed.** Pending-tx-pool persistence in shard
checkpoints; witness-request and witness on-chain shapes; gl0-side
matching of request to witness.

**Pros.**
- **Strong atomicity via gl0 commitment.** Unlike Option I (where
  read freshness is only as good as shard 2's last checkpoint),
  here the witness is **bound to the current round** by gl0's
  commitment.
- **Cleaner consistency story than Option II** — the request and
  witness are matched at gl0, not at the shard layer.
- **Lower latency than Option II** (2 rounds vs 3).

**Cons.**
- **Still breaks synchronous semantics.** Multi-block convergence is
  visible to the application.
- **Pending-tx-pool persistence is a new primitive.** Shards must
  carry pending state across snapshots.
- **gl0 matching logic adds central complexity.** The aggregator
  must pair requests to witnesses across shards.

### §3.4 Option IV — Pull-down to gl0 leader ("DS-committee pattern")

**Mechanism.** Modeled on Zilliqa's DS-committee:

1. Cross-shard txs are not validated in any shard.
2. Shards detect that a tx is cross-shard (e.g., references state
   in another metagraph) and bypass it to a special "cross-shard
   queue" surfaced in their checkpoint.
3. gl0's leader, after aggregating shard checkpoints, runs a
   **second validation pass** against the union of all shards'
   states — it can do this because gl0 holds the subtree roots and
   can request inclusion proofs from any shard to fill in needed
   slices.
4. Cross-shard txs are admitted in the gl0 snapshot, not in any
   shard's checkpoint.

**Latency.** **1 gl0 snapshot** (cross-shard txs are bundled into
the gl0 snapshot directly). ~7 s.

**Atomicity.** Strong (single-pass validation at gl0).

**Primitives needed.** A cross-shard tx queue in shard checkpoints;
a gl0-side validator that re-executes cross-shard txs against the
aggregated state.

**Pros.**
- **Lowest latency.** Single gl0 snapshot.
- **Strongest atomicity** — single-pass validation at gl0.
- **Familiar pattern** — close to the current pre-sharding model
  where gl0 re-validates everything.

**Cons.**
- **Re-introduces gl0 as universal executor.** The headline
  architectural payoff of Option A — gl0 stops re-executing
  metagraph transitions — is *partially undone* for the cross-shard
  slice. gl0 must hold (or fetch) sufficient state to re-execute
  cross-shard txs.
- **gl0 leader becomes a throughput bottleneck.** All cross-shard
  txs serialise through gl0's leader.
- **Cross-shard / intra-shard execution paths diverge.** The
  shard's local executor handles single-shard txs; gl0 handles
  cross-shard txs. Validation logic is duplicated.

---

## §4 Recommendation

### §4.1 Headline recommendation

**Adopt Option I (State request + inclusion proof) as the v1
cross-shard primitive.** Treat Option III (escrow / pre-commit) as a
v2 escalation path for the subset of transactions that need
strong-atomicity guarantees Option I cannot provide. Defer Options II
(receipts) and IV (pull-down) explicitly.

### §4.2 Rationale

Option I scores best against the load-bearing criteria for v1:

1. **Preserves synchronous tx semantics.** The Tessellation user
   model — submit a tx and wait for confirmation, with confirmation
   bounded by gl0's normal cadence — is preserved. Application
   authors do not have to refactor around multi-block convergence.
   This is the dominant criterion for adoption inside the existing
   metagraph ecosystem.

2. **Reuses existing primitives — no new wire types, no new
   transport.** MPT inclusion proofs (`HistoricalMptProofService`),
   committee-signed subtree roots (per-shard checkpoint into gl0
   snapshot, already designed in `CROSS-SHARD-MITIGATION-PROPOSAL.md`
   §2.7), P2P request-response (the existing gl0 overlay) — all
   present. The cost is integration code, not protocol design.

3. **Latency is competitive.** Best-case ~10–100 ms (intra-snapshot
   read); worst-case ~7 s (read forces wait for shard 2's next
   checkpoint). This compares favourably to NEAR's
   1-block-per-hop (now ~0.6 s), beats IBC's 15–25 s, and beats
   Polkadot HRMP's 12–18 s.

4. **Safety substitutes are already in place.** Shard 2's
   committee-signed subtree root is the integrity anchor. Combined
   with Option A's VRF-eligibility for committee members and
   Option C's slashing for equivocation, the read is no worse than
   reading shard 2's own committed state — and shard 2's committed
   state is the canonical truth for shard 2 anyway (gl0 doesn't
   re-execute under §2.7 of the mitigation proposal).

5. **Minimal protocol surface.** No new tx types, no new lifecycle
   states, no GC machinery, no postponed-receipt bookkeeping. The
   primitive is "a remote MPT read with proof".

6. **Aligns with the Polkadot insight without inheriting the
   weight.** Polkadot's MQC head + recipient verification is the
   right *shape*; Tessellation's overlapping-operator model lets us
   skip the channel-deposit + governance-open overhead.

### §4.3 The read-after-write consistency gap — recommendation

Option I's largest limitation is the read-after-write consistency
gap: a tx in shard 2 at round N is invisible to shard 1 until
round N+1 (after gl0 commits shard 2's round-N checkpoint).

**Recommendation for v1:** accept the gap. Document it as a known
property: cross-shard reads see the most-recent **gl0-committed**
state, not the most-recent shard-2-local state. Most cross-metagraph
references in production today are not latency-tight enough to
notice ~7s of staleness — `SpendActionValidator`'s
`AllowSpend`-reference pattern is typically used in batched
multi-metagraph workflows (e.g., DEX swaps with pre-authorised
allow-spends), not in low-latency-tight composite operations. The
metagraph workload that *would* need read-after-write consistency
(e.g., a real-time multi-metagraph atomic settlement) is exactly the
class that should escalate to Option III in v2.

### §4.4 What we are explicitly **not** committing to

- An implementation. This is a primitive-shape recommendation;
  surface design is downstream.
- The exact request-response protocol wire format.
- The exact timeout / retry policy for unresponsive shards (see
  §5.4).
- A v2 escalation to Option III. That's a separate design once we
  have v1 data and have identified workloads that need stronger
  atomicity.

### §4.5 Sequencing

v1 (Option I) should be sequenced **before** the gl0 currency
re-validation removal (A4 in `CROSS-SHARD-MITIGATION-PROPOSAL.md`
§5.3 — the hard-fork step). Reasoning: until shards are the sole
re-executors, gl0 *can* serve as a fallback for cross-shard reads
(it holds all metagraphs' currency state). After A4, gl0 stops
holding currency state, and Option I becomes the *only* mechanism for
cross-shard reads. The wire + proof infrastructure should be live and
debugged before A4 flips.

Concretely:
- **Phase A1+A2** (`CROSS-SHARD-MITIGATION-PROPOSAL.md` §5.3):
  VRF-sortition + admission gates. **No cross-shard primitive needed
  yet** — gl0 still re-executes.
- **Phase A3**: subtree-stub MPT pattern deployed. Stubs in place;
  full subtrees synced to shard members; **Option I primitive lands
  here** as the read-by-proof mechanism non-shard members already
  need to do anyway.
- **Phase A4** (hard fork): gl0 stops re-executing. **Option I is
  the load-bearing cross-shard mechanism from this point forward.**

---

## §5 Open questions

These are flagged for the implementation workstream and the
broader sharding plan. This document does not attempt to close
them.

### §5.1 How wide is the cross-shard read surface in practice?

`SpendActionValidator` is the obvious cross-metagraph case. What
other validators in the current acceptance pipeline take state from
multiple metagraphs? An audit of `GlobalSnapshotAcceptanceManager`,
`MetagraphValidator`, and the various per-metagraph validators is
needed to characterise the cross-shard read frequency. If it's
**rare** (say <1% of txs), Option I's per-tx round-trip cost is
fine; if it's **common** (>20%), batching or pre-fetching may be
needed to amortise the read cost.

### §5.2 Read consistency under shard reorg

Option I's read is rooted at gl0's most-recent commitment of shard 2.
Under the Taktikos LDD + maxvalid-tk fork-choice rule
(`:project_taktikos_protocol`), gl0 itself can experience reorgs up
to depth-k. **If shard 2's committed-state slice changes due to a
gl0 reorg, what happens to shard 1's tx that already validated
against the now-orphaned state?** The tx's MPT proof is no longer
valid against the new gl0-canonical state.

Candidate answers:
- **Re-validate on reorg.** If a tx's witnessing proof is rooted at
  an orphaned gl0 snapshot, the tx must be re-validated against the
  new canonical state. Adds reorg-handling complexity.
- **Wait for depth-k finality before admitting cross-shard reads.**
  Eliminates the reorg problem but adds k snapshots of latency
  (~30 min at k=255). Probably too slow.
- **Accept staleness up to one gl0 snapshot; rely on subsequent
  reorgs being rare.** Probabilistic; needs the empirical reorg
  frequency to characterise.

**Open. Likely requires simulation.**

### §5.3 What about cross-shard *writes*?

The discussion in §3 focused on cross-shard *reads*. But the
`SpendActionValidator` case can be more subtle: a single tx may
both *read* state from metagraph A and *write* state to metagraph
B. Pure-read Option I covers reads; writes require either:
- The tx is treated as **two separate per-shard sub-txs** (shard A's
  consumed `AllowSpend`, shard B's resulting balance change), with
  cross-shard coordination via gl0 — closer to Option III.
- The tx is admitted in *one* shard which sees its own write, and
  the other shard's effect is applied async via a receipt — closer
  to Option II.

**For v1 with Option I**: confine to read-only cross-shard txs.
Cross-shard writes are out of scope and likely require Option III
escalation. **Open. Audit the actual cross-shard write surface
before committing.**

### §5.4 Liveness backstop — shard unresponsiveness

If no shard 2 member responds to a state request within timeout,
shard 1's tx stalls. Mitigations:
- **Fan-out the request.** Ask multiple shard 2 members; any honest
  one can respond. (P2P-overlay-native; no protocol change.)
- **Fall back to gl0-served read pre-A4.** Until the hard fork,
  gl0 holds all currency state and can serve the read directly.
  Post-A4, this fallback evaporates.
- **Reject the tx with a "shard 2 unreachable" error.** Surface
  the failure to the submitter; let them retry. Acceptable for v1.

**Open. Recommended for v1: fan-out + reject-on-timeout. Refine if
shard unreachability becomes a real-world issue.**

### §5.5 Composition with NIPoPoW per-shard towers

Per-shard NIPoPoW headers (`NIPOPOW-PROPOSAL.md` §2) commit to
shard state at structural-depth boundaries. **Can the per-shard
tower serve as the inclusion-proof root in Option I?** Probably
yes — and this gives Option I a clean light-client story (a light
client verifying shard 1's tx that read shard 2's state can verify
the embedded inclusion proof against shard 2's tower entry it
already holds). This is the bridge between Option I and the
metakit-sdk TS inclusion verifier
(`:reference_metakit_sdk_ts_inclusion_verifier`).

**Open. Cross-reference with NIPoPoW workstream.**

### §5.6 Caching / pre-fetching to amortise read cost

If the same cross-shard reads are repeated (e.g., a hot `AllowSpend`
that many txs reference), naive Option I repeats the proof
verification per tx. Candidate optimisation:
- **Cache verified reads within a checkpoint window.** A proof
  verified once at gl0 snapshot N can be re-used by all txs in
  shard 1's checkpoint N+1 that reference the same key.
- **Pre-fetch the shard 2 state slice at the start of the
  checkpoint.** Shard 1's leader gathers all expected cross-shard
  reads upfront, batches the requests, and uses the proofs to
  validate the whole checkpoint's worth of txs.

**Open. Premature optimisation; revisit when v1 is live and we have
read-rate data.**

### §5.7 Witnessing for KES + committee context

The state-read proof is signed by shard 2's committee at gl0
snapshot N-1 (the previous snapshot). The KES period at N-1 may have
been a different operator key set than at the current period N.
**Does the KES verification context for cross-shard reads need
explicit period-rooting?** Probably — the verifier must know which
KES period the signature was produced under to validate against the
right KES tree. This is the same period-rooting machinery the
slashing-evidence pipeline (§6.2 of mitigation proposal) needs.

**Open. Cross-reference with KES Wave 2 design
(`KES10-WAVE-2-DESIGN.md`).**

---

## §6 Cross-references

| Doc | Relationship |
|---|---|
| [`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md) §2.6, §2.7 | Defines the sharding architecture (overlapping operators, subtree-stub MPT, gl0 stops re-executing). This doc's recommendation lives **inside** that architecture. |
| [`CROSS-SHARD-MITIGATION-PROPOSAL.md`](./CROSS-SHARD-MITIGATION-PROPOSAL.md) §5.2 | Out-of-scope directive: no changes to metagraph submission pipeline. This doc respects that — Option I requires no metagraph-side change. |
| [`SLASHING-DESIGN.md`](./SLASHING-DESIGN.md) §1, §4 | The equivocation-slashing primitive. If a shard 2 member serves a fraudulent read proof, the same KES-rooted slashing path applies. |
| [`KES10-WAVE-2-DESIGN.md`](./KES10-WAVE-2-DESIGN.md) | KES period machinery needed for §5.7 (period-rooting cross-shard read signatures). |
| [`NIPOPOW-PROPOSAL.md`](./NIPOPOW-PROPOSAL.md) §2 | Per-shard tower; §5.5 sketches the composition with Option I for light clients. |
| [`COMMITTEE-SORTITION-DESIGN.md`](./COMMITTEE-SORTITION-DESIGN.md) | Committee-signature primitive that signs the per-shard subtree root used as the inclusion-proof anchor in Option I. |
| `modules/node-shared/.../swap/SpendActionValidator.scala` | The canonical cross-metagraph-reference validator that motivates this work. §0 dissects its current shape. |
| `modules/node-shared/.../nakamoto/HistoricalMptProofService.scala` | The MPT inclusion proof primitive Option I reuses. |
| `:project_sharding_direction_clarified` (memory) | Confirms the sharding direction: execution sharded, NOT data sharded; subtree-stub MPT + state-proof verification at gl0. |
| `:project_post_nipopow_phase_order` (memory) | Hard fork sequenced last; Option I lands in A3, before A4 hard-fork removal of gl0 currency re-validation. |
| `:reference_metakit_sdk_ts_inclusion_verifier` (memory) | TS inclusion verifier in Constellation Labs metakit-sdk; the light-client view (§5.5) can adapt this rather than port. |

---

## Sources

### Polkadot
- [XCM Transport Methods (XCMP, HRMP, VMP) — Polkadot Wiki](https://wiki.polkadot.com/learn/learn-xcm-transport/)
- [Introduction to Cross-Consensus Message Format (XCM) — Polkadot Wiki](https://wiki.polkadot.com/learn/learn-xcm/)
- [Messaging Overview — The Polkadot Parachain Host Implementers' Guide](https://paritytech.github.io/polkadot/book/messaging.html)
- [HRMP Pallet — Polkadot Parachain Host Implementers' Guide](https://paritytech.github.io/polkadot/book/runtime/hrmp.html)
- [XCM Channels — Polkadot Developer Docs](https://docs.polkadot.com/develop/interoperability/xcm-channels/)
- [Opening HRMP Channels Between Parachains — Polkadot Developer Docs](https://wiki.polkadot.network/docs/build-hrmp-channels)
- [polkadot/runtime/parachains/src/hrmp.rs — GitHub](https://github.com/paritytech/polkadot/blob/master/runtime/parachains/src/hrmp.rs)
- [XCMP: Cross-Chain Message Passing — Figment.io](https://www.figment.io/insights/xcmp-cross-chain-message-passing/)
- [Polkadot XCMP Explained — How Parachains Talk to Each Other (PolkaWorld)](https://polkaworld.medium.com/polkadot-xcmp-explained-how-parachains-talk-to-each-other-b9ccbc07ef44)

### Cosmos IBC
- [ibc/spec/core/ics-004-channel-and-packet-semantics/README.md — cosmos/ibc](https://github.com/cosmos/ibc/blob/main/spec/core/ics-004-channel-and-packet-semantics/README.md)
- [ibc/spec/core/ics-002-client-semantics/README.md — cosmos/ibc](https://github.com/cosmos/ibc/blob/main/spec/core/ics-002-client-semantics/README.md)
- [ibc/spec/relayer/ics-018-relayer-algorithms/README.md — cosmos/ibc](https://github.com/cosmos/ibc/blob/main/spec/relayer/ics-018-relayer-algorithms/README.md)
- [IBC-Go Documentation — Cosmos Docs](https://docs.cosmos.network/ibc/latest/intro)
- [IBC-Go Overview](https://ibc.cosmos.network/main/ibc/overview/)
- [Light Client Development — Cosmos Developer Portal](https://tutorials.cosmos.network/academy/3-ibc/5-light-client-dev.html)
- [Deep Dive into Cosmos Inter Blockchain Communication Protocol — bcas.io](https://blog.bcas.io/deep-dive-cosmos-inter-blockchain-communication-protocol)
- [Inter-Blockchain Communication Message Relay Time Measurement and Analysis in Cosmos — MDPI](https://www.mdpi.com/2076-3417/13/20/11135)

### NEAR
- [Sharding Design: Nightshade — NEAR Protocol](https://pages.near.org/papers/nightshade/)
- [Nightshade: NEAR's Ultimate Sharding Technique — Stader Labs](https://www.staderlabs.com/blogs/near/nightshade-nears-ultimate-sharding-technique/)
- [NEAR Smart Contract Auditing: Sharding & Cross Contract Calls — Sigma Prime](https://blog.sigmaprime.io/near-sharding-cross-contract-calls.html)
- [Nomicon — Receipts](https://nomicon.io/RuntimeSpec/Receipts)
- [Nomicon — Transactions in the Blockchain Layer](https://nomicon.io/ChainSpec/Transactions)
- [Blink and It's Final: NEAR Launches 600ms Blocks and 1.2s Finality — NEAR](https://pages.near.org/blog/blink-and-its-final-near-launches-600ms-blocks-and-1-2s-finality/)
- [Enabling Cross-chain Composability: NEAR Protocol's Nightshade — Proximity](https://medium.com/@ProximityFi/enabling-cross-chain-composability-how-near-protocols-nightshade-will-disrupt-defi-3922d52c840c)

### Ethereum
- [Phase 2 pre-spec: cross-shard mechanics — Ethereum Research (Vitalik et al.)](https://ethresear.ch/t/phase-2-pre-spec-cross-shard-mechanics/4970)
- [eth2 quick update no. 2 — Ethereum Foundation Blog](https://blog.ethereum.org/2019/10/31/eth2-quick-update-no-2)
- [Algorithm for Cross-shard Cross-EE Atomic User-level ETH Transfer in Ethereum — arXiv](https://arxiv.org/abs/2102.09688)
- [Vitalik Buterin reevaluates Ethereum's rollup-centric roadmap — The Block](https://www.theblock.co/post/388285/vitalik-buterin-reevaluates-rollup-centric-roadmap-arguing-l2s-decentralized-far-slower-while-ethereum-base-layer-advanced)
- [Ethereum 2 Rollup Centric — Julian's Notes](https://www.jvillella.com/ethereum-2-rollup-centric)
- [Vitalik Buterin outlines 'endgame' roadmap for ETH 2.0 — Cointelegraph](https://cointelegraph.com/news/vitalik-buterin-outlines-endgame-roadmap-for-eth-2-0)
- [Rollup protocol overview — Optimism Docs](https://docs.optimism.io/stack/rollup/overview)
- [What is a Canonical Bridge? — Cube Exchange](https://www.cube.exchange/what-is/canonical-bridge)
- [L2 Bridging Cost Comparison (Native vs Third-Party) — Eco Support](https://eco.com/support/en/articles/14798707-l2-bridging-cost-comparison-native-vs-third-party)
- [Are L2s Really Secured by Ethereum? — Hazeflow Research](https://research.hazeflow.xyz/p/are-l2s-really-secured-by-ethereum)

### Zilliqa
- [The ZILLIQA Technical Whitepaper [Version 0.1] (PDF)](https://docs.zilliqa.com/whitepaper.pdf)
- [The Not-So-Short ZILLIQA Technical FAQ [Version 0.1] (PDF)](https://docs.zilliqa.com/techfaq.pdf)
- [Provisioning Sharding for Smart Contracts: A Design for Zilliqa — Zilliqa Blog](https://blog.zilliqa.com/provisioning-sharding-for-smart-contracts-a-design-for-zilliqa-cd8d012ee735/)
- [Practical Smart Contract Sharding with Ownership and Commutativity Analysis (PLDI '21 — Zilliqa)](https://docs.zilliqa.com/cosplit-pldi21.pdf)
- [Limitations of Zilliqa's sharding approach — Alexander Skidanov / NEAR Protocol](https://medium.com/nearprotocol/limitations-of-zilliqas-sharding-approach-8f9efae0ce3b)

---

*Research document. No code commitment. The recommendation in §4 is
the v1 primitive-shape proposal; implementation surface is the next
workstream's domain. Open questions in §5 are flagged for the
broader sharding plan and likely require simulation or further
design work to close.*
