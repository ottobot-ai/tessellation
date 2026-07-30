# Reward Math Determinism Audit

Status: evidence only; no reward-curve change is activated.

## Confirmed live formula

`GlobalDelegatedRewardsDistributor.calculateEmissionRewards` computes:

```text
r       = P_initial / P_current, or 0 when P_current <= 0
t       = (epochProgress - asOfEpoch) / epochsPerYear
impact  = Math.pow(Double(r), Double(iImpact))
decay   = Math.exp(Double(-lambda * t * impact))
rate    = min(0.06, iTarget + (iInitial - iTarget) * decay)
mint    = HALF_UP_0(totalSupply * rate / epochsPerYear)
```

The live implementation is at
`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/rewards/GlobalDelegatedRewardsDistributor.scala:230-284`.
The comment at lines 227-229 is wrong: it begins with `i_initial`, while the
implementation begins with `iTarget` at line 272.

The 24-digit decimal context at lines 86-87 does not make the two
transcendental operations deterministic. Both inputs are converted to binary64
and passed through `Math.pow` and `Math.exp` at lines 260 and 267 before their
results are converted back to decimal. The resulting amount is rounded HALF_UP
to an integer at line 283 and is then allocated into root-bearing reward state.
The producer and follower call this distributor from
`GlobalSnapshotConsensusFunctions.scala:598-618`.

The Java 21 [`Math` contract](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Math.html)
explicitly says equivalent elementary functions are not defined to return
bit-for-bit identical results across implementations and permits platform
intrinsics within the stated ulp bounds.

## Reachable divergence witness

This witness uses the repository's actual pre-`1049436` Testnet emission constants and a
schema-valid oracle price accepted by the live price-state transition:

```text
iTarget=5/1000, iInitial=6/100, lambda=1/10, iImpact=35/100
P_initial=25/1, P_current=92233722547509450/9223372036854775807
asOfEpoch=997094, epochProgress=1001274, epochsPerYear=732000
totalSupply=3693588685_00000000
```

The RED witness runs both `PricingUpdateValidator` and `PriceStateUpdater`
rather than installing a hand-built record. Testnet permits every metagraph with
zero minimum cadence at `application.conf:248-250`. The witness submits the same
price at epochs `1001105`, `1001189`, and `1001273`; all three validations pass.
The 84-epoch window rule rotates it to `upcomingPrice`, then to
`currentPrice`, with next-window values `1001189`, `1001273`, and `1001357`.
The exact promoted price is therefore the reward input at epoch `1001274`.
The live updater enforces that two-window promotion at
`PriceStateUpdater.scala:92-121`; the pricing validator imposes neither a source
restriction in this profile nor a price-value bound at
`PricingUpdateValidator.scala:40-52,85-110`.

On the repository's OpenJDK 21 runtime:

```text
price impact          = 15.462474607697764
exp argument          = -0.008829664461772767
Math-path decay       = 0.991209202547143
StrictMath-path decay = 0.9912092025471428
Math-path mint        = 30031351592
StrictMath-path mint  = 30031351591
```

Those elementary-function results survive the live decimal operations and
straddle the final half-datum boundary. The executable failing invariant witness
is `GlobalDelegatedRewardsMathDeterminismRedSuite` in the isolated `RedTest`
configuration. It deliberately requires both Java-permitted elementary-function
paths to mint the same integer amount and remains red until the live reward
calculation is representation-independent.

The difference is not absorbed before the consensus root. At this Testnet epoch,
the configured distribution weights sum to one and
`protocolWalletMetanomics` has weight `30/100`
(`DelegatedRewardsConfigProvider.scala:153-170`). Production derives reserved
rewards from the variable emission at
`GlobalDelegatedRewardsDistributor.scala:175-224` and rounds each reserved
transaction HALF_UP at lines 466-477. The Math total therefore credits that
address `9009405478`, while the StrictMath total credits `9009405477`. GSAM
accepts those reward transactions at
`GlobalSnapshotAcceptanceManager.scala:2296-2307` and merges their delta into
consensus `balanceChanges` at lines 2813-2819. Identical prior state can thus
produce different rooted balances, not merely different diagnostic totals.

This confirms that the live reachable state domain does not enforce the invariant
"identical consensus inputs yield one reward amount." The witness does not need
out-of-profile supply or cadence values; it uses the Testnet defaults, the live
two-window promotion schedule, and a valid `NonNegFraction` price.

## Input domain and other failure edges

The live types do not bound the curve to the default configuration:

| Input | Admitted range |
| --- | --- |
| `iTarget`, `iInitial`, `lambda`, `iImpact`, configured and oracle prices | numerator `0..Long.MaxValue`, denominator `1..Long.MaxValue` |
| `epochProgress`, `asOfEpoch`, `totalSupply` | `0..Long.MaxValue` |
| `epochsPerYear` | `1..Long.MaxValue` |
| positive-price ratio | approximately `0..Long.MaxValue^2` before binary64 conversion |

`NonNegFraction` establishes those refinements at
`modules/shared/src/main/scala/io/constellationnetwork/schema/NonNegFraction.scala:21-26`;
`EmissionConfigEntry` adds no relational or magnitude bounds at
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala:638-647`.
Consequences include:

- `priceRatio=2` and `iImpact=1024` are valid but
  `Math.pow(2, 1024)` is positive infinity. Constructing the following
  `BigDecimal` at line 260 raises instead of returning a typed rejected
  transition.
- The rate has only an upper `0.06` cap at lines 274-276. There is no lower
  bound and no validation that `iInitial >= iTarget`.
- A zero oracle price is valid: `PricingUpdateValidator.scala:43-52,85-110`
  checks source and update cadence but not positivity. Line 257 maps zero price
  to ratio zero. With positive impact this makes `decay=1` and selects the
  initial/high inflation rate, whereas the positive-price limit as current price
  approaches zero selects the target/low rate. This is the discontinuity already
  covered by audit finding ECO-16.
- `dagPrices` is typed as an ordinary `Map`, and line 247 treats
  `headOption` as the initial price. The default provider currently supplies a
  `SortedMap`, but the type and curve do not enforce minimum-epoch selection.

The fraction-to-decimal conversion itself rounds division under Scala's default
decimal context (`NonNegFraction.scala:25-26`). A replacement must consume the
stored numerator and denominator as an exact reduced rational; routing through
`toBigDecimal` first would freeze an accidental intermediate approximation.

## Replacement gate

Do not silently replace `Math` with `StrictMath`, an approximate decimal
library, or the existing continued-fraction helpers. A replacement needs:

1. A normative choice that the live formula (`iTarget + ...`), rather than the
   contradictory comment, is the intended curve.
2. Rooted ordinal-zero launch parameters and exact bounds for every fraction, price,
   epoch difference, and supply input. The current schema permits numerators,
   denominators, epochs, and supply up to `Long.MaxValue`.
3. A deterministic rational/fixed-point algorithm with a proved interval around
   both fractional power and exponential results. It may return an amount only
   when both interval endpoints have the same HALF_UP integer; otherwise it
   increases precision or fails closed under a frozen resource bound.
4. Ordinal-zero use of the frozen target algorithm. This greenfield fork has no
   deployed fork-only reward history and must not retain the binary64 path as an
   active-chain compatibility era. A future upstream-v4 snapshot importer may
   verify source history with isolated v4 rules; those rules never select target
   consensus execution.
5. Golden vectors at rounding boundaries, zero price, zero impact, pre-transition
   epochs, underflowed decay, maximum admitted inputs, and explicit
   non-convergence/resource exhaustion.

The existing `Ratio` infrastructure is useful but not a launch-ready
drop-in. `RatioOps.pow` accepts only an integer exponent
(`RatioOps.scala:35-36`). `ExpInterpreter.make` discards its convergence flag
and returns the approximation unconditionally
(`ExpInterpreter.scala:24-43`), while `LentzMethod` stops at a configured
iteration bound without returning a proved enclosure
(`LentzMethod.scala:23-62`).

One exact replacement design is feasible after the gate choices:

1. Convert every stored fraction directly to a reduced `Ratio`.
2. Bound and reduce `iImpact=p/q`, then enclose `r^(p/q)` with an integer
   `q`-th-root inequality over outward-rounded fixed-point integers.
3. Enclose `exp(-z)` after deterministic power-of-two range reduction using
   alternating rational Taylor bounds, then square the interval with outward
   rounding.
4. Propagate the interval through the exact rational rate, cap, supply, and
   epochs-per-year operations.
5. Emit only when both endpoints select the same HALF_UP integer. Otherwise
   increase precision on a frozen schedule and finally return a typed
   non-convergence failure.

That algorithm targets the exact real-valued formula rather than choosing an
unreviewed polynomial reward curve. It can replace the fork-only binary64 path
from target ordinal zero. Historical upstream-v4 verification, if later needed
for migration, belongs only in the isolated importer.

Until those choices are ratified and implemented, the live reward path remains
a consensus-determinism blocker.
