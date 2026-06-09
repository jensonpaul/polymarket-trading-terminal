how to track how fast is the rising/declinee
rate of decline/risting
relative/cumulative/momentary/intervals

---

average of all efficiency ratios

---

is the distance range increasing?
(high - low)
velocity of increase as rangePos stays near extremes

---

The change.

For example:

### ER slope

```rust
er_slope =
    er_5s - er_30s;
```

Interpretation:

```text
positive
= trend becoming cleaner

negative
= trend becoming messier
```

For a reversion strategy:

```text
high range position
+
negative ER slope
```

is interesting.

---

### Z-score slope

```rust
z_slope =
    z30 - z5m;
```

Example:

```text
z30 = 0.5
z5m = 2.0
```

means:

> BTC was extreme but is becoming less extreme.

Potential exhaustion.

---

### Range-position velocity

Instead of:

```text
rangePos = 0.92
```

look at:

```rust
range_velocity =
    range_pos_now -
    range_pos_5_seconds_ago;
```

Example:

```text
0.95 -> 0.80
```

Huge information.

The move is unwinding.

---

---

z_score is relative to the average mean in the window/30s/60s. It is not relative to the origin price? It does not have anything to do with the origin price?
Correct.


Ah, this is an important point—so let’s break it down carefully.

---

### 1. What a z-score is, in general

A **z-score** measures how far a value is from its mean **in terms of standard deviations**:

[
z = \frac{x - \mu}{\sigma}
]

Where:

* (x) = current value
* (\mu) = mean of the series (recent history)
* (\sigma) = standard deviation of the series

Interpretation:

* (z = 0) → right at the mean.
* (z = 1) → 1 standard deviation above the mean.
* (z = -2) → 2 standard deviations below the mean.

So a z-score is **relative**, not absolute. It tells you how “extreme” a current value is compared to its recent history.

---

### 2. What your `z30`, `z60`, `z5m` are likely measuring

From your metric table:

* `z30` = z-score over 30 seconds
* `z60` = z-score over 60 seconds
* `z5m` = z-score over 5 minutes

They’re probably measuring **BTC price deviation from its short-term average**, scaled by its local volatility.

Example:

| Metric | Value | Interpretation                    |
| ------ | ----- | --------------------------------- |
| z30    | 1.5   | BTC is 1.5 std dev above 30s mean |
| z60    | 0.5   | BTC is 0.5 std dev above 60s mean |
| z5m    | -0.2  | BTC is slightly below 5m mean     |

This gives you a **multi-scale snapshot** of how “stretched” BTC is at very short time scales.

---

### 3. Why this matters for your strategy

* High z-score → price is unusually far from recent average → possible snap-back (reversion).
* Low z-score → price near average → little immediate overreaction.
* Comparing different horizons (30s vs 5m) shows **momentum vs exhaustion**:

Example:

```text
z30 = 2.0, z5m = 0.5
```

* BTC spiked quickly compared to last 30s, but over 5m it’s only slightly above average → **short-term overextension** → potential snap-back.

```text
z30 = 0.2, z5m = 2.0
```

* BTC has been trending up steadily → **sustained momentum**, less likely to snap back immediately.

---

### 4. What z-score does **not** tell you

* It doesn’t tell you direction of the trend beyond “above/below mean.”
* It doesn’t predict Polymarket tokens directly; it only tells you BTC is unusually high/low relative to short-term history.
* Absolute z-scores can be misleading if volatility is very low or high—always interpret in context.

---

💡 **TL;DR:**

`z` = how extreme BTC’s current price is **compared to its recent past**, measured in standard deviations. High absolute values → price is “stretched,” which can hint at reversals. Differences between short-term and longer-term z-scores reveal **momentum vs exhaustion**, which is more actionable than just the raw value.

---




Looking at your code:

```rust
let z_score_30 = z_score_of(current_f, &prices_30s);
let z_score_60 = z_score_of(current_f, &prices_60s);
let z_score_5m = z_score_of(current_f, &all_prices);
```

and:

```rust
fn z_score_of(value: f64, samples: &[f64]) -> f64 {
    let mean = samples.iter().sum::<f64>() / n;
    let std  = var.sqrt();

    (value - mean) / std
}
```

The origin price never appears anywhere in the calculation.

Your z-scores are measuring:

> "How unusual is the current BTC price compared to the distribution of prices observed recently?"

not

> "How far has BTC moved from the start of the window?"

---

For example:

### Case 1: Strong trend

```text
Start:   100
Now:     110
```

Suppose BTC steadily climbed:

```text
100
101
102
...
109
110
```

Then:

```text
origin distance = +10%
```

but

```text
mean ≈ 105
std  ≈ 3
z    ≈ +1.7
```

Moderately high z-score.

---

### Case 2: Sudden spike

```text
100
100
100
100
110
```

Now:

```text
origin distance = +10%
```

same as before.

But:

```text
mean ≈ 102
std  ≈ 4
z    ≈ +2.0
```

Higher z-score.

The origin metric sees both scenarios as identical.

The z-score says:

> "The second move is much more abnormal."

---

### What each metric is actually measuring

Your metrics fall into distinct families:

#### Origin-based

```rust
distance_from_origin_pct
avg_distance_from_origin
momentum_persistence
```

These all depend directly or indirectly on:

```rust
btc_origin_price
```

They answer:

> "What has BTC done since the window began?"

---

#### Distribution-based

```rust
z_score_30
z_score_60
z_score_5m
```

These depend only on:

```rust
recent mean
recent standard deviation
```

They answer:

> "How statistically unusual is the current price?"

---

#### Range-based

```rust
range_position
```

depends only on:

```rust
high_5m
low_5m
current
```

and answers:

> "Where am I within the recent range?"

---

#### Path-based

```rust
efficiency_ratio
```

depends on:

```rust
net displacement
path length
```

and answers:

> "How straight was the move?"
>
> not
>
> "How large was the move?"

---
