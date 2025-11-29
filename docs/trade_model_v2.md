# Trade Data Model — Curated Layer

**Status:** Draft for internal discussion  
**Version:** 2.0

---

## 1. Purpose

This document proposes a scalable, flexible, and governed data model for the curated **Trade** dataset in BigQuery, implemented using dbt.

The model is designed to support:

- Surveillance & Conduct analytics
- Risk, P&L, Finance & MI
- Data Science & Research
- Operational reporting
- Future regulatory use cases

It provides a **canonical representation** of a trade while cleanly handling multi-leg structures, product-specific details, lifecycle events, and temporal reconstruction.

---

## 2. Alignment with LBG Data Architecture

This model is designed as an **integrated component of the LBG Global Data Model**, aligned with the current LBG data mesh architecture.

Key alignment points:

- **Domain ownership:** The Trade domain owns this model, with clear contracts for downstream consumers.
- **Interoperability:** The `trade_id` is globally unique across LBG domains, enabling reliable cross-domain joins.
- **Data products:** The curated layer exposes well-defined data products (trade_header, trade_event, etc.) with documented SLAs.
- **Federated governance:** Product-specific satellites allow domain teams to extend the model without centralised bottlenecks.
- **Discovery:** All models are registered in the LBG data catalogue with lineage, schema documentation, and ownership metadata.

---

## 3. Guiding Principles

1. **Clear grain** — one *logical trade* per row in the header.
2. **Separation of concerns** — generic fields in core; product specifics in satellites.
3. **Explicit identity contract** — stable curated `trade_id`, globally unique within LBG.
4. **Lean core** — avoid the "400-column monster" trap.
5. **BigQuery-native design** — nested structures, clustered partitions, low-cost scans.
6. **Temporal clarity** — clear definition of current state vs lifecycle vs snapshots.
7. **Consumer-first** — simple for casual users; flexible for experts.
8. **Soft deletes only** — no physical row deletion; all state changes via lifecycle events.

---

## 4. Trade Identity Contract

Trade identity is where most trade data models fail. We make it explicit:

- `trade_id` is a **synthetic**, curated-layer surrogate key.
- Generated per **logical trade**, persisted forever.
- **Not regenerated** on amendments.
- One logical trade may map to multiple source records.
- Source identity retained as:
  - `source_trade_id`
  - `source_system`

### 4.1 Global Uniqueness

The `trade_id` is **globally unique within the LBG Global Data Model**. This enables reliable joins across domains without collision risk.

**ID Generation Rule:**

```
trade_id = UUID5(namespace=LBG_TRADE_NAMESPACE, name="{domain_name}:{source_system}:{source_trade_id}")
```

Where:
- `LBG_TRADE_NAMESPACE` is a fixed UUID namespace for the Trade domain
- `domain_name` identifies the originating business domain (e.g., "markets", "treasury")
- `source_system` is the upstream system code
- `source_trade_id` is the native identifier from source

This deterministic approach ensures:
- Idempotent generation (same inputs always produce same `trade_id`)
- No coordination required between ingestion processes
- Traceability back to source
- Collision-free identity across all LBG domains

### 4.2 Lifecycle and Identity

Lifecycle mutations are represented as **events**, not as new `trade_id`s. Upstream identity issues are handled in **staging**, not curated.

---

## 5. Temporal Semantics

We clearly separate *current state*, *lifecycle*, and *point-in-time snapshots*.

### 5.1 trade_header = Current-State Table

- One row per `trade_id`.
- Always the **latest known state**.
- Columns reflect current values.
- Not versioned.
- Includes:
  - `as_of_date`
  - `ingest_timestamp`

### 5.2 trade_event = Full Lifecycle Table

Tracks all events affecting the trade:

- NEW, AMEND, CANCEL, ALLOCATE, TRANSFER, GIVEUP, etc.

Enables point-in-time reconstruction by replaying events.

Stores:

- **explicit changed fields** via `changed_fields` array
- **full trade snapshot** in `trade_snapshot_json` for quick replay without reconstruction
- **optional event payload** for event-specific metadata

The `trade_snapshot_json` field captures the complete trade state at each event, enabling:

- Fast point-in-time queries without event replay
- Audit trail with full before/after visibility
- Simplified debugging and reconciliation

### 5.3 Delete Handling

**Deletes are never physical.** All deletions are represented via:

- A `CANCEL` or `DELETE` event in `trade_event`
- The `trade_status` field set to `CANCELLED` or `DELETED` in `trade_header`
- The `is_active` flag set to `false`

This ensures full auditability and the ability to reconstruct any historical state.

### 5.4 trade_header_snapshot (Optional)

For consumers needing *daily full-state PIT reconstruction*:

- One row per trade per `snapshot_date`
- Populated nightly via scheduled dbt job

**Retention Policy:**

| Tier | Retention | Granularity |
|------|-----------|-------------|
| Hot | 90 days | Daily snapshots |
| Warm | 2 years | Weekly snapshots (Monday) |
| Cold | 7 years | Monthly snapshots (month-end) |

**Refresh Policy:**

- Daily refresh at 06:00 UTC
- Full rebuild on schema changes
- Backfill available on request for new consumers

---

## 6. Data Model Components

### 6.1 Core: trade_header (Current State)

**Grain:** one logical trade per row.

Key fields include:

- **Identity:** `trade_id`, `source_trade_id`, `source_system`, `domain_name`
- **Classification:** `product_type`, `asset_class`, `instrument_id`
- **Business:** `book_id`, `desk_id`, `trader_id`, `primary_counterparty_id`
- **Counterparties:** `counterparties ARRAY<STRUCT<role, counterparty_id>>`
- **Economics:** `trade_date`, `trade_time`, `direction`, `notional`, `notional_ccy`, `price`, `price_ccy`, `qty`, `qty_uom`
- **Linking:** `parent_trade_id`, `package_id`, `order_id`
- **Status:** `trade_status`, `is_active`, `as_of_date`, `is_current`
- **Metadata:** `ingest_timestamp`, `source_event_timestamp`

**Partition:** `trade_date` or `as_of_date`  
**Cluster:** `instrument_id`, `book_id`, `product_type`, `trade_status`

---

### 6.2 Legs

**Legs hold leg-level economics. Satellites hold trade-level conventions.**

Leg-level fields:

- pay/receive direction
- rates (fixed/floating)
- notional & currency
- effective/maturity dates
- spread/compounding flags
- underlying instrument per leg

#### Recommended Approach: Dual Access Pattern

We provide **both** nested and normalised access to leg data, optimising for different consumer needs.

**Primary Storage: Nested Legs**

`legs ARRAY<STRUCT<...>>` inside `trade_header`.

Benefits:
- Efficient storage (no join overhead for trade-level queries)
- Atomic trade reads
- Natural fit for BigQuery

**Materialised View: trade_leg**

A dbt model that UNNESTs legs into a normalised table keyed by `trade_id`, `leg_id`.

Benefits:
- Simplified leg-level analytics and aggregations
- Easier joins for consumers unfamiliar with UNNEST
- Standard SQL patterns for transformations

```sql
-- trade_leg.sql
SELECT
  trade_id,
  leg.leg_id,
  leg.direction,
  leg.notional,
  leg.notional_ccy,
  leg.rate_type,
  leg.fixed_rate,
  leg.floating_index,
  leg.effective_date,
  leg.maturity_date,
  -- ... other leg fields
FROM {{ ref('trade_header') }},
UNNEST(legs) AS leg
```

This dual approach means:
- Trade-centric queries use `trade_header` directly (no joins)
- Leg-centric analytics use `trade_leg` (familiar patterns)
- No storage duplication in BigQuery (view or incremental model)

---

### 6.3 trade_event (Lifecycle Table)

Captures all state changes.

Fields:

- `event_id` — unique event identifier
- `trade_id` — FK to trade_header
- `event_type` — NEW, AMEND, CANCEL, ALLOCATE, TRANSFER, GIVEUP, DELETE, etc.
- `event_timestamp` — when the event occurred
- `event_source` — system that generated the event
- `changed_fields ARRAY<STRUCT<field_name, old_value, new_value>>` — explicit deltas
- `trade_snapshot_json JSON` — full trade state at this point in time
- `event_payload JSON` — event-specific metadata (optional)

The `trade_snapshot_json` field enables:

- Instant point-in-time state retrieval without replaying events
- Audit queries showing exact trade state at any historical moment
- Simplified reconciliation and debugging

**Note:** The snapshot is the complete denormalised trade state, not just header fields. For very high-volume scenarios, snapshot population can be made optional per event type.

---

### 6.4 Product-Specific Satellites

Attach to `trade_id` (and optionally `leg_id`).

Examples:

#### Rates: trade_rates_detail

- conventions, reset rules, day count, clearing flags

#### FX: trade_fx_detail

- ccy1/ccy2, near/far dates, settlement conventions

#### Equity: trade_equity_detail

- underlying, expiry, strike, exchange

#### Repo: trade_repo_detail

- collateral type, haircut, basket, settlement type

---

### 6.5 Dimensions

| Dimension | SCD Strategy | Rationale |
|-----------|--------------|-----------|
| `dim_counterparty` | SCD-2 | Legal entity changes need historical tracking |
| `dim_book` | SCD-2 | Book hierarchy changes affect reporting attribution |
| `dim_trader` | SCD-2 | Trader desk moves require historical accuracy |
| `dim_desk` | SCD-2 | Org structure changes need temporal joins |
| `dim_venue` | SCD-1 | Venue attributes rarely change; current state sufficient |
| `dim_instrument` | SCD-1 | See rationale below |

#### Why dim_instrument uses SCD-1

Instrument dimensions present unique challenges for SCD-2:

1. **Volume:** Millions of instruments across asset classes; SCD-2 would multiply storage significantly.
2. **Churn:** OTC instruments especially have frequent minor attribute updates (ratings, classifications).
3. **Join complexity:** SCD-2 temporal joins on high-cardinality dimensions are expensive in BigQuery.
4. **Consumer expectations:** Most analytics need current instrument attributes, not historical.

**Mitigation for historical needs:**

- `trade_header_snapshot` captures instrument attributes at trade time
- `trade_event.trade_snapshot_json` preserves point-in-time instrument state
- A separate `instrument_history` table is available for instrument-specific temporal analysis

If regulatory or surveillance requirements mandate full instrument SCD-2, we can revisit — but this adds significant complexity and cost.

---

## 7. JSON Usage Policy

**Allowed:**

- `trade_snapshot_json` in events (for quick replay)
- Event payloads for event-specific metadata
- Rare/tail attributes in satellites
- Temporary onboarding fields

**Not allowed:**

- Core economics
- Full product schemas
- Frequently-filtered fields
- Anything relied on by marts

JSON is an *escape hatch*, not a modelling strategy.

---

## 8. dbt Project Structure

```
models/
  staging/
    fx_source.sql
    rates_source.sql
    repo_source.sql
    equity_source.sql

  curated/
    trade_header.sql
    trade_leg.sql          -- UNNEST view for leg-level access
    trade_event.sql
    trade_rates_detail.sql
    trade_fx_detail.sql
    trade_equity_detail.sql
    trade_repo_detail.sql

  dimensions/
    dim_instrument.sql
    dim_counterparty.sql
    dim_book.sql
    dim_trader.sql
    dim_desk.sql
    dim_venue.sql

  snapshots/
    trade_header_snapshot.sql

  marts/
    trades_flat.sql
    trades_with_legs.sql
    trades_enriched.sql
```

---

## 9. Benefits

### Technical

- Lean core, BigQuery-native
- Nested structures with normalised access layer
- Easy product onboarding
- Clean lineage in dbt
- Globally unique identity

### Business

- Unified trade view across LBG
- Supports surveillance, MI, risk, DS
- Handles multi-leg & multi-ccy structures
- Aligned with data mesh architecture

### Governance

- Stable, globally unique identity
- Strong auditability via lifecycle with snapshots
- Clear PIT strategy with defined retention
- No physical deletes — full audit trail
- JSON usage controlled

---

## 10. Summary Recommendation

Adopt a **core-plus-satellites** model:

- Globally unique curated `trade_id` with deterministic generation
- Canonical current-state `trade_header`
- Dual-access leg model (nested + normalised view)
- Full lifecycle `trade_event` with JSON snapshots for quick replay
- Product satellites for domain-specific fields
- Dimensions with appropriate SCD strategies
- PIT snapshots with defined retention and refresh policies
- Soft deletes only — all state changes via lifecycle
- Controlled JSON usage
- Full alignment with LBG Global Data Model and data mesh architecture

This provides a flexible, future-proof, BigQuery-native trade model for analytics, MI, surveillance, and operations.

---

## Appendix A: ID Generation Example

```python
import uuid

LBG_TRADE_NAMESPACE = uuid.UUID('a1b2c3d4-e5f6-7890-abcd-ef1234567890')

def generate_trade_id(domain_name: str, source_system: str, source_trade_id: str) -> str:
    name = f"{domain_name}:{source_system}:{source_trade_id}"
    return str(uuid.uuid5(LBG_TRADE_NAMESPACE, name))

# Example
trade_id = generate_trade_id("markets", "MUREX", "TRD-12345")
# -> deterministic UUID, same every time for same inputs
```

---

## Appendix B: Snapshot Retention Implementation

```sql
-- Retention management (run weekly)
DELETE FROM trade_header_snapshot
WHERE 
  -- Remove daily snapshots older than 90 days (keep weekly)
  (snapshot_date < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
   AND EXTRACT(DAYOFWEEK FROM snapshot_date) != 2)
  OR
  -- Remove weekly snapshots older than 2 years (keep monthly)
  (snapshot_date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
   AND EXTRACT(DAY FROM snapshot_date) > 7)
  OR
  -- Remove monthly snapshots older than 7 years
  snapshot_date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR)
```
