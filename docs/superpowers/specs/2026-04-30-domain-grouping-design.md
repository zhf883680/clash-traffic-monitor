# Design: Domain Grouping & Traffic Labeling

**Status:** Draft  
**Scope:** Phase 1 (eTLD+1 auto-grouping) + Phase 2 (label_rules system)  
**Last updated:** 2026-04-29

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Feature Summary](#2-feature-summary)
3. [Design Decisions & Trade-offs](#3-design-decisions--trade-offs)
4. [Architecture](#4-architecture)
5. [Phase 1 — eTLD+1 Auto-Grouping](#5-phase-1--etld1-auto-grouping)
6. [Phase 2 — Label Rules System](#6-phase-2--label-rules-system)
7. [API Reference](#7-api-reference)
8. [Data Models & Schema](#8-data-models--schema)
9. [Future Work](#9-future-work)
10. [Phase 1 Implementation Tasks](#10-phase-1-implementation-tasks)
11. [Phase 2 Implementation Tasks](#11-phase-2-implementation-tasks)

---

## 1. Problem Statement

### 1.1 CDN Hostname Fragmentation

CDN-heavy services (YouTube, Netflix, etc.) distribute traffic across dozens of ephemeral, session-specific subdomains. A typical session produces entries like:

```
r1---sn-abc123.googlevideo.com      82 MB
r4---sn-xyz789.googlevideo.com      61 MB
rr3---sn-def456.googlevideo.com     47 MB
r7---sn-ghi012.googlevideo.com      39 MB
...  (20+ more rows)
```

The host list becomes noisy and the total YouTube traffic — the only number the user cares about — is invisible. The same pattern affects Akamai, Fastly, CloudFront, and virtually every major CDN.

### 1.2 IP-Only Traffic (No Hostname)

Mihomo routes traffic for some services entirely by IPCIDR rules, meaning no hostname is resolved or stored. Telegram is the canonical example:

```
91.108.56.111     12 MB
91.108.56.100      8 MB
149.154.167.41     6 MB
...
```

The underlying ASN is `AS62041` (Telegram Messenger), but the dashboard shows raw IPs with no label. The user has to cross-reference Mihomo's ruleset manually to understand what service this is.

### 1.3 Goal

Show traffic grouped by **logical service**, not by DNS artifact or raw IP:

```
googlevideo.com    229 MB    ▶ (drill down to subdomains)
Telegram           26 MB     ▶ (drill down to IPs)
```

---

## 2. Feature Summary

| | Phase 1 | Phase 2 |
|---|---|---|
| **What it does** | Auto-groups subdomains to eTLD+1 | User-defined rules with custom labels |
| **Covers** | CDN fragmentation (YouTube, Netflix, etc.) | IP-only traffic (Telegram), custom labels, overrides |
| **Rule source** | Automatic heuristic | Manual rules table |
| **Config** | Global on/off toggle | Per-rule CRUD |
| **Retroactive** | Yes — query-time, no rewrite | Yes — query-time, no rewrite |
| **Requires DB change** | `app_settings` key only | New `label_rules` table |

---

## 3. Design Decisions & Trade-offs

### 3.1 Where to Apply Grouping: Query-time vs. Write-time

**Options considered:**

| | Write-time (normalize on ingest) | Query-time (normalize on read) |
|---|---|---|
| Description | `normalizeHost()` called in `processConnections()`, stored normalized value | Raw host stored; normalization applied in query handlers |
| Raw data preserved | No — original subdomain lost | Yes — full fidelity always available |
| Retroactive rule changes | No — must re-ingest or rewrite rows | Yes — change rule, instant effect on all history |
| Performance | Cheaper at read time | Cheap enough (in-memory, no SQL overhead) |
| Rule-change cost | High (data migration or dual-store) | Zero |

**Decision: Query-time.** The primary driver is retroactivity — when a user adds a CIDR rule for Telegram, they want it to apply to historical data immediately. Write-time would require either discarding history or maintaining a second normalized column. Raw data is also more valuable for future features (PSL upgrade, export, debugging).

---

### 3.2 eTLD+1: Heuristic vs. Public Suffix List (PSL)

**The problem:** What does "last 2 domain parts" actually mean?

```
r1---sn-abc.googlevideo.com   → "last 2 parts" → googlevideo.com   ✓
bbc.co.uk                     → "last 2 parts" → co.uk             ✗ (should be bbc.co.uk)
user.github.io                → "last 2 parts" → github.io         ✗ (should be user.github.io)
s3.amazonaws.com              → "last 2 parts" → amazonaws.com     ✗ (should be s3.amazonaws.com)
```

**Options:**

| | Heuristic (last 2 parts) | Public Suffix List (PSL) |
|---|---|---|
| Correctness | ~95% for common cases | 100% per IANA/Mozilla data |
| Dependencies | Zero | ~100 KB data file or external lib |
| Multi-part TLDs (.co.uk) | Broken — strips too much | Correct |
| Delegated registries (.github.io) | Broken — strips too much | Correct |
| Complexity | ~10 lines | ~200 lines + data bundling |
| Updatability | N/A | PSL data ages |

**Decision: Heuristic for Phase 1, with a PSL seam.** The heuristic works for the primary use cases (CDN subdomains under single-part TLDs like `.com`, `.net`, `.io`). A single `normalizeHost(host string) string` function is the only place the heuristic lives — replacing it with a PSL implementation later requires changing exactly one function with zero API surface changes.

**Known limitations of the heuristic:**
- `bbc.co.uk` → groups to `co.uk` (wrong; all UK sites collapse)
- `user.github.io` → groups to `github.io` (wrong; all GitHub Pages collapse)
- `mybucket.s3.amazonaws.com` → groups to `amazonaws.com` (debatable)
- Any ccTLD with 2-part registrations

These are acceptable until PSL is implemented in a future phase.

---

### 3.3 Rule Storage: `app_settings` vs. Dedicated `label_rules` Table

**`app_settings` (existing key-value store):**
```sql
CREATE TABLE IF NOT EXISTS app_settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);
```

Storing rules here would require serializing a JSON array:
```
key: "label_rules"  value: '[{"type":"cidr","pattern":"91.108.0.0/16","label":"Telegram",...}]'
```

| | app_settings (JSON blob) | label_rules (dedicated table) |
|---|---|---|
| Individual rule CRUD | Full read-modify-write cycle on every change | Direct INSERT/UPDATE/DELETE by id |
| Ordering | Must encode in JSON | `priority` column, ORDER BY |
| SQL filtering | Cannot query by type/pattern/label | Full SQL expressivity |
| Conflict detection | Manual JSON scan | UNIQUE constraint |
| Future: Mihomo sync column | Adding to JSON schema | ALTER TABLE ADD COLUMN |
| Existing pattern | Yes | No (new table) |

**Decision: Dedicated `label_rules` table.** Rules are structured records needing individual CRUD and ordering — the key-value model is wrong for this. The "it needs a schema migration" concern is minimal: it's one `CREATE TABLE IF NOT EXISTS` block in `openDatabase()`, identical in complexity to every other table in the schema.

---

### 3.4 Drill-down UX: New View vs. Navigate to Substats

When a user clicks a grouped row (e.g., `googlevideo.com 229 MB`), they should be able to see the underlying subdomains.

**Options:**
- **Inline expand:** Expand the row in place to show sub-rows
- **Navigate to substats:** Reuse the existing drill-down pattern (click row → substats view filtered by that value)

**Decision: Navigate to substats.** The substats view already exists, already handles filtering, already has its own trend/detail interactions. Building a new inline expand would duplicate that logic. The user explicitly selected this option.

---

### 3.5 Toggle Scope: Global vs. Per-dimension vs. Per-session

**Decision: Global toggle in `app_settings`.** Stored as `domain_grouping_enabled` (`"1"` / `"0"`). A per-dimension toggle would complicate the UI and the query logic. Per-session (URL param) would be invisible to non-technical users. Global is the simplest contract: one checkbox in settings affects all views.

---

### 3.6 Chains-based View (Rejected)

Mihomo already evaluates its ruleset and stores the matched proxy chain in the `chains` field. One could label Telegram traffic by detecting "TELEGRAM" in chains. This was considered and rejected because:

- Not all users name their proxy groups after services
- `chains` is a proxy routing artifact, not a service identity
- It would work for Telegram only if a group literally named "Telegram" exists
- Phase 2 CIDR rules solve the same problem generically

---

## 4. Architecture

### 4.1 Overall Data Flow

```
Mihomo /connections
        │
        ▼
processConnections()          ← raw host + IP stored as-is
        │
        ▼
aggregateBuffer               ← in-memory 1-min buckets
        │  (flush after 10 min)
        ▼
traffic_aggregated (SQLite)   ← raw host + IP persisted

                              ┌─────────────────────────────────┐
                              │   QUERY LAYER (new, Phase 1+2)  │
                              │                                 │
  HTTP request                │  1. Load label_rules from DB    │
        │                     │  2. Read domain_grouping toggle │
        ▼                     │  3. For each row:               │
  queryAggregate()   ─────────►     applyLabel(host) →          │
  queryByFilters()            │       Phase 2: exact match      │
                              │       Phase 2: wildcard match   │
                              │       Phase 2: CIDR match       │
                              │       Phase 1: eTLD+1 fallback  │
                              │  4. Merge rows sharing same     │
                              │     normalized host/label       │
                              │  5. Return to frontend          │
                              └─────────────────────────────────┘
```

### 4.2 Label Resolution Pipeline

```
applyLabel(rawHost string, rules []labelRule, groupingEnabled bool) string
          │
          ├─ if !groupingEnabled → return rawHost (passthrough)
          │
          ├─ Phase 2: iterate rules by priority (ascending)
          │     ├─ type="domain", pattern="telegram.org"
          │     │     └─ exact match? → return rule.label
          │     ├─ type="domain", pattern="*.googlevideo.com"
          │     │     └─ wildcard match? → return rule.label
          │     └─ type="cidr", pattern="91.108.0.0/16"
          │           └─ rawHost is IP && IP in CIDR? → return rule.label
          │
          └─ Phase 1 fallback: normalizeHost(rawHost)
                └─ normalizeHost("r1---sn-abc.googlevideo.com")
                        → "googlevideo.com"
```

### 4.3 Rule Matching Order

```
Priority 1 (lowest number = checked first)
    │
    ├── exact domain   "telegram.org"           → literal string match
    ├── wildcard       "*.googlevideo.com"       → suffix match after stripping "*."
    ├── cidr           "91.108.0.0/16"           → net.ParseCIDR + IP contains
    │
    └── (no rule matched)
           │
           └── Phase 1 eTLD+1 heuristic         → last 2 parts of hostname
                      │
                      └── (rawHost is a bare IP) → return rawHost unchanged
```

### 4.4 Component Map

```
main.go
  │
  ├── openDatabase()
  │     └── CREATE TABLE label_rules  (Phase 2)
  │
  ├── normalizeHost(host string) string         ← Phase 1 seam
  │     └── strips all but last 2 domain parts
  │
  ├── applyLabel(host, rules, enabled) string   ← Phase 2 dispatcher
  │     ├── matchExactDomain()
  │     ├── matchWildcard()
  │     ├── matchCIDR()
  │     └── normalizeHost()  (fallback)
  │
  ├── loadLabelRules(db) []labelRule             ← Phase 2 loader
  │
  ├── queryAggregate()     ← apply label pipeline here
  ├── queryByFilters()     ← apply label pipeline here
  │
  ├── handleLabelRules()   ← Phase 2 CRUD handler
  │     ├── GET    → list all rules
  │     ├── POST   → create rule
  │     ├── PUT    → update rule by id
  │     └── DELETE → delete rule by id
  │
  └── handleAggregate / handleSubstats
        └── pass groupingEnabled from app_settings to queryAggregate
```

---

## 5. Phase 1 — eTLD+1 Auto-Grouping

### 5.1 Feature Behaviour

When `domain_grouping_enabled = "1"` (default: on):

- All query responses normalize `host` to its eTLD+1 before aggregating
- Rows sharing the same normalized host are **summed** — upload, download, count all merged
- The original subdomain is preserved in SQLite; normalization is query-time only
- Drill-down from a grouped row navigates to the existing substats view with `host=googlevideo.com` filter

When disabled:
- All existing behaviour unchanged — raw hostnames returned

### 5.2 `normalizeHost` Specification

```go
// normalizeHost returns the eTLD+1 of a hostname using a 2-part heuristic.
// IP addresses and single-label hosts are returned unchanged.
// Seam: replace the body of this function with a PSL lookup to upgrade accuracy.
func normalizeHost(host string) string {
    // Strip port if present
    // Return bare IPs unchanged
    // Return single-label hosts (e.g., "localhost") unchanged
    // Split by ".", take last 2 parts
    // Return "parts[n-2].parts[n-1]"
}
```

Examples:

| Input | Output | Notes |
|---|---|---|
| `r1---sn-abc.googlevideo.com` | `googlevideo.com` | CDN subdomain stripped |
| `googlevideo.com` | `googlevideo.com` | Already eTLD+1 |
| `www.youtube.com` | `youtube.com` | Normal subdomain stripped |
| `91.108.56.111` | `91.108.56.111` | IP unchanged |
| `localhost` | `localhost` | Single label unchanged |
| `bbc.co.uk` | `co.uk` | Known limitation — PSL would fix |
| `user.github.io` | `github.io` | Known limitation — PSL would fix |

### 5.3 Query Aggregation Merge

After applying `normalizeHost` (or `applyLabel` in Phase 2), the result set may have multiple rows with the same normalized host. These must be merged:

```
Before normalization:
  r1---sn-abc.googlevideo.com   82 MB down,  1 MB up
  r4---sn-xyz.googlevideo.com   61 MB down,  1 MB up
  rr3---sn-def.googlevideo.com  47 MB down,  0 MB up

After normalization + merge:
  googlevideo.com              190 MB down,  2 MB up   count=3
```

This is a post-SQL in-memory reduction step — we do not modify the SQL query itself.

### 5.4 Drill-down Behaviour

When the user clicks `googlevideo.com` (normalized) in the host tab:

- The substats endpoint receives `host=googlevideo.com`
- The query handler detects this is a normalized (eTLD+1) host and runs a `LIKE '%googlevideo.com'` match against `traffic_aggregated.host`
- The substats view shows all raw subdomains grouped under that domain

Alternatively: pass a `raw=1` flag in the substats request to disable normalization for that call, showing the literal subdomains of the matched host.

### 5.5 Settings Toggle

Stored in `app_settings`:
```
key: "domain_grouping_enabled"   value: "1" (default) | "0"
```

Exposed via `GET/PUT /api/settings/mihomo` or a new settings endpoint. The toggle is displayed in the existing Settings modal alongside the Mihomo URL config.

---

## 6. Phase 2 — Label Rules System

### 6.1 Feature Behaviour

Users can define custom rules that map patterns to human-readable labels:

| Rule type | Pattern | Label | Effect |
|---|---|---|---|
| `cidr` | `91.108.0.0/16` | `Telegram` | All IPs in range show as "Telegram" |
| `cidr` | `149.154.160.0/20` | `Telegram` | Same — multiple CIDRs per label OK |
| `domain` | `*.googlevideo.com` | `YouTube CDN` | Overrides eTLD+1 for this domain |
| `domain` | `netflix.com` | `Netflix` | Exact domain match |

Rules are evaluated before the eTLD+1 fallback, in ascending `priority` order. A matching rule short-circuits — no further rules checked.

### 6.2 Rule Types

**`type = "domain"`**
- Pattern `"telegram.org"` → exact match against normalized host
- Pattern `"*.googlevideo.com"` → wildcard: `host` ends with `.googlevideo.com`
- Wildcard only supports leading `*` (e.g., `*.example.com`), not mid-string wildcards

**`type = "cidr"`**
- Pattern `"91.108.0.0/16"` → parsed as `net.IPNet`; matches when `rawHost` is a bare IP within that range
- IPv4 and IPv6 both supported
- Invalid CIDR patterns are skipped with a log warning (not a startup error)

### 6.3 Matching Algorithm

```
for rule in sortedByPriority(enabledRules):
    if rule.type == "domain":
        if rule.pattern starts with "*." :
            suffix = rule.pattern[1:]   // ".googlevideo.com"
            if strings.HasSuffix(host, suffix): return rule.label
        else:
            if host == rule.pattern: return rule.label
    else if rule.type == "cidr":
        if isIPAddress(host):
            ip = net.ParseIP(host)
            if cidr.Contains(ip): return rule.label

// no rule matched
return normalizeHost(host)   // Phase 1 fallback
```

### 6.4 Rules UI (Settings Modal)

A new "Label Rules" section in the settings modal:

```
┌─────────────────── Label Rules ───────────────────────────────────────┐
│  [+ Add Rule]                                                         │
│                                                                       │
│  #   Pri  Type    Pattern             Label           Enabled         │
│  ─── ──── ─────── ─────────────────── ─────────────── ───────         │
│  1   10   cidr    91.108.0.0/16       Telegram        [✓]  [✎] [✗]   │
│  2   10   cidr    149.154.160.0/20    Telegram        [✓]  [✎] [✗]   │
│  3   20   domain  *.googlevideo.com   YouTube CDN     [✓]  [✎] [✗]   │
│                                                                       │
│  [Save]                                                               │
└───────────────────────────────────────────────────────────────────────┘
```

Inline edit form appears when `[✎]` is clicked. Rows reorder by priority automatically.

### 6.5 Rule Caching

Label rules are loaded from SQLite on each query. For Phase 2 this is acceptable (rules list is small, <100 rows typical). A future optimization could cache rules in `service` struct and invalidate on write.

---

## 7. API Reference

### 7.1 Existing Endpoints (Modified Behaviour)

| Endpoint | Change |
|---|---|
| `GET /api/traffic/aggregate` | When `domain_grouping_enabled=1`, returns normalized/labeled hosts; rows merged |
| `GET /api/traffic/substats` | Accepts normalized host as filter; runs LIKE match on raw `host` column |
| `GET /api/traffic/trend` | Same normalization applied to host dimension |

**New query parameter (Phase 1):**
```
GET /api/traffic/aggregate?raw=1
```
Disables grouping for this request (e.g., for drill-down into raw subdomains).

### 7.2 New Endpoints (Phase 2)

```
GET    /api/label-rules              List all rules (ordered by priority)
POST   /api/label-rules              Create a rule
PUT    /api/label-rules/{id}         Update a rule
DELETE /api/label-rules/{id}         Delete a rule
PATCH  /api/label-rules/{id}/toggle  Enable/disable a rule
```

### 7.3 Settings Endpoint Extension (Phase 1)

```
GET  /api/settings/mihomo
PUT  /api/settings/mihomo

Extended response body:
{
  "url": "http://localhost:9090",
  "secret": "...",
  "domainGroupingEnabled": true      ← new field
}
```

### 7.4 `GET /api/label-rules` Response

```json
[
  {
    "id": 1,
    "type": "cidr",
    "pattern": "91.108.0.0/16",
    "label": "Telegram",
    "priority": 10,
    "enabled": true
  },
  {
    "id": 3,
    "type": "domain",
    "pattern": "*.googlevideo.com",
    "label": "YouTube CDN",
    "priority": 20,
    "enabled": true
  }
]
```

### 7.5 `POST /api/label-rules` Request Body

```json
{
  "type": "cidr",
  "pattern": "91.108.0.0/16",
  "label": "Telegram",
  "priority": 10,
  "enabled": true
}
```

Validation:
- `type` must be `"domain"` or `"cidr"`
- `pattern` must be non-empty; for `cidr` type, must be parseable by `net.ParseCIDR`
- `label` must be non-empty
- `priority` must be >= 0

---

## 8. Data Models & Schema

### 8.1 Existing Schema (unchanged)

```sql
-- Primary query table — raw host values stored, never normalized
CREATE TABLE IF NOT EXISTS traffic_aggregated (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_start INTEGER NOT NULL,
    bucket_end   INTEGER NOT NULL,
    source_ip    TEXT NOT NULL,
    host         TEXT NOT NULL,         -- raw: "r1---sn-abc.googlevideo.com" or "91.108.56.111"
    destination_ip TEXT NOT NULL DEFAULT '',
    process      TEXT NOT NULL,
    outbound     TEXT NOT NULL,
    chains       TEXT NOT NULL DEFAULT '[]',
    upload       INTEGER NOT NULL,
    download     INTEGER NOT NULL,
    count        INTEGER NOT NULL,
    UNIQUE(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains)
);

-- Scalar config store
CREATE TABLE IF NOT EXISTS app_settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);
-- Existing keys: mihomo_url, mihomo_secret, auto_switch_enabled,
--               auto_switch_threshold_*, auto_switch_window_*
-- New key (Phase 1): domain_grouping_enabled
```

### 8.2 New: `label_rules` Table (Phase 2)

```sql
CREATE TABLE IF NOT EXISTS label_rules (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    type     TEXT    NOT NULL CHECK(type IN ('domain', 'cidr')),
    pattern  TEXT    NOT NULL,
    label    TEXT    NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    enabled  INTEGER NOT NULL DEFAULT 1
    -- future: source TEXT DEFAULT 'manual'  (for Mihomo sync)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_label_rules_pattern
    ON label_rules(type, pattern);

CREATE INDEX IF NOT EXISTS idx_label_rules_priority
    ON label_rules(priority ASC, id ASC);
```

**Column notes:**
- `type`: enum enforced by CHECK constraint
- `pattern`: unique per type — same CIDR can't appear twice
- `priority`: lower = checked first; ties broken by `id`
- `enabled`: 0/1 integer (SQLite has no boolean type)
- `source` column (commented out): reserved for Phase 3 Mihomo rule sync without a schema change

### 8.3 Go Structs

```go
// Phase 1
// normalizeHost encapsulates the eTLD+1 heuristic.
// This function is the sole seam for a future PSL upgrade.
func normalizeHost(host string) string { ... }

// Phase 2
type labelRule struct {
    ID       int64  `json:"id"`
    Type     string `json:"type"`     // "domain" | "cidr"
    Pattern  string `json:"pattern"`
    Label    string `json:"label"`
    Priority int    `json:"priority"`
    Enabled  bool   `json:"enabled"`
}

// applyLabel resolves a raw host to its display label.
// Rules are evaluated in priority order; normalizeHost is the fallback.
func applyLabel(host string, rules []labelRule, groupingEnabled bool) string { ... }
```

---

## 9. Future Work

### 9.1 PSL Upgrade (Phase 3)

Replace the body of `normalizeHost()` with a proper Public Suffix List lookup:

- Bundle `public_suffix_list.dat` as an embedded asset (≈100 KB)
- Use `golang.org/x/net/publicsuffix` or a custom parser
- Zero API surface changes — `normalizeHost` signature stays identical
- Fixes `bbc.co.uk → co.uk` and `user.github.io → github.io` regressions

### 9.2 Mihomo Rule Sync (Phase 3)

Import IPCIDR and DOMAIN rules directly from Mihomo's parsed ruleset:

```
GET {mihomo}/rules   → [{type:"IPCIDR", payload:"91.108.0.0/16", proxy:"TELEGRAM"}, ...]
```

Map Mihomo rule types to `label_rules`:
- `IPCIDR` → `type="cidr"`
- `DOMAIN-SUFFIX` → `type="domain"`, prefix with `*.`
- `DOMAIN` → `type="domain"`

The reserved `source TEXT DEFAULT 'manual'` column distinguishes synced from user-defined rules. Synced rules would be read-only in the UI.

To enable: add the `source` column via `ALTER TABLE label_rules ADD COLUMN source TEXT DEFAULT 'manual'` (no data loss).

### 9.3 Rule Performance Cache

For high-throughput environments, cache `[]labelRule` in the `service` struct:

```go
type service struct {
    ...
    labelRulesMu    sync.RWMutex
    labelRulesCache []labelRule
    labelRulesDirty bool
}
```

Invalidate on any write to `label_rules`. Load on first query after invalidation.

### 9.4 Per-dimension Toggle

Allow grouping on `host` dimension but not `sourceIP` or `outbound`. Would require `domain_grouping_enabled` to become a per-dimension setting or a query parameter.

---

## 10. Phase 1 Implementation Tasks

All changes are in `main.go` and `web/` unless noted. No new files required.

---

### Task 1: `normalizeHost()` function

**File:** `main.go`  
**Location:** After existing helper functions, before query handlers (~line 1630)

```go
func normalizeHost(host string) string {
    // 1. Strip port (host may be "example.com:443")
    // 2. Return bare IPs unchanged (net.ParseIP != nil)
    // 3. Split by ".", if len(parts) <= 2 return host unchanged
    // 4. Return parts[len-2] + "." + parts[len-1]
}
```

Write unit tests in `main_test.go` covering:
- CDN subdomain (`r1---sn-abc.googlevideo.com` → `googlevideo.com`)
- Bare IPv4 (`91.108.56.111` → unchanged)
- Bare IPv6 (`2001:db8::1` → unchanged)
- Already eTLD+1 (`youtube.com` → unchanged)
- Single label (`localhost` → unchanged)
- Host with port (`example.com:443` → `example.com`)
- Three-part domain (`www.youtube.com` → `youtube.com`)

---

### Task 2: `domain_grouping_enabled` setting

**File:** `main.go`

2a. Add `domainGroupingEnabled bool` field to the response struct for `GET /api/settings/mihomo`.

2b. In `handleMihomoSettings` GET handler: read `domain_grouping_enabled` from `app_settings`, include in response.

2c. In `handleMihomoSettings` PUT handler: accept and persist `domainGroupingEnabled` to `app_settings`.

2d. Add helper `loadDomainGroupingEnabled(db *sql.DB) bool` — defaults to `true` if key absent.

---

### Task 3: Post-query merge in `queryAggregate`

**File:** `main.go`  
**Location:** `queryAggregate()` and any callers that build `[]aggregatedData`

3a. After building the raw result slice, if `groupingEnabled`:
- For each row, replace `row.Label` (the host) with `normalizeHost(row.Label)`
- Accumulate into a `map[string]*aggregatedData`
- Merge: `upload += row.Upload`, `download += row.Download`, `count += row.Count`
- Convert map back to sorted slice

3b. Ensure `handleAggregate` passes `groupingEnabled` to `queryAggregate`.

3c. Ensure `handleSubstats` **disables** normalization (always passes `groupingEnabled=false`) so the drill-down view always shows raw subdomains.

---

### Task 4: Substats drill-down — LIKE match for normalized hosts

**File:** `main.go`  
**Location:** `handleSubstats` / underlying query

When `host` filter value is a normalized eTLD+1 (e.g., `googlevideo.com`) rather than a full subdomain, the existing `WHERE host = ?` will return zero rows.

4a. Detect if the filter host is a normalized form: it matches its own `normalizeHost()` output AND is not an IP address.

4b. If so, change the SQL predicate from `host = ?` to `host LIKE ?` with value `'%' + host`.

4c. This ensures clicking `googlevideo.com` in the host tab shows all raw subdomains in substats.

---

### Task 5: `raw=1` query parameter (optional, but enables testing)

**File:** `main.go`  
**Location:** `handleAggregate`

5a. If `?raw=1` is present in the request, set `groupingEnabled = false` regardless of the setting.

5b. This allows the frontend (and tests) to compare normalized vs. raw output without toggling the global setting.

---

### Task 6: Settings UI toggle

**File:** `web/app.js`, `web/index.html`, `web/styles.css`

6a. In the Settings modal (existing), add a checkbox row: **"Group subdomains by root domain"**.

6b. On modal open: `GET /api/settings/mihomo` → populate checkbox from `domainGroupingEnabled`.

6c. On Save: include `domainGroupingEnabled` in the `PUT /api/settings/mihomo` body.

6d. After save, refresh the currently active tab's data so the effect is immediately visible.

---

### Task 7: Unit tests

**File:** `main_test.go`

7a. `TestNormalizeHost` — all cases from Task 1 spec.

7b. `TestQueryAggregateGrouping` — given a set of raw aggregatedData rows with CDN subdomains, assert the grouped output merges them correctly.

7c. `TestDomainGroupingToggle` — assert that `raw=1` or `groupingEnabled=false` returns un-merged rows.

---

### Task 8: Integration smoke test (manual)

Before marking Phase 1 complete:

- [ ] Build and run: `go build -o traffic-monitor main.go && ./traffic-monitor`
- [ ] Verify host tab shows `googlevideo.com` instead of 20+ subdomains
- [ ] Click `googlevideo.com` → substats shows individual subdomains
- [ ] Disable grouping in settings → host tab reverts to raw subdomains
- [ ] Re-enable → grouping resumes, no data loss
- [ ] `go test ./...` passes

---

### Phase 1 Task Summary

| # | Task | Scope | Complexity |
|---|---|---|---|
| 1 | `normalizeHost()` function + tests | `main.go`, `main_test.go` | Low |
| 2 | `domain_grouping_enabled` setting R/W | `main.go` | Low |
| 3 | Post-query merge in `queryAggregate` | `main.go` | Medium |
| 4 | Substats LIKE match for normalized hosts | `main.go` | Low |
| 5 | `?raw=1` passthrough param | `main.go` | Low |
| 6 | Settings UI toggle | `web/` | Low |
| 7 | Unit tests | `main_test.go` | Medium |
| 8 | Manual smoke test | — | Low |

---

## 11. Phase 2 Implementation Tasks

All changes are in `main.go` and `web/` unless noted. Phase 1 must be complete before starting Phase 2.

---

### Task 1: `label_rules` DB schema

**File:** `main.go`  
**Location:** `openDatabase()`, alongside existing `CREATE TABLE` statements

1a. Add the table and indexes from §8.2:

```go
_, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS label_rules (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        type     TEXT    NOT NULL CHECK(type IN ('domain', 'cidr')),
        pattern  TEXT    NOT NULL,
        label    TEXT    NOT NULL,
        priority INTEGER NOT NULL DEFAULT 100,
        enabled  INTEGER NOT NULL DEFAULT 1
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_label_rules_pattern
        ON label_rules(type, pattern);
    CREATE INDEX IF NOT EXISTS idx_label_rules_priority
        ON label_rules(priority ASC, id ASC);
`)
```

1b. No migration needed — `CREATE TABLE IF NOT EXISTS` is idempotent against existing databases.

---

### Task 2: `labelRule` struct + `loadLabelRules()`

**File:** `main.go`  
**Location:** Alongside existing struct definitions and helper functions

2a. Define the struct (from §8.3):

```go
type labelRule struct {
    ID       int64  `json:"id"`
    Type     string `json:"type"`
    Pattern  string `json:"pattern"`
    Label    string `json:"label"`
    Priority int    `json:"priority"`
    Enabled  bool   `json:"enabled"`
}
```

2b. Implement `loadLabelRules(db *sql.DB) ([]labelRule, error)`:
- `SELECT id, type, pattern, label, priority, enabled FROM label_rules WHERE enabled = 1 ORDER BY priority ASC, id ASC`
- Return the slice; caller logs and ignores error if non-critical

---

### Task 3: `applyLabel()` function

**File:** `main.go`  
**Location:** After `normalizeHost()`, before query handlers

Implement `applyLabel(host string, rules []labelRule, groupingEnabled bool) string` per §4.2 and §6.3:

```go
func applyLabel(host string, rules []labelRule, groupingEnabled bool) string {
    if !groupingEnabled {
        return host
    }
    for _, r := range rules {
        switch r.Type {
        case "domain":
            if strings.HasPrefix(r.Pattern, "*.") {
                if strings.HasSuffix(host, r.Pattern[1:]) {
                    return r.Label
                }
            } else if host == r.Pattern {
                return r.Label
            }
        case "cidr":
            if ip := net.ParseIP(host); ip != nil {
                if _, cidr, err := net.ParseCIDR(r.Pattern); err == nil {
                    if cidr.Contains(ip) {
                        return r.Label
                    }
                }
            }
        }
    }
    return normalizeHost(host)
}
```

Note: invalid CIDR patterns are silently skipped (the `err == nil` guard). Pre-parsing CIDRs in `loadLabelRules` is a future optimization (§9.3).

---

### Task 4: Wire `applyLabel` into query handlers

**File:** `main.go`  
**Location:** `queryAggregate()` and any callers that call `normalizeHost()` directly

4a. In `queryAggregate()` (and equivalent merge path in `queryByFilters()`):
- Load rules once per request: `rules, _ := loadLabelRules(svc.db)`
- Replace `normalizeHost(row.Label)` with `applyLabel(row.Label, rules, groupingEnabled)`

4b. The merge-by-key logic introduced in Phase 1 (Task 3) is unchanged — it already keys on the normalized/labeled string, so CIDR-labeled rows (e.g., multiple IPs all resolving to `"Telegram"`) will merge correctly.

4c. `handleSubstats` remains unaffected — it still calls with `groupingEnabled=false` so raw subdomains are shown in drill-down.

---

### Task 5: `handleLabelRules` CRUD handler

**File:** `main.go`  
**Location:** Alongside existing `handle*` functions; routes registered near line 1743

5a. Implement a single `handleLabelRules(w http.ResponseWriter, r *http.Request)` function dispatching on method and path suffix:

| Method | Path suffix | Action |
|---|---|---|
| `GET` | `/api/label-rules` | List all rules (including disabled), ordered by priority |
| `POST` | `/api/label-rules` | Create rule — validate type, pattern, label; return 201 + created object |
| `PUT` | `/api/label-rules/{id}` | Full update of type/pattern/label/priority/enabled |
| `DELETE` | `/api/label-rules/{id}` | Delete by id; return 204 |
| `PATCH` | `/api/label-rules/{id}/toggle` | Flip `enabled` 0↔1; return updated object |

5b. Validation for POST/PUT (per §7.5):
- `type` ∈ `{"domain", "cidr"}`
- `pattern` non-empty; for `cidr`, must pass `net.ParseCIDR`
- `label` non-empty
- `priority` >= 0

5c. Register routes in the existing route block:
```go
mux.HandleFunc("/api/label-rules", svc.handleLabelRules)
mux.HandleFunc("/api/label-rules/", svc.handleLabelRules)
```

---

### Task 6: Settings UI — Label Rules panel

**File:** `web/app.js`, `web/index.html`, `web/styles.css`

6a. Add a "Label Rules" section below the existing domain-grouping toggle in the Settings modal (see §6.4 wireframe).

6b. On modal open: `GET /api/label-rules` → render the rules table with Edit / Delete / Toggle controls.

6c. "Add Rule" button: show an inline form with fields for Type, Pattern, Label, Priority; `POST /api/label-rules` on submit.

6d. Edit (`✎`): populate the inline form with the selected rule's values; `PUT /api/label-rules/{id}` on save.

6e. Delete (`✗`): confirm then `DELETE /api/label-rules/{id}`; remove row from table.

6f. Toggle checkbox: `PATCH /api/label-rules/{id}/toggle`; update row enabled state in place.

6g. After any write, reload the active tab's traffic data so label changes take effect immediately (same pattern as the Phase 1 toggle).

---

### Task 7: Unit tests

**File:** `main_test.go`

7a. `TestApplyLabel`:
- CIDR match: IP in range → label returned
- CIDR no-match: IP outside range → falls back to `normalizeHost`
- Wildcard domain: `r1---sn.googlevideo.com` + rule `*.googlevideo.com → "YouTube CDN"` → `"YouTube CDN"`
- Exact domain: `telegram.org` + rule → label
- Grouping disabled: rules present but `groupingEnabled=false` → raw host returned
- Priority order: two rules match — lower priority number wins
- Invalid CIDR in rule: skipped without panic, fallback used

7b. `TestLoadLabelRules`:
- Disabled rules excluded from result
- Results ordered by priority ASC, id ASC

7c. `TestLabelRulesCRUD` (uses in-memory `:memory:` SQLite):
- Create, read, update, delete, toggle a rule via the DB layer

---

### Task 8: Integration smoke test (manual)

Before marking Phase 2 complete:

- [ ] Build and run: `go build -o traffic-monitor main.go && ./traffic-monitor`
- [ ] Open Settings → Label Rules; add a CIDR rule `91.108.0.0/16 → Telegram` with priority 10
- [ ] Verify IPs in that range show as "Telegram" in the host tab
- [ ] Click "Telegram" → substats shows individual raw IPs
- [ ] Add a wildcard rule `*.googlevideo.com → YouTube CDN`; verify it overrides the eTLD+1 grouping
- [ ] Disable the rule → traffic reverts to `googlevideo.com` label
- [ ] Delete the rule → removed from list, traffic still grouped by eTLD+1
- [ ] Toggle domain grouping off → all rules ignored, raw hosts shown
- [ ] `go test ./...` passes

---

### Phase 2 Task Summary

| # | Task | Scope | Complexity |
|---|---|---|---|
| 1 | `label_rules` DB schema | `main.go` (`openDatabase`) | Low |
| 2 | `labelRule` struct + `loadLabelRules()` | `main.go` | Low |
| 3 | `applyLabel()` function | `main.go` | Medium |
| 4 | Wire `applyLabel` into query handlers | `main.go` | Low |
| 5 | `handleLabelRules` CRUD handler + routes | `main.go` | Medium |
| 6 | Settings UI — Label Rules panel | `web/` | High |
| 7 | Unit tests | `main_test.go` | Medium |
| 8 | Manual smoke test | — | Low |
