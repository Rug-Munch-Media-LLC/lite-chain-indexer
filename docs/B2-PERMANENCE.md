# B2 Permanence + 90-Day Retention — lite-chain-indexer

Status: DESIGN (proposed). Author: architecture lane, 2026-10-03.
Scope: the canonical cold/permanent tier for chain Parquet, replacing the
rclone-crypt ship path with Backblaze B2 as the canonical mechanism.

Doctrine (from `rmi_playbook(indexers)`, verified 2026-09-10):
**local storage is a cache, not the record.** Hourly Parquet rotation +
B2 upload is the durability contract. A failing daily integrity-check means
data loss, not noise.

---

## 0. Executive summary

The requested end-state — "B2 MCP is canonical, retire rclone" — is
**blocked today by two defects, not by design**. A design that ignores this
will ship a lake nobody can read or ship to.

1. **The B2 MCP cannot write.** The MCP's `s3_*` tools are all
   S3-compatible, and the MCP's key is an **account master key**. Backblaze
   rejects master keys on its S3 API (`InvalidAccessKeyId`). The **native
   B2 tools work fine** (38 capabilities, including `writeFiles`). So
   today the MCP is a *control-plane* tool (create/inspect/lifecycle) and
   *cannot* be the data plane.
2. **`rmi-chain-lake-90d` already exists** — non-crypt, allPrivate,
   SSE-B2, with the exact `chains/` 90-day lifecycle rule this task asked
   us to create — and it is **completely empty and referenced by nothing**
   (0 refs in the repo, 0 in `/root/scripts`, 0 in systemd).

**Recommendation:** adopt the existing `rmi-chain-lake-90d` as canonical,
repair the MCP key, and use the plain `rugmunch-b2:` rclone remote as the
transport in the interim. Details and the exact credential fix in §7.

---

## 1. Verified current state (2026-10-03)

| Bucket | bucketId | Type | SSE | Lifecycle | Objects |
|---|---|---|---|---|---|
| `rmi-chain-data` | `405c80b4467ec3e1a3050c19` | **allPublic** | SSE-B2 | `parquet/` hide@90d→del@+1d | **7,506** |
| `rmi-chain-lake-90d` | `b0dc40f4c69ee3d1a3050c19` | allPrivate | SSE-B2 | `chains/` hide@90d→del@+1d | **0** |
| `rmi-chainlake` (new, this task) | `f07cf0f4166e0311a3150c19` | allPrivate | SSE-B2 | none yet | 0 (verified) |
| `rmi-backup-encrypted` | `20acd0f4667e83a193f50c19` | allPrivate | SSE-B2 | — | fileLock **enabled** |
| `rmi-backup-encrypted-v2` | `f0ece0d4464eb3e1a3050c19` | allPrivate | SSE-B2 | — | — |
| `rmi-artifacts` | `40dcd024a6eec391a3050c19` | allPrivate | SSE-B2 | `qdrant-snapshots/` hide@30d | — |

Lake contents:

- `rmi-chain-data/parquet/` — **PLAIN, unencrypted, MCP-readable.**
  7,506 files / 65.0 GB, 68 distinct days (`2026-06-25` … `2026-10-02`),
  21 chains, layout `{chain}/{YYYY-MM-DD}/{chain}[_dex]_{date}[-partNN].parquet`.
- `b2-crypt-v2:chain-data/parquet/` — **rclone-crypt encrypted, NOT
  MCP-readable.** 14,059 files, freshest day `2026-10-03` (today).
- `rugmunch-b2:rmi-chain-lake-90d/chains/` — empty.

Ship path (verified from `/root/scripts/b2-offload.sh`):

```
b2-offload.service  ExecStart=/bin/bash /root/scripts/b2-offload.sh   (daily 05:00)
  ├─ rclone sync /backups/...  b2-crypt:offsite-backups/<name>       <- 401 FAILING
  └─ rclone sync /srv/models   rugmunch-b2:rmi-artifacts/models      <- 401 FAILING
```

**The lake ship path is NOT `b2-offload`.** That service moves offsite
backups and the model lake. Chain Parquet reaches B2 by a separate leg
(the indexer's own `rclone copy`, see §5) — which is why
`rmi-chain-data/parquet` advances daily. `b2-offload` being broken is a
real and separate incident (§8, D3) but it is **not** evidence that chain
data is not shipping.

`b2-retention-status.sh` (read-only observability) already watches
`rmi-chain-data|parquet` and `rmi-artifacts|snapshots`, exporting
`b2_retention_*` Prometheus metrics. It does **not** watch
`rmi-chain-lake-90d`.

Layout source of truth (code):
- `lite_indexer/storage/parquet/helpers.py` — `_local_dir`, `_local_path`,
  `_remote_dir`, `_remote_path`: `{chain}/{YYYY-MM-DD}/`.
- `lite_indexer/storage/cold_store.py` — `ColdStore`, reads via
  `HISTORICAL_PARQUET_REMOTE` (default `b2-crypt-v2:chain-data/parquet`),
  modes `mount` / `fetch` / `httpfs`. Its own docstring records the blocker:
  *"(the production `b2-crypt:` remote is rclone-crypt encrypted and cannot
  use httpfs)"*. This is the code-level proof of the blocker.

---

## 2. The blocker, precisely

rclone-crypt encrypts **filenames and directory segments**, not just file
contents. Consequences:

1. B2 object keys under `b2-crypt-v2:` are ciphertext. `b2_lake_index.py`
   / `b2_lake_points.py` (cc-ing-* repos) and any DuckDB `read_parquet()`
   glob need plaintext keys. **The b2-storage MCP cannot produce them.**
2. `cold_store.py` can only read it via the `mount` or `fetch` mode — both
   require a local rclone + FUSE mount. A node without the mount has no
   cold tier.
3. The `httpfs` mode (`s3://…`, zero rclone dependency, DuckDB-native) is
   **structurally unavailable** against the crypt remote.

So "B2 via MCP" and "rclone-crypt" are mutually exclusive by construction.
One of them must go. This is not a tuning problem.

---

## 3. Canonical layout in the new bucket

Bucket: **`rmi-chain-lake-90d`** (`b0dc40f4c69ee3d1a3050c19`).
Non-crypt, allPrivate, SSE-B2 bucket-default encryption, no object lock.

```
chains/<chain>/<YYYY-MM-DD>/<chain>_<YYYY-MM-DD>-part<NN>.parquet
chains/<chain>/<YYYY-MM-DD>/<chain>_dex_<YYYY-MM-DD>.parquet
chains/<chain>/<YYYY-MM-DD>/_manifest.json      # coverage: start/end block, rows
chains/_probe/…                                  # health probes
```

Rules:

- **Date must stay the terminal directory segment.** Every retention and
  observability decision keys off it (§4), and `b2-retention-status.sh`
  already documents exactly this segment as its parse target.
- **`<chain>` first** so a per-chain prefix is always a directory prefix,
  never a substring.
- **`<table>` second**, so retention/scoping by table is possible without
  touching the date. `cold_store.py` needs `transfers` and `dex` for every
  chain; `spl_balance_changes` is Solana-only (see `TABLE_KINDS`).
- **`-part<NN>`** matches the observed multi-part split
  (`celo_2026-08-24-part00..08.parquet`) — ~8 parts per chain-day implies
  ~1 GB parts, so keep the producer's split as-is; do not re-partition.
- `_manifest.json` per chain-day: coverage (`start_block`/`end_block` from
  `helpers._block_range`), row counts, export timestamp, source commit. Lets
  a restore drill verify coverage without reading Parquet footers, and lets
  `cold_store` skip a day entirely when the window is empty.

**Keeps the crypt layout's shape** (`chain-data/parquet/<chain>/…`) so the
producer's `_remote_dir` needs only its remote-root prefix changed, not a
new path builder. One-line producer change, no layout migration.

---

## 4. 90-day rolling retention

### Recommendation: **B2 bucket lifecycle rules. Not a scheduled job.**

Exact rule (already present on `rmi-chain-lake-90d`, revision 2):

```
lifecycleRules: [
  { fileNamePrefix: "chains/",
    daysFromUploadingToHiding: 90,
    daysFromHidingToDeleting:  1,
    daysFromStartingToCancelingUnfinishedLargeFiles: null }
]
```

Recommended additional rule at create/update time (currently absent):

```
  { fileNamePrefix: "chains/",
    daysFromStartingToCancelingUnfinishedLargeFiles: 7 }
```

Why lifecycle beats a job:

| | Lifecycle rule | Scheduled retention job |
|---|---|---|
| Enforcement | B2-side, always on, cannot be skipped by a crashed host | Host-side; a dead/suspended timer means **unbounded growth** |
| Cost of failure | silent over-retention for ≤1 day | silent permanent retention (cost) or a bad delete (irreversible) |
| Reclamation | automatic, day after hide | depends on job health |
| Auditability | declarative config in the bucket itself | a script you must trust |
| Per-chain rolling | **not supported** — prefix + age only | yes |
| Blast radius of a bug | none (no deletion logic of ours) | full delete authority over the lake |

The task's own framing — *"B2 lifecycle only hides/deletes by prefix+age;
per-chain rolling needs either a prefix convention or a job"* — is correct,
and the resolution is the **prefix convention**, because:

- **Per-chain *different* retention is not needed.** Rolling 90d applies
  uniformly to all 20+ chains. A per-chain rule would be 20+ rules ×
  lifecycle-replace semantics for zero behavioural gain.
- **`chains/` is one stable prefix**; the date lives inside the key. A
  single age rule therefore *is* the rolling window, computed by B2 from
  upload time. We never parse dates to decide deletion.

Why the **two-phase hide→delete** matters (the non-obvious part):

- B2 delete of a non-current version in a versioned bucket creates a
  *delete marker*, hiding the file from listings **without** reclaiming
  bytes. Storage is only actually freed when the hidden version is deleted.
- So `daysFromUploadingToHiding: 90` (hide) followed by
  `daysFromHidingToDeleting: 1` (purge hidden version) is what **actually
  returns the money**. A single `daysFromUploadingToHiding`-only rule looks
  like it works — listings shrink, the bill does not.
- This matches the existing `rmi-artifacts` (`qdrant-snapshots/` hide@30d
  → del@1d) and `rmi-rag-permanence` (`snapshots/` hide@90d → del@1d)
  precedent. Consistent fleet-wide.
- Cost of the phase: up to ~1 extra day of billed bytes. Free correctness.
- `_probe/` must live **under** `chains/` so it ages out; a separate
  top-level prefix would never be reclaimed.

**Deliberately NOT enabled: Object Lock.** Not asked for, and it would
conflict with a rolling window — a retention lock cannot be shortened, so a
mis-set lock would make 90d rolling impossible to honour and force bucket
deletion (itself impossible while locked). B2 is already durable; immutability
is a separate decision for a separate ticket.

**Retained escape hatch:** the rules are *declared data* in the bucket, not
code. `b2_retention_*` metrics + `b2-retention-status.timer` (daily 06:34)
already compute `next_expiry` per prefix; extend its table to
`rmi-chain-lake-90d|chains|chains` once the lake moves.

---

## 5. The ship path

Target: hourly Parquet → B2, **no rclone dependency on the read path**,
MCP-native where the tool surface allows.

### Stage 0 — today (broken auth, crypt path)
Indexer's export leg → `rclone copy` → `b2-crypt-v2:chain-data/parquet`.
Readable only via FUSE mount or `rclone cat`.

### Stage 1 — canonical target layout, plain transport (RECOMMENDED NEXT)
1. Add `lifecycleRules` to the new bucket (the `chains/` rule above +
   the 7-day multipart-abort rule) via
   `b2_update_bucket(bucketId="b0dc40f4c69ee3d1a3050c19", …)`.
   (Gated call — confirm=true.)
2. Flip the producer's remote root to the **plain** `rugmunch-b2:` remote:
   `rmi-chain-lake-90d/chains`. This is a **prefix change only** —
   `_remote_dir` already produces `{remote}/{chain}/{date}`.
3. Point `ColdStore` at the plain remote and switch `httpfs=True`.
   `cold_store.py`'s `httpfs` branch already exists and already reads
   `s3://$B2_BUCKET/<rel>`; the **only** thing that has been blocking it is
   that the bucket was crypt. This single flip removes the rclone/FUSE
   dependency from every reader.
4. Dual-write one chain-day into the new bucket as a rehearsal; compare
   `sha256` against the crypt copy via `rclone cat` (the only way to read
   crypt) — do this **before** any cutover.
5. Cut `HISTORICAL_PARQUET_REMOTE` to the plain remote. Keep
   `b2-crypt-v2:` mounted read-only for the migration window.

*Why rclone here and not the MCP:* `s3_put_object` is unusable
(master-key rejection, §7) and the native equivalent
(`b2_upload_file`) is **not exposed by this MCP build** — the tool list is
`b2_*` + `s3_*` only, and `s3_put_object` is capped at 1 MiB with no
multipart. Shipping multi-GB Parquet through it is not possible today.
rclone is the only working writer. It is a *transport*, not the source of
truth, and Stage 2 removes it from the read path entirely.

### Stage 2 — MCP canonical (requires the §7 credential fix)
Once `B2_APPLICATION_KEY_ID` is a non-master application key:

- **Reads** go MCP-native: `b2_list_buckets`, `s3_list_objects_v2` /
  `s3_head_object`, `s3_get_object`, presigned GetObject.
- **Writes** go via `s3_create_multipart_upload` +
  `s3_get_presigned_upload_part_url` → direct PUT → `s3_complete_multipart_upload`.
  This is the documented large-object path; the 5 MiB-min / 5 GiB-max part
  rules are already in the tool contract.
- **Presigned URLs** additionally give us a browser/client-direct path
  (§7, freemium).
- Delete only for the probe/verification path; retention stays lifecycle.

Note the server currently runs `B2_DESTRUCTIVE_POLICY=block`, which refuses
`PutObject` presigned minting and `delete_bucket`. That must be relaxed (or
the writes done by a principal outside this server) before Stage 2.

---

## 6. Migration plan — legacy crypt archive

Two buckets hold overlapping history:

| Archive | Files | Window | MCP-readable | Freshest day |
|---|---|---|---|---|
| `rmi-chain-data/parquet` (plain) | 7,506 | 2026-06-25 → 10-02 | **yes** | 10-02 |
| `b2-crypt-v2:chain-data/parquet` (crypt) | 14,059 | (older) | no | **10-03 (today)** |

The **crypt** copy is the fresher and larger one. That inverts the naive
plan: the archive with the *live tail* is the unreadable one.

### Decision framing (operator's call — we do NOT execute)

**Option A — FREEZE the crypt archive (RECOMMENDED)**

- Stop writing to `b2-crypt-v2:` at cutover; keep the remote + the mount
  + `rclone cat` path alive for N days (suggest 30, matching the crypto
  rotation precedent).
- **Egress cost: $0.** Nothing is read out of B2 in bulk; you only read
  the days you still need, through the existing mount.
- The crypt copy already satisfies "the record exists" — the playbook's
  requirement is *durability*, and a crypt archive is durable.
- 14,059 files is small enough that "freeze" is cheap to reverse: if a
  historical THA query needs a crypt-era day, `rclone cat` it on demand.
- **Cost of the option:** `cold_store` keeps `mount`/`fetch` mode for that
  window, so the rclone dependency is not yet fully removable; and the
  pre-cutover tail (freshest data) lives in the crypt bucket, so the
  cutover must ensure the producer back-fills the gap day(s) into the
  plain bucket.

**Option B — MIGRATE (decrypt → re-upload)**

- `rclone copy b2-crypt-v2:chain-data/parquet rugmunch-b2:rmi-chain-lake-90d/chains`
  — rclone decrypts on the fly; no local staging disk needed.
- **Egress cost ≈ the crypt archive's stored size.** Backblaze egress is
  free for the first 3 GB/month per account, then billed (~$0.01/GB-class,
  verify against the current price sheet before committing). This archive
  is **tens of GB** (the comparable plain lake is 65 GB / 7.5k files, and
  the crypt one is ~2× the files) — so expect a **low-hundreds-of-dollars**
  class one-off, not a trivial one.
- Cost is *egress*, plus a full re-write of every object (Class B/C
  operations), plus the wall-clock of decrypting and re-uploading the
  whole history.
- **Only worth it if** you need MCP-native / presigned / no-rclone reads
  for the *historical* window, or you intend to hard-delete the crypt
  buckets to stop paying for a second copy. Note B2 bills stored bytes on
  *both* copies during the overlap, so a migration also pays double
  storage until the crypt copy is deleted — which is why this is an
  operator decision, not a default.

**Option C — leave `rmi-chain-data/parquet` as-is.** It is already plain,
MCP-readable, already at 90d, and already monitored. Do **not** delete it
and do **not** re-ship it: the sensible reading is that it is an
**unencrypted-but-public** lake (note `bucketType: allPublic`), and the
new canonical bucket is the **private** replacement. Its remaining role is
to keep serving until the new bucket is warm. Freeze + expire it naturally
at 90d once `rmi-chain-lake-90d` carries a full window.

**Recommendation: A + C.** Freeze crypt, let the plain public lake expire
by its own lifecycle rule, stand up the private canonical bucket. $0 egress,
no irreversible action, nothing deleted.

---

## 7. Freemium boundary hook — bulk Parquet export via x402

The new bucket being private + SSE-B2 is what makes a metered product
possible: raw object URLs must never be public (as `rmi-chain-data` is
today — `allPublic`). Instead:

1. **Buyer calls a paid endpoint** (x402 / pay-per-call; the `x402-gateway`
   + `rmi-mcp-x402` stack is the fleet mechanism).
2. On settlement, the server mints a **time-limited presigned GetObject**
   (`s3_get_presigned_url(operation="GetObject", expiresIn=…)`), scoped to
   a **specific chain + day + kind** key, never the bucket root.
3. Client downloads **directly from B2** — bytes never traverse the API
   host, so egress cost and bandwidth are B2's problem, not ours.

Design constraints this places on the layout (why §3 matters):

- A day-partitioned, per-chain keyspace means the grant can be **scoped to
  one prefix**; there is no way to hand out "the lake" as one unit.
- `_manifest.json` per chain-day is the natural **catalog/billing** record:
  rows, coverage, byte size → price the export before issuing the URL.
- Presigned URLs are bearer capabilities — keep `expiresIn` short (minutes,
  not days) and never log them. `s3_get_presigned_url` already redacts
  them from the MCP response.

**Two blockers to clear before this ships:**
- `B2_DESTRUCTIVE_POLICY=block` currently refuses `PutObject` minting; it
  must also permit `GetObject` minting for this to work at all.
- The `s3_*` tools need a **non-master application key** (§0).

**Access model:** dedicate a `rmi-chainlake` bucket-scoped application key
with `readFiles`+`listFiles` for the signing service only. Never hand the
MCP server a delete-capable key for this bucket; lifecycle owns deletion.

---

## 8. Operator decisions required

| # | Decision | Recommendation | Blocking? |
|---|---|---|---|
| D1 | Which bucket is canonical? | **`rmi-chain-lake-90d`** (exists, private, correct layout prefix, 90d rule) over the new `rmi-chainlake` | yes |
| D2 | Legacy crypt archive | **Freeze** (Option A), keep mount 30d, $0 egress | yes |
| D3 | `b2-offload` 401 since ≥2026-09-30 | **Separate incident.** Backslaze-side key/authorization for `b2-crypt:`/`rugmunch-b2:`. Not the lake ship path, but offsite backups + `/srv/models` are NOT landing | no (for this design) |
| D4 | Fix the MCP credential | Provision a **non-master** application key → `B2_APPLICATION_KEY_ID`/`B2_APPLICATION_KEY`; sets `B2_DESTRUCTIVE_POLICY` deliberately | yes, for MCP-canonical |
| D5 | Keep `rmi-chain-lake-90d` allPrivate? | yes — required for the x402 hook; retire `rmi-chain-data` (allPublic) as it expires | no |
| D6 | `httpfs` vs presigned for internal reads | `httpfs` server-side (DuckDB-native, no MCP round-trip); presigned only for the paid/freemium edge | no |

## 9. Explicitly NOT done (per task constraints)

- No data migrated; nothing deleted; no crypt bucket or remote touched.
- `b2-offload` left enabled and failing (see D3 — needs its own ticket).
- No git push. Doc written to the working tree only.
- `rmi-chainlake` left **empty**; probe object created, read back, deleted
  (verified 0 objects remaining).
