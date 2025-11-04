Here’s a tight, no-BS `README.md` you can drop in. It reads like a builder wrote it, not a marketer.

---

# Ticker Enrichment

End-to-end tool to **enrich portfolio holdings** by filling missing ticker symbols from names. Upload a CSV/XLSX, get a **preview with candidates**, set overrides, then **download an enriched CSV**. Built fast, shipped cleaner.

**Live**

* UI: [https://ticker-ui.onrender.com](https://ticker-ui.onrender.com)
* API: [https://ticker-api-ex8c.onrender.com](https://ticker-api-ex8c.onrender.com)

## What this does (in practice)

* Accepts a holdings file with columns like `Name`, `Symbol`, `Price`, `# of Shares`, `Market Value`
* For rows missing `Symbol`, queries market data APIs (Finnhub / Polygon) to find the likely ticker
* Ranks candidates with a simple score + source trust boost
* Let's me **bulk-apply** top candidates with a minimum score threshold
* Learns **aliases** (my chosen overrides) to bias future runs
* Ships an **audit trail** (activity log + commit preview with risk) before writing the CSV

## Why it’s not fluff

* **API-first** fallbacks with optional **local maps** (ETF/alias canon)
* Explicit **risk model**: LOW/MED/HIGH based on score + source
* **Keyboard** flows: faster than mousing through tables
* **Deterministic output** for the same input + override set (I can defend diffs)

---

## Stack

* **Frontend**: React + Vite + TypeScript + Chakra UI
* **Backend**: FastAPI (Python 3.11), Uvicorn
* **Deploy**: Render (Blueprint)
* **APIs**: Finnhub, Polygon
* **Package managers**: **pnpm** (UI), pip (API)

---

## Quickstart (Local)

### 1) Backend

```bash
cd backend
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Create `.env` (or set env vars in your shell):

```
FINNHUB_API_KEY=...
POLYGON_API_KEY=...
ALLOW_ALL_CORS=1
RESOLVER_VERSION=2025.11.03
HTTP_TIMEOUT=4.0
HTTP_QPS=0.8
```

Run:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 2) Frontend

```bash
cd frontend
corepack enable
pnpm install
echo VITE_API_BASE=http://localhost:8000 > .env.local
pnpm dev
```

Open: [http://localhost:5173](http://localhost:5173)

---

## Blueprint (Render)

`render.yaml` (already in repo):

```yaml
services:
  - type: web
    name: ticker-api
    env: python
    plan: free
    rootDir: backend
    buildCommand: pip install -r requirements.txt
    startCommand: uvicorn app.main:app --host 0.0.0.0 --port $PORT
    autoDeploy: true
    envVars:
      - key: PYTHON_VERSION
        value: "3.11.9"
      - key: FINNHUB_API_KEY
        sync: false
      - key: POLYGON_API_KEY
        sync: false
      - key: ALLOW_ALL_CORS
        value: "1"
      - key: RESOLVER_VERSION
        value: "2025.11.03"
      - key: HTTP_TIMEOUT
        value: "4.0"
      - key: HTTP_QPS
        value: "0.8"

  - type: web
    name: ticker-ui
    env: static
    rootDir: frontend
    buildCommand: |
      corepack enable
      pnpm install --frozen-lockfile
      pnpm run build
    staticPublishPath: dist
    envVars:
      - key: VITE_API_BASE
        value: https://ticker-api-ex8c.onrender.com
```

**Node pinned** via `frontend/package.json`:

```json
"engines": { "node": "20.x", "pnpm": "9.x" },
"packageManager": "pnpm@9.11.0"
```

---

## File format

* Accepts `.csv`, `.xlsx`, `.xls`
* **Required column**: `Name`
* **Optional**: `Symbol`, `Price`, `# of Shares`, `Market Value`
* Output preserves all columns and writes/updates `Symbol` where we have a decision.

---

## Frontend features

* Status chips: **FILLED**, **AMBIGUOUS**, **NOT_FOUND**, **UNCHANGED**, **ENRICHED**
* Search: name / symbol / status / top candidate
* **Bulk Apply** top candidates with a min score slider
* **Row Drawer** to review candidates and set override
* **Commit Preview** (sorted by risk) before writing CSV
* **Activity Log** with copy/download
* **Insights**: coverage, avg candidates/row, top sources, status counts
* **Shortcuts**:

  * `⌘/Ctrl + K` focus search
  * `⌘/Ctrl + Enter` commit preview
  * `⌘/Ctrl + L` toggle Local Maps
  * `A` bulk-apply top candidates
  * `R` reset session
  * `?` shortcuts modal

---

## API (minimal)

### Preview a file

`POST /files/preview-file`
**FormData**

* `file`: CSV/XLSX
* `use_local_maps`: `"true" | "false"`

**200 Response** (array of rows, trimmed):

```json
[
  {
    "index": 0,
    "status": "AMBIGUOUS",
    "input": { "Name": "Apple Inc.", "Symbol": null },
    "candidates": [
      { "symbol": "AAPL", "name": "Apple Inc", "score": 0.93, "source": "FINNHUB" },
      { "symbol": "APLE", "name": "Apple Hospitality REIT", "score": 0.41, "source": "POLYGON" }
    ]
  }
]
```

### Commit (write CSV)

`POST /files/commit-file`
**FormData**

* `file`: original file
* `use_local_maps`: `"true" | "false"`
* `overrides_json`: JSON blob mapping `{ [rowIndex]: "SYMBOL" }`

**200 Response**

* `Content-Disposition: attachment; filename="enriched_holdings.csv"`

---

## Risk model (simple but explicit)

* Base on `score` (0–1)
* Source trust bonus for `FINNHUB`, `POLYGON`, `YFINANCE`, `LOCAL` (+0.05 capped)
* **LOW**: ≥ 0.85
* **MEDIUM**: ≥ 0.60 and < 0.85
* **HIGH**: < 0.60 or missing score

This keeps me honest when bulk-applying.

---

## Learned aliases (self-tuning)

* When I override a row, the app stores `{ normalizedName → chosenSymbol }`
* On future previews, if a candidate matches the learned symbol, it’s moved to the top with a tiny bump (+0.05)
* I can clear an override per row; alias remains until I overwrite it

---

## Troubleshooting (actual fixes, not vibes)

* **Static build failed on Render (npm + Node 22)**
  Use **Node 20** and **pnpm**. It’s already pinned. Build uses:

  ```bash
  corepack enable && pnpm install --frozen-lockfile && pnpm run build
  ```
* **TS6133 “declared but never read”**
  TypeScript is strict. Remove unused vars/params or turn off `noUnusedLocals/noUnusedParameters` if you like footguns (I don’t).
* **CORS**
  `ALLOW_ALL_CORS=1` is set for demo. Lock it down for prod.
* **405 on HEAD /**
  Ignore. Render health checks do this. The app serves `GET /` fine.

---

## Demo checklist (what I’ll show)

* **Input**: small CSV with 10–20 rows (mix of clear wins, ambiguous names, and missing symbols)
* **Flow**:

  1. Upload → show **status breakdown** + **coverage**
  2. Open a few rows → **drawer** → override one to show **alias learning**
  3. **Bulk apply** with min score 0.85 → show counts
  4. **Commit preview** → scan **HIGH** risk changes
  5. **Confirm** → download `enriched_holdings.csv`
  6. Re-upload **same file** to show determinism + learned alias effect
* **Metrics** to call out:

  * Coverage %
  * Avg candidates/row
  * Time to preview / commit
  * # of overrides (manual) vs auto
* **Artifacts**:

  * Activity log (download)
  * Final CSV (upload to Sheets quickly to visualize diffs)

---

## Roadmap (near-term)

* Add **source-level policy** (e.g., prefer US common stock, exclude REITs/ETFs unless requested)
* Lightweight **caching** by normalized name (TTL)
* **Pinned overrides file** (repo-level JSON) for teams
* Per-row **notes** field → write to CSV as `Notes`
* Optional **confidence column** write-back
* Quick **/healthz** + request id for API logs

---

## Notes

This is a focused coding exercise turned into a usable tool. It’s intentionally opinionated: speed, clarity, auditability > bells and whistles.

---

## License

MIT (for now). If I upstream API keys or private maps later, I’ll scope them out or move to env-only.

---

If you want me to wire a slick public demo (gif/video + sample dataset + talk track), I’ve already laid the checklist.
