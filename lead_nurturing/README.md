# Lead Nurturing: AI-driven lead nurturing for two-wheeler sales

An end-to-end lead nurturing solution for an Indian two-wheeler OEM, built from the PRD
(*PRD: AI-Driven Lead Nurturing System for Two-Wheeler Sales (India)*) on the Databricks Data Intelligence Platform:

- **Bronze / silver:** leads from every channel plus our contact attempts (dialler, WhatsApp / SMS receipts) are
  ingested, cleansed, de-duplicated and consolidated into one lead per person per purchase journey.
- **Gold:** each lead gets AI enrichment with Databricks AI Functions (`ai_classify`, `ai_query`; no ML models): lead
  types, purchase intent, main barrier and the next best action (NBA). Gold also builds the contact history, an
  activity timeline, the work queue and KPI tables.
- **Serving:** the gold tables the app needs are synced to **Lakebase** (Postgres).
- **Consumption:** the **Lead Nurture Hub** app (dashboards, pipeline tracking, journeys, follow-up queues, lead 360)
  reads from Lakebase, and **Genie** answers plain-language questions on the gold tables.

All data logic is in Databricks notebooks, run in order as tasks of one job on serverless compute and deployed with a
Databricks Asset Bundle. **The source data is simulated** (`01_generate_source_data.py`): numbers quoted below come from
that pilot dataset (3 months, ~21.5k source records, 12,219 leads).

| Item | Value |
|---|---|
| Catalog.schema | `<your_catalog>.lead_nurturing` |
| Volume | `landing`: raw files, one folder per lead channel, plus `contact_attempts/` and `reference/` |
| Job | `lead_nurturing_bronze_silver`: 01 → 02 → … → 08, then 09 (Genie) and 10 (Lakebase sync) in parallel; daily 06:30 IST schedule (paused) |
| App | **Lead Nurture Hub**: https://<your-app-name>-<workspace-id>.<cloud>.databricksapps.com (reads from Lakebase) |
| Lakebase | Autoscaling project `lead-nurturing`, branch `production`, endpoint `primary`, database `databricks_postgres` (schemas `lead_nurturing`, `lead_nurturing_app`) |
| Genie agent | **Lead Nurturing Insights**: https://<your-workspace>.cloud.databricks.com/genie/rooms/<your-genie-space-id> |
| Docs | [PRD](<prd-document-url>) · [Business deck with speaker notes](<business-deck-url>) |
| Workspace code | `/Workspace/Users/<you>/lead_nurturing` |

## Architecture

```
 SOURCES (landing volume)            BRONZE (full reload)            SILVER (conformed)                       GOLD
 ─────────────────────────           ────────────────────            ──────────────────                       ────
 website/  paid_digital/  ─┐                                        silver_lead_records (1 row / record)
 whatsapp/ auto_portal/    │  RF     bronze_<channel>_leads   ─┐     ├─▶ silver_lead_records_quarantine
 call_centre/ dealer_dms/  ├──────▶  (8 tables)                ├──▶  ├─▶ silver_lead_record_xref (record → lead)
 events_btl/ finance_partner/                                  │     └─▶ silver_leads (1 row / lead) ─────┐
 contact_attempts/ (dialler + WA/SMS receipts) ─▶ bronze_contact_attempts ─▶ silver_contact_attempts ──────┤
 reference/ (geography, dealers, models)       ─▶ bronze_ref_*            ─▶ silver_dim_*                 │
                                                                                                          ▼
   gold_lead_360 · gold_nba_action_queue · gold_lead_activity · gold_lead_ai_enrichment · gold_nba_decision_log
   gold_offer_catalog · gold_kpi_* · gold_lead_metrics (metric view)
        │  ai_classify / ai_query (Foundation Model APIs)                    │
        ├──▶ Lakebase synced tables (*_sync) ──▶ Lead Nurture Hub app ◀──▶ lead_nurturing_app.action_outcomes
        └──▶ Genie space "Lead Nurturing Insights" ◀── Ask Genie in the app (Genie One MCP via Unity AI Gateway, on behalf of the user)
```

RF = `read_files` (Auto Loader's schema inference as a batch read), with type inference off (all strings) and
`_rescued_data` for schema drift.

**Every run is a full refresh:** bronze reloads all landing files, and every silver and gold table is rebuilt from the
layer below (no checkpoints or incremental state). The only history kept is `gold_nba_decision_log`, one batch of
decisions per run. Governance is through Unity Catalog (permissions, lineage, column descriptions; mobile and email are
not sent to the AI and are hidden from Genie) and Unity AI Gateway (model and Genie MCP access).

## Repository layout

```
lead_nurturing/
├── databricks.yml                       # bundle: variables (catalog, schema, warehouse_id, notify_email) + dev target
├── resources/
│   ├── lead_nurturing_job.yml           # the job: one task per notebook, parameters, schedule (paused)
│   └── lead_nurturing_app.yml           # the app: SQL warehouse (Genie) + Lakebase (postgres) resources, user API scopes
├── notebooks/
│   ├── 00_common.py                     # config, shared cleansing functions, Lakebase helper (%run by all)
│   ├── 01_generate_source_data.py       # simulated lead feeds + our contact attempts → landing volume
│   ├── 02_bronze_ingestion.py           # read_files (full reload) → bronze_<channel>_leads, bronze_contact_attempts, bronze_ref_*
│   ├── 03_silver_reference.py           # silver_dim_* (geography, dealers, coverage, models, aliases)
│   ├── 04_silver_lead_records.py        # per-channel mapping → canonical schema, cleansing, enrichment, DQ flags
│   ├── 05_silver_leads.py               # identity resolution, de-dup, consolidation, xref, quarantine
│   ├── 06_silver_contact_attempts.py    # call attempts + WhatsApp / SMS receipts, one row per attempt, linked to its lead
│   ├── 07_data_quality_checks.py        # bronze ↔ silver reconciliation (leads, contact attempts); fails the job on breaks
│   ├── 08_gold_lead_intelligence.py     # contact history, AI enrichment + next best action, activity timeline, queue, KPIs
│   ├── 09_genie_agent.py                # column descriptions, gold_lead_metrics metric view, Genie agent as code
│   └── 10_lakebase_sync.py              # Lakebase synced tables: create if missing, refresh, app access
├── app/                                 # Lead Nurture Hub (Databricks App)
│   ├── app.py                           # stdlib HTTP server: page APIs, outcome write-back, Genie One MCP proxy
│   ├── db.py                            # Lakebase connection pool (psycopg) with short-lived OAuth credentials
│   ├── queries.py                       # Postgres SQL for every page
│   ├── app.yaml, requirements.txt       # command + env; psycopg[binary], psycopg-pool
│   └── static/                          # index.html, app.js (vanilla JS + ECharts), app.css, genie-host.js (MCP App View host)
├── scripts/bundle.sh                    # bundle wrapper (exports an OAuth token from a CLI profile)
└── README.md
```

Each notebook also runs on its own (interactive): set the `catalog` / `schema` widgets and run all.

## Deploy and run

```bash
./scripts/bundle.sh validate
./scripts/bundle.sh deploy                       # job, app definition and workspace files

# first load: 3 months of history (Aug–Oct 2026), lead feeds and contact attempts
./scripts/bundle.sh run lead_nurturing_job --params mode=backfill

# daily run (what the schedule runs): adds the day's simulated feed, then fully rebuilds every layer and Lakebase
./scripts/bundle.sh run lead_nurturing_job

# deploy / restart the app after code changes
./scripts/bundle.sh run lead_nurture_hub
```

`scripts/bundle.sh` uses CLI profile `<your-cli-profile>` by default (`DATABRICKS_PROFILE=<other> ./scripts/bundle.sh …`
to change it).

Prerequisites: the schema and the Lakebase project / branch / database exist. The notebooks create the volume, tables,
Genie space and Lakebase synced tables; the bundle creates the job and the app. The app's Postgres role is created by
its `lakebase` resource; `10_lakebase_sync.py` grants it access.

| Job parameter | Default | Meaning |
|---|---|---|
| `mode` | `daily` | `backfill` writes the whole history (existing files are never rewritten); `daily` writes `run_date` |
| `run_date` | today (IST) | day to simulate in daily mode |
| `lead_episode_days` | `30` | inactivity gap that starts a new lead (purchase journey) |
| `ai_active_days` | `30` | AI scope: open leads with activity in the last N days (see Gold) |
| `nba_model` | `databricks-claude-sonnet-5` | model for the next best action; must support structured output |

**Run time** (full run): about 5 minutes for bronze → data quality, **~70–80 minutes for the gold AI step** (~5.6k leads
in AI scope, about 1 lead per second), ~3 minutes for Genie and ~1.5 minutes for the Lakebase sync. Narrow
`ai_active_days` to make it faster.

**Run the app locally** (reads the same Lakebase as your CLI user): install `psycopg[binary]` and `psycopg-pool`, then
`cd app && LOCAL_PROFILE=<your-cli-profile> PORT=8765 python app.py`.

## Silver rules in brief

**Cleansing (every channel, same rules)**: `notebooks/00_common.py`
- Mobile: strip `+91` / `91` / leading `0` / spaces / dashes → 10 digits; valid if `^[6-9]\d{9}$` and not all one digit.
- Name: trim, collapse spaces, title case; test/junk names (`test`, `asdf`, `dummy`, …) flagged.
- Email lower-cased and validated; pincode must be 6 digits and known.
- Geography resolved by pincode → city name → district name → capturing dealer's location (`geo_resolution` records which).
- Model free text (`BREEZE-125 Disc`, `breeze125`, `BRZ125`) normalised and mapped to the catalog via aliases.
- Language labels (`Hindi`, `HI`, `hi-IN`, `hin`) → ISO codes; purchase timeline, gender, occupation standardised.
- All timestamps stored in UTC; IST wall-clock sources converted.

**Validity**: a record is quarantined if it has a missing/invalid mobile, is a test/junk lead, has an unparseable or
future timestamp, a *Wrong Number* call disposition, or no consent to share (finance partner). Non-blocking issues
(unresolved geography/model, invalid email, missing name, schema drift) are kept in `dq_issues`.

**De-duplication and consolidation**: `notebooks/05_silver_leads.py`
1. Re-delivered records collapse to one (`REDELIVERED`): same channel, source id **and content** (a hash of the key
   fields), because source ids are not always unique (e.g. a dealer reusing an enquiry serial).
2. Identity = normalised mobile.
3. A lead is a purchase journey: a gap of more than `lead_episode_days` (30) starts a new lead (`is_repeat_lead`,
   `previous_lead_id`). `lead_id` = hash of mobile + journey start.
4. Same person + same channel within 30 min = `DUPLICATE_SUBMISSION` (not counted as a new enquiry).
5. Survivorship: identity, geography and demographics come from the most trusted channel
   (dealer DMS > finance partner > website > call centre > WhatsApp > portal > paid digital > events);
   intent (model, timeline, language, consent) from the most recent non-null value.
6. Adds first/last-touch attribution, channel/campaign lists, journey signals (finance, exchange, test ride,
   booking), a rule-based lead stage, rule-based lead-type tags (PRD §7.1) and the assigned dealer
   (dealer that captured the lead, else pincode coverage).

## Gold layer: AI lead intelligence (`08_gold_lead_intelligence.py`)

| Step | How |
|---|---|
| Lead context | Plain-text profile + up to 12 most recent touches (channel, type, model, disposition, notes) + our outreach (call attempts, connects, unanswered calls, messages, last disposition, last logged outcome, 6 most recent attempts). Mobile and email are never included. |
| Lead types | `ai_classify` **v2.1**, multi-label, labels with descriptions (PRD §7.1), confidence + rationale |
| Purchase intent | `ai_classify` v2.1: `hot` / `warm` / `cold` / `not_interested`, confidence + rationale |
| Main barrier | `ai_classify` v2.1: price, finance/EMI, exchange value, competitors, EV range, availability, family, postponed, unreachable, none |
| Next best action | `ai_query` (`nba_model`) with a strict JSON-schema `responseFormat`: action (14-action catalog), channel, owner, timing, approved offer, message in the customer's language, DSE talking points, one-line summary, priority 1–100, reasoning, risk flags |
| Guardrails | Not interested (incl. a *not interested* call disposition or logged outcome) → `PAUSE_CONTACT`; 4+ unanswered calls since the customer was last reached → message instead of calling; no WhatsApp consent → voice call; the model only sees offers the lead is eligible for (fuel, tier, model, dates, rules), and if it still picks another one the offer **and the drafted message / talking points are withheld**; voice calls and visits only due 09:00–21:00 IST; rule-based fallback if the AI call fails |

AI runs only for **actionable leads**: open, not marked not interested and active in the last `ai_active_days` days,
with 4 LLM calls per lead. Booked, not-interested and inactive leads get a rule-based action (delivery update, pause, or
a light-touch re-engagement nudge by the bot); `decision_source` = `AI` or `RULE` says which. `nba_model` must support
structured output: the Claude 5.5 endpoints rejected `responseFormat` when this was built, so the default is
`databricks-claude-sonnet-5`. The prompt version (`prompt_version`, currently `v3`) is logged with every decision.

| Gold table | Grain / purpose |
|---|---|
| `gold_offer_catalog` | Approved schemes the NBA may recommend |
| `gold_nba_decision_log` | Audit of every classification + recommendation, one batch per run (model, prompt version, guardrails, errors) |
| `gold_lead_ai_enrichment` | Enrichment and NBA per lead from the latest run |
| `gold_lead_360` | One row per lead: silver profile + contact history (call attempts, connects, messages, last contact, logged outcomes) + AI fields + NBA + first response, SLA met (15 min), open / leaked, ageing |
| `gold_lead_activity` | One row per event per lead, in order: enquiries (duplicates tagged), contact attempts, NBA decisions (when they change between runs), outcomes logged in the app |
| `gold_nba_action_queue` | Open action per lead with due time (calling window applied), owner queue (`CALL_CENTRE`, `BOT`, `DEALER_DSE:<dealer>`, …) and rank |
| `gold_kpi_funnel_daily` | Funnel counts by enquiry date × region × state × tier × channel × lead type × intent × segment |
| `gold_kpi_channel_campaign` | First-touch channel / campaign: funnel, test-ride, booking and hot-lead rates |
| `gold_kpi_dealer_performance` | Dealer scorecard: SLA adherence, median first response, leakage, bookings, pending dealer actions |
| `gold_kpi_ai_lead_mix` | Region × tier × lead type × intent × barrier × recommended action / channel / owner |

## Contact history and lead activity

| Layer | Table | Content |
|---|---|---|
| Bronze | `bronze_contact_attempts` | outbound call attempts (dialler: outcome, disposition, duration) and WhatsApp / SMS delivery receipts; structured, no message text |
| Silver | `silver_contact_attempts` | one row per attempt, UTC time, normalised mobile, `customer_reached` (connected call / WhatsApp reply), `lead_id` (same mobile, inside the lead's journey window) |
| Gold | `gold_lead_360` columns | `call_attempts`, `connected_calls`, `unanswered_calls_since_reached`, `messages_sent / read / replied`, `first_call_attempt_at`, `first_reached_at`, `last_contact_at / channel / outcome`, `last_call_disposition`, `outcomes_logged`, `last_logged_outcome` |
| Gold | `gold_lead_activity` | the full ordered timeline of a lead |

How it is used:
- **Stage and SLA:** a connected call or WhatsApp reply moves a `NEW` lead to `CONTACTED`; a test ride booked on a call → `TEST_RIDE_REQUESTED`. First response (SLA, leakage) = the first call attempt or human touch. Reaching the customer counts as activity.
- **NBA:** the outreach summary and recent attempts are in the lead context; *not interested* dispositions / outcomes pause the lead; unanswered-call cap guardrail.
- **App:** contact columns in Leads and watchlists, contact summary and action status on Follow-ups, contact history and the activity timeline in the lead 360 drawer.

The simulator only emits attempts that already happened by the time it runs (a dialler export never holds future
retries), and dates its date-only feeds (events 12:00 IST, finance partners 10:00 IST) the way silver does.

## Genie agent: conversational analytics (`09_genie_agent.py`)

The **Lead Nurturing Insights** agent is defined as code and created or updated in place (looked up by title) on every
run, after the gold tables are rebuilt. Best practices it follows:

| Practice | Implementation |
|---|---|
| Few, curated data sources | `gold_lead_metrics` (metric view), `gold_lead_360`, `gold_nba_action_queue`, `gold_kpi_dealer_performance`, `gold_kpi_channel_campaign`, `gold_lead_activity` |
| Unity Catalog metadata first | Table comments + a description for every business column (re-applied after each full refresh) |
| KPIs in SQL, not prose | Metric view `gold_lead_metrics` (YAML 1.1): 23 dimensions and 21 measures (booking rate, SLA adherence, leakage, median first response, call connect rate, reach rate, …) with display names and synonyms |
| Hide confusing / sensitive columns | Technical, duplicate and PII columns hidden (mobile, email, hashes, raw AI output, error columns, rule-based tags, source ids) |
| Entity matching + synonyms | On categorical columns users type (state, city, model, channel, campaign, intent, lead type, barrier, action, dealer, event type) |
| Join specs | action queue ↔ lead 360 (1:1), lead 360 → dealer scorecard (N:1), lead activity → lead 360 (N:1) |
| SQL snippets | filters (open, hot, festive season, EV), expressions (enquiry week / month), measures (booking rate, SLA, hot share) |
| One short text instruction | Which table for which question, business definitions, value formats, clarification rules and a summary section |
| Trusted example SQL | 12 verified queries for multi-step / ambiguous questions (funnel, festive trend, dealer SLA, work queues, multi-label lead types, explain-a-lead, a lead's event sequence, unreached hot leads) |
| Benchmarks | 14 questions with SQL answers, phrased differently from the examples; run them from the agent's Benchmarks tab after changes |
| Validated before saving | Every example and benchmark SQL is executed; the notebook fails if any breaks |

Sample questions: *"What is the booking rate by city tier?"*, *"Which dealers have the most open hot leads?"*,
*"What is the call connect rate by source channel?"*, *"Show the full sequence of events for lead LD…"*,
*"Why was lead LD… recommended a qualification call?"*.

## Serving layer: Lakebase (`10_lakebase_sync.py`)

The app does not query the lakehouse: it reads Postgres copies of the gold tables in Lakebase and writes outcomes there.
Only Ask Genie uses the SQL warehouse.

| Postgres table | Source / purpose | Primary key |
|---|---|---|
| `lead_nurturing.gold_lead_360_sync` | `gold_lead_360` | `lead_id` |
| `lead_nurturing.gold_nba_action_queue_sync` | `gold_nba_action_queue` | `lead_id` |
| `lead_nurturing.gold_lead_activity_sync` | `gold_lead_activity` | `lead_id, event_no` |
| `lead_nurturing.gold_kpi_dealer_performance_sync` | `gold_kpi_dealer_performance` (dealer names) | `assigned_dealer_code` |
| `lead_nurturing.gold_offer_catalog_sync` | `gold_offer_catalog` | `offer_code` |
| `lead_nurturing_app.action_outcomes` | native table: outcomes logged in the app; `08_gold_lead_intelligence` reads it back | `outcome_id` |

- Synced tables use **SNAPSHOT** mode (a full copy per refresh, schema changes included), which fits the full-refresh
  gold tables. The `lakebase_sync` task creates any missing synced table, refreshes all of them in parallel after the
  gold step, and re-grants the app's role (`USAGE` + `SELECT` on `lead_nurturing`, `SELECT, INSERT` on the outcomes table).
- The app connects as its service principal through the `lakebase` app resource (`postgres`, `CAN_CONNECT_AND_CREATE`),
  with a short-lived OAuth credential per connection (`app/db.py`, psycopg pool).
- The notebooks reach Lakebase with **pg8000** (pure Python) through `lakebase_run()` in `00_common.py`, using an
  OAuth credential for the identity running the job. psycopg's binary libpq can abort the notebook's Python process
  when it loads next to libraries already in memory.
- The Lakebase database is not registered as a Unity Catalog catalog (that needs CREATE CATALOG on the metastore), which
  is why the gold notebook reads the outcomes table directly.

## App: Lead Nurture Hub

| Group | Page | What it shows |
|---|---|---|
| Dashboards | Overview | Summary banner, 8 KPIs with sparklines and period deltas, daily trend, funnel, *needs attention* counters, source mix, purchase intent, regional scorecard, top next steps |
| Dashboards | Lead pipeline | Open-lead KPIs (reach, connect rate, 15-min SLA, attempts to reach, stalled, leaked), stage board, ageing heatmap, weekly outreach, call outcomes, attempts until reached, watchlists (hot not reached, 3+ unanswered calls, no contact 7+ days, leaked), coverage by source |
| Dashboards | Journeys | Source → stage flow, booking rate by number of touches, funnel by any dimension, speed to lead by channel, weekly enquiries vs outreach, weekly cohorts |
| Work | Leads | Lead search; click a row for the lead 360 drawer (next step, buyer profile, contact history, activity timeline, profile, outcome logging) |
| Work | Follow-ups | Team work queues (call centre, dealer DSEs / managers, fleet) with live action status `PENDING` → `DONE` / `RETRY` / `CLOSED`; the bot's automated nudges are a separate card |
| Reports | Dealers, Channels & campaigns, Geography, Buyer insights | Scorecards and breakdowns |

- **Needs attention:** clicking a counter (Overview) or the Stalled / Leaked tiles (Lead pipeline) opens the list of
  those leads in a side panel; click a lead for its 360 view.
- **Next-step details:** hover any next step (marked ⓘ) in Leads, watchlists or Follow-ups for channel, owner, timing,
  due time, offer, priority, the drafted message (with copy), talking points, reasoning and the rules applied.
- Global filters (period, region, tier, channel, segment) apply to every dashboard and report.
- **Ask Genie:** Genie One MCP through Unity AI Gateway, on behalf of the signed-in user (`user_api_scopes`: `sql`,
  `ai-gateway`, `dashboards.genie`); answers render in Genie's interactive MCP App View, with a text fallback.

## Next steps (per PRD)
- Swap the simulator for real connectors (website / WhatsApp webhooks, Meta / Google lead APIs, portal feeds, CRM / DMS
  extracts, dialler and WhatsApp BSP logs).
- Write outcomes and actions back to the CRM / DMS, and send WhatsApp / SMS through the BSP to close the loop.
- Unity Catalog column masks on `mobile` / `email` and row filters by region / dealer (PRD FR-7.5).
- Share the app and the Genie agent with business user groups (`CAN_USE` / `CAN_RUN`, plus `SELECT` on the gold tables and metric view).
- Configure Unity AI Gateway limits and guardrails for the model endpoints; enable the daily schedule.

