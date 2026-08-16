# BNPL Governance Workshop

A hands-on workshop built around a fictional Buy-Now-Pay-Later (BNPL) use case, demonstrating
Confluent Cloud's data governance and stream processing capabilities: Schema Registry, Data
Contracts, Client-Side Field Level Encryption (CSFLE), Flink SQL, and Tableflow.

Run the notebooks in order -- each one picks up where the previous one left off, and all four
share a single `.env` file for credentials.

## Prerequisites

- Python 3.10+, with VS Code or Cursor (Jupyter extension enabled).
- A Confluent Cloud account with an environment, Kafka cluster, Schema Registry, and (for Part 2)
  a Flink compute pool already provisioned.
- The `.venv/` virtual environment in this folder (create it with `python3 -m venv .venv` if it
  doesn't exist).

## Setup

1. Copy the credentials template and fill in your values:

   ```bash
   cp .env.example .env
   ```

2. Open **`kredivo-00-setup.ipynb`** and run it top to bottom. It checks your Python/venv setup,
   installs dependencies, validates `.env`, and (optionally) checks the Confluent CLI, then runs a
   live connectivity smoke test against your cluster.

## Notebooks

### `kredivo-00-setup.ipynb` -- Part 0: Environment Setup

Run this **first**. It doesn't touch Confluent Cloud resources itself (aside from a read-only
connectivity check) -- it just verifies everything the other three notebooks assume is already in
place:

- Python 3.10+ and a virtual environment.
- Required packages installed (`confluent-kafka`, `python-dotenv`, `jsonschema`, `pandas`,
  `requests`, `cel-python`).
- A valid `.env` file, reporting exactly which variables are missing and which notebook needs them.
- (Optional, for Part 2/3 CLI-driven steps) whether the Confluent CLI is installed and logged in.
- A live smoke test: connects to Schema Registry and lists subjects, then connects to the Kafka
  cluster's admin API.

Every check prints ✅ / ⚠️ / ❌ so you can see at a glance what still needs fixing before moving on.

### `kredivo-01.ipynb` -- Part 1: Schema Registry, Data Contracts & CSFLE

Demonstrates Confluent Cloud Schema Registry's governance features on a `bnpl_transactions_<you>`
topic (topic name namespaced by your `PARTICIPANT_ID` or OS username):

1. **Schema creation & evolution** -- registers a JSON Schema V1 for a `Transaction` record, then
   evolves it to V2 by adding an optional `merchant` field.
2. **Schema validation** -- produces a record missing the required `amount` field (rejected before
   it reaches Kafka) and a valid record (delivered successfully), proving the schema is actively
   enforced.
3. **Data Contracts (CEL rules)** -- attaches a CEL rule (`amount > 0 && amount < 10000`) to the
   schema. A negative `amount` is still valid JSON but now fails the *contract*, showing that
   Data Contracts enforce business rules schemas alone can't express.
4. **Client-Side Field Level Encryption (CSFLE)** -- tags a `national_id` field as `PII`, attaches
   an `ENCRYPT` rule backed by a local KMS driver, and produces a record to a second topic
   (`..._secure`). The plaintext is encrypted client-side before it ever leaves the process.
5. **Consuming with and without decryption** -- reads the encrypted topic with a plain consumer
   (sees ciphertext) and then with a correctly configured deserializer (sees the decrypted
   plaintext), showing CSFLE is enforced transparently through the rule registry.

Ends with a **Cleanup** cell (`CLEANUP = True`) that deletes both topics and their schema subjects.

### `kredivo-02.ipynb` -- Part 2: Flink Stream Processing

Picks up from Part 1 and uses **Confluent Cloud Flink SQL** to process a stream of orders, all
namespaced under `orders_..._<you>` / `product_catalog_<you>` topics:

1. **Produce with duplicates** -- generates 200 unique orders and deliberately resends a random
   subset 1-2 extra times (simulating at-least-once delivery retries), keeping the exact ground
   truth duplicate percentage for later comparison.
2. **Duplicate-rate monitoring** -- a Flink SQL streaming query grouped into 2-minute tumbling
   windows continuously calculates what percentage of incoming records are duplicates, writing the
   result to its own sink topic.
3. **Deduplication** -- the classic `ROW_NUMBER() OVER (PARTITION BY order_id ...)` pattern keeps
   only the first occurrence of each order, dropping every resend.
4. **Filter + trim** -- keeps only confirmed, high-value orders (`amount > 500`) and drops
   internal-only fields (like `channel`) that downstream analytics consumers don't need.
5. **Enrichment** -- joins the order stream against a small static product catalog (`INNER JOIN`)
   to add product name, category, and unit price. Explains why `INNER JOIN` (not `LEFT JOIN`) is
   required for an append-only sink, and how a temporal join would work instead for a reference
   table that changes over time.
6. **Read-back** -- peeks at the deduped, filtered, and enriched output topics as plain consumers.

If the Confluent CLI isn't installed/logged in or the Flink REST credentials aren't set, every
Flink cell prints the SQL/command it would have run instead of failing. Ends with a **Cleanup**
cell that tears down all six topics, their schema subjects, and any Flink statements tagged with
your participant namespace.

### `kredivo-03.ipynb` -- Part 3: Tableflow

Picks up from Part 2's `orders_enriched_<you>` topic and uses **Tableflow** to turn it into a
first-class **Apache Iceberg table**, queryable by any Iceberg-aware engine with zero Kafka client
involved:

1. **Enable Tableflow** on `orders_enriched_<you>` using Confluent-managed storage and the Iceberg
   table format, via `confluent tableflow topic enable`.
2. **Wait for materialization** -- polls `confluent tableflow topic describe` until the first sync
   (topic history → Parquet + Iceberg metadata) completes.
3. **Query with `pyiceberg`** -- loads the table directly through Confluent's Iceberg REST Catalog
   using a Tableflow-scoped API key, no Kafka client involved.
4. **Query with DuckDB** -- the same check a data analyst on their own laptop would run, attaching
   DuckDB's `iceberg` extension to the same catalog endpoint.
5. **Reference snippets** for **Snowflake**, **Amazon Athena/Trino**, and **Apache Spark** (not
   executed live in the workshop, since those accounts aren't available here) -- showing the same
   Iceberg table is reachable from any of them via the Iceberg REST Catalog protocol.

Requires a separate `TABLEFLOW_API_KEY` / `TABLEFLOW_API_SECRET` (distinct from the Kafka, Schema
Registry, and Flink keys) for the query steps; without it, those cells print what they'd run
instead of failing.

Ends with a **combined Cleanup** cell that, when enabled, tears down **everything created across
all three notebooks**: disables Tableflow, then deletes Part 2's six topics/subjects/Flink
statements, then Part 1's two topics/subjects -- rebuilding its own Kafka/Schema Registry clients
from `.env` since each notebook normally runs in a separate Jupyter kernel.

## Files

| File | Purpose |
|---|---|
| `kredivo-00-setup.ipynb` | Environment/credentials checks -- run first |
| `kredivo-01.ipynb` | Schema Registry, Data Contracts, CSFLE |
| `kredivo-02.ipynb` | Flink SQL: dedup, filter, enrich |
| `kredivo-03.ipynb` | Tableflow: Iceberg tables over the enriched topic |
| `.env.example` | Template for the credentials every notebook reads from `.env` |
| `.env` | Your actual credentials (git-ignored, never committed) |
| `.gitignore` | Excludes `.env`, `.venv/`, local backups, and OS/cache files |

## Environment variables

All variables live in one `.env` file (see `.env.example` for the full list). Optional
`PARTICIPANT_ID` namespaces every topic this workshop creates so multiple attendees running the
same notebooks don't collide; it defaults to your OS username if unset.
