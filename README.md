# contoso-data-product-snowflake-airflow3

The Contoso data product on **Snowflake**, orchestrated by **an external Apache
Airflow 3**.

This is the half of the cell a Snowflake team would actually write: four vendor
ingests, a bronze built by `COPY INTO` from an internal stage, the dbt profiles
cosmos renders silver and gold from, and the target binding that switches
between [`snowflake-emulator`](https://github.com/calvinchengx/snowflake-emulator)
and a real Snowflake account.

Its platform is
[`snowflake-platform-airflow3`](https://github.com/calvinchengx/snowflake-platform-airflow3):

```bash
make verify PRODUCT=../contoso-data-product-snowflake-airflow3 DAG=contoso_daily
```

## What this cell exists to establish

[`contoso-data-product-snowflake-tasks`](https://github.com/calvinchengx/contoso-data-product-snowflake-tasks)
already shows these steps produce the family's numbers on Snowflake. The Fabric
and Databricks Airflow cells already show an external Airflow can drive a
medallion. What neither shows is that the **Snowflake binding** is
orchestrator-independent.

So nothing else moves: same steps, same engine, **the same pins**. If this
cell's gold differs from the Tasks cell's, the orchestrator is the only
remaining candidate.

## What is here

| | |
|---|---|
| `dags/contoso_daily.py` | provision → four mapped vendor ingests → bronze → silver → gold → publish |
| `src/contoso_sf_airflow/` | the bindings: ingests, `COPY INTO` bronze, the target and connection resolvers |
| `dbt/{silver,gold}/profiles.yml` | profiles only. The models arrive from core and are rendered from a manifest |
| `scripts/manifest.py` | builds those manifests, and refuses to stamp one missing a test that exists as a file |

## What is not here, and will not be

Transform SQL, an ODCS contract, or an expected number. Those live once, in
[contoso-data-product](https://github.com/calvinchengx/contoso-data-product), and
this leaf depends on it **by tag**. `test_no_transform_sql_is_committed` fails if
a `.sql` file ever appears.

## Two things that differ from the Tasks leaf

Both are forced by *where the code runs*, not by the orchestrator.

**Every address comes from a Connection.** The Tasks leaf defaults each vendor to
`http://localhost:182xx` — that platform's published port — because its steps run
on the host. Here the worker is a container, and a published-port default would
be wrong in a way that *looks right*: it would resolve, connect to whatever is on
that port, and on a developer's machine that is plausibly the **sibling Tasks
stack's vendor**. The cell would ingest another cell's bytes and report success.
`test_no_vendor_address_has_a_host_default` forbids the literals.

**The stage is a volume.** Ingest runs in the worker; the warehouse is another
container. A host path would mean the two reading different filesystems, and the
symptom is not an error — it is a `COPY INTO` that loads **zero rows**.

## The guard this family has paid for twice

Cosmos's default `TestBehavior.AFTER_EACH` renders one test task per model and
evaluates **no singular test** — a singular test is attached to no model, so a
per-model task has nowhere to hang it. The DAG renders clean, runs clean, and
publishes no guarantee.

That is **G29** (the Fabric Airflow cell, where core's one silver singular test
had never run) and **G41** (the Tasks cell, where five ODCS contracts were listed
from a directory and evaluated by nothing).

Both groups use `AFTER_ALL`, and
`test_every_singular_test_in_both_projects_has_a_task_to_run_it` asserts the
*shape* rather than the config value. It was checked against the defect: switch
either group to `AFTER_EACH` and it fails.

## Where this fits

- [The family](https://github.com/calvinchengx/contoso-data-product/blob/main/docs/00-family.md)
- [The plan](https://github.com/calvinchengx/contoso-data-product/blob/main/docs/01-plan.md)

Apache-2.0.
