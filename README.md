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

## What the product contains

The SQL is not here. It lives in the core so seven leaves cannot drift into
seven versions of it, and that costs you a click, so this list gives it back.
`make show-product` copies the same files into `product/` where you can open
them; the block below is generated from the pinned package and a test fails
when it falls behind.

<!-- BEGIN product inventory: python -m contoso_product.show --markdown -->

The product is [`contoso-data-product`](https://github.com/calvinchengx/contoso-data-product/tree/v0.5.1) at **v0.5.1**, the version this repository pins. It is not vendored here: these files live there and are staged locally by `make show-product`.

**silver**: 8 models, 1 singular test

- [`silver_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_customers.sql)
- [`silver_fx_daily`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_fx_daily.sql)
- [`silver_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_orders.sql)
- [`silver_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_party.sql)
- [`silver_product_hierarchy`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_product_hierarchy.sql)
- [`silver_quarantine_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_quarantine_orders.sql)
- [`silver_web_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_web_customers.sql)
- [`silver_web_order_lines`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_web_order_lines.sql)

Assertions over silver, each failing the build on its own:

- [`silver_orders_never_holds_a_non_positive_quantity`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/tests/silver_orders_never_holds_a_non_positive_quantity.sql)

**gold**: 9 models, 5 singular tests

- [`dim_country`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_country.sql)
- [`dim_customer`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_customer.sql)
- [`dim_date`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_date.sql)
- [`dim_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_party.sql)
- [`dim_product`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_product.sql)
- [`fct_daily_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/fct_daily_revenue.sql)
- [`fct_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/fct_orders.sql)
- [`fct_revenue_summary`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/fct_revenue_summary.sql)
- [`fct_sales`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/fct_sales.sql)

Assertions over gold, each failing the build on its own:

- [`both_selling_systems_reach_the_pack`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/both_selling_systems_reach_the_pack.sql)
- [`every_country_resolves_to_the_dimension`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/every_country_resolves_to_the_dimension.sql)
- [`fiscal_year_is_not_the_calendar_year`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/fiscal_year_is_not_the_calendar_year.sql)
- [`money_is_never_stored_as_float`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/money_is_never_stored_as_float.sql)
- [`revenue_summary_loses_no_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/revenue_summary_loses_no_revenue.sql)

<!-- END product inventory -->

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
