"""The Contoso daily pipeline on Snowflake, orchestrated by Airflow 3.

WHAT THIS CELL IS FOR. `snowflake-platform-tasks` already shows these steps
produce the family's numbers on Snowflake, and the two Airflow 3 cells already
show an external Airflow can drive a medallion. What none of them shows is that
the SNOWFLAKE binding is orchestrator-independent. This cell is where that is
either true or not: the same steps, against the same engine, at the same pins,
driven by a different orchestrator. If its gold differs from the Tasks cell's,
the orchestrator is the only remaining candidate.

SO NOTHING ELSE MOVES. Bronze is still `COPY INTO` from an internal stage,
silver and gold are still core's dbt projects, and the pins are the Tasks cell's
exactly. Changing any of those alongside the orchestrator would leave two
candidate explanations for a difference in the numbers, which would cost this
cell the one thing it exists to establish.

WHAT DOES CHANGE, and there are two, both forced by where the code now runs:

  THE STAGE IS A VOLUME. Ingest runs inside the worker; the warehouse is another
  container. A host path would have the worker writing one filesystem and the
  warehouse reading another, and the symptom is not an error -- it is a
  `COPY INTO` that loads zero rows. The platform mounts one named volume into
  both and names it in PRODUCT_STAGE.

  EVERY ADDRESS COMES FROM A CONNECTION. The Tasks leaf defaults to this
  platform's published ports because its steps run on the host. Here a published
  port would resolve to whatever else is on it -- plausibly the SIBLING Tasks
  stack -- so there are no host defaults at all.

TASK SDK ONLY (`airflow.sdk`). That is Airflow 3's boundary between task code
and the scheduler's internals, and it is what lets this same file run on a
managed Airflow in production without edits.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import pathlib

import pendulum
from airflow.sdk import Asset, dag, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior

from contoso_product import gold_dir, silver_dir

# The profiles live here; the MODELS come from the installed product package.
DBT_DIR = pathlib.Path(__file__).resolve().parent.parent / "dbt"

# dbt IS NOT INSTALLED IN THE WORKER, and cannot be. `dbt-snowflake` pins
# `snowflake-connector-python` back below what ingest and bronze need, and
# pyproject.toml declares the two groups CONFLICTING rather than letting one
# silently win. So the dbt tasks build their own virtualenv from these
# requirements -- the same two-environment split the Tasks cell gets by running
# each step with its own dependency group, drawn at a task boundary instead.
#
# The pins are literal because a venv is built from a list, not a lockfile.
# Keep them equal to the `dbt` group in pyproject.toml.
DBT_REQUIREMENTS = [
    "dbt-core>=1.9,<2",
    "dbt-snowflake>=1.8,<2",
]
# An operator who has already provisioned a dbt environment can name it and skip
# the per-run build; unset, cosmos creates one.
DBT_VENV = os.environ.get("COSMOS_DBT_VENV")

# What bronze calls its tables, against what the silver models ask for. The
# indirection is the PRODUCT's -- `source('bronze', var('bronze_pos_orders'))` --
# so a platform whose bronze predates the vendor-prefixed scheme supplies its
# own names rather than renaming its tables.
BRONZE_NAMES = {
    "bronze_pos_customers": "bronze_pos_customers",
    "bronze_pos_orders": "bronze_pos_orders",
    "bronze_web_customers": "bronze_web_customers",
    "bronze_web_orders": "bronze_web_orders",
    "bronze_web_products": "bronze_web_products",
    "bronze_ref_product_hierarchy": "bronze_product_hierarchy",
    "bronze_ref_fx_rates": "bronze_fx_rates",
    "bronze_erp_customer_changes": "bronze_erp_changes",
}


def _manifest(name: str) -> pathlib.Path:
    """The manifest, having proved it describes the product that is installed.

    A manifest is a generated artifact describing a PINNED package's SQL. If the
    pin moves and this does not, cosmos renders yesterday's models against
    today's code and reports nothing wrong. Raising here shows up as a DAG
    import error, which is loud; rendering a stale graph is silent, and this
    family has already lost a day to a stale artifact that looked green.
    """
    target = DBT_DIR / name / "target"
    manifest, stamp = target / "manifest.json", target / "manifest.stamp.json"
    if not manifest.exists():
        raise RuntimeError(
            f"no dbt manifest at {manifest}. Build it with "
            f"`python scripts/manifest.py` (the platform's `make manifest` does "
            f"this) -- without it cosmos falls back to a parser that drops the "
            f"contracts, and the DAG would look healthy anyway."
        )
    built_from = json.loads(stamp.read_text(encoding="utf-8")).get(
        "contoso_data_product"
    ) if stamp.exists() else None
    installed = importlib.metadata.version("contoso-data-product")
    if built_from != installed:
        raise RuntimeError(
            f"the {name} manifest was built from contoso-data-product "
            f"{built_from} but {installed} is installed. Rebuild it -- rendering "
            f"it would run the pinned package's code against a graph describing "
            f"a different version."
        )
    return manifest


# DERIVED FROM THE PRODUCT, not listed here. A hand-kept list is a second place
# the set of tables lives, and it is wrong on the day core adds one -- quietly.
SILVER_MODELS = sorted(p.stem for p in (silver_dir() / "models").glob("*.sql"))
GOLD_MODELS = sorted(p.stem for p in (gold_dir() / "models").glob("*.sql"))
GOLD_ASSETS = [Asset(f"contoso://gold/{m}") for m in GOLD_MODELS]

# The four vendors, as the product's ingest modules name them. Each is its own
# module because each is its own failure -- a wrong key, a mangled binary body,
# a short change stream -- and one function would report all four as
# "ingest failed". Four MAPPED tasks, so each retries alone.
VENDORS = [
    {"name": "Contoso POS", "module": "ingest_pos"},
    {"name": "Contoso Web", "module": "ingest_web"},
    {"name": "Contoso Reference", "module": "ingest_reference"},
    {"name": "Contoso ERP", "module": "ingest_erp_cdc"},
]


@dag(
    dag_id="contoso_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["contoso", "snowflake", "medallion"],
)
def contoso_daily():
    @task
    def provision() -> dict:
        """Warehouse and database. Names resolved, never stored."""
        from contoso_sf_airflow.provision import main as provision_main
        from contoso_sf_airflow.target import DATABASE, WAREHOUSE

        rc = provision_main()
        if rc != 0:
            raise RuntimeError(f"provision exited {rc}")
        return {"warehouse": WAREHOUSE, "database": DATABASE}

    @task
    def land(vendor: dict, ctx: dict) -> dict:
        """One vendor into the internal stage. Mapped, so each retries alone."""
        import importlib

        mod = importlib.import_module(f"contoso_sf_airflow.{vendor['module']}")
        rc = mod.main()
        if rc != 0:
            raise RuntimeError(f"{vendor['name']} ingest exited {rc}")
        return {"vendor": vendor["name"]}

    @task
    def to_bronze(landed: list[dict]) -> dict:
        """`COPY INTO` from the stage, and the contract checked afterwards."""
        from contoso_sf_airflow.bronze import main as bronze_main

        rc = bronze_main()
        if rc != 0:
            raise RuntimeError(f"bronze exited {rc}")
        return {"vendors": [x["vendor"] for x in landed]}

    @task
    def silver_env(ctx: dict) -> dict:
        """The environment silver's dbt tasks run with, resolved at RUN time.

        Not at parse time: the credential is provisioned by the platform after
        the dag-processor has already scanned this file at least once, and a DAG
        that fails to parse for a missing connection reads as a broken DAG.
        """
        from contoso_sf_airflow.target import SCHEMA_SILVER, dbt_env

        env = dbt_env(SCHEMA_SILVER)
        # Bronze landed in the schema silver writes to, so the source lookup and
        # the model output agree without a second schema to provision. Named
        # rather than defaulted, because the models default it to `bronze`.
        env["DBT_BRONZE_SCHEMA"] = SCHEMA_SILVER
        return env

    @task
    def gold_env(ctx: dict) -> dict:
        from contoso_sf_airflow.target import DATABASE, SCHEMA_GOLD, SCHEMA_SILVER, dbt_env

        env = dbt_env(SCHEMA_GOLD)
        env["CONTOSO_SILVER_DATABASE"] = DATABASE
        env["CONTOSO_SILVER_SCHEMA"] = SCHEMA_SILVER
        # LAKEHOUSE_ID IS A FABRIC NAME AND THIS IS NOT FABRIC, but core's gold
        # sources.yml spells the silver database
        # `env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID'))`, and
        # Jinja evaluates the DEFAULT EAGERLY -- so the fallback is read whether
        # or not the first name is set, and a Fabric-only variable becomes
        # mandatory on every engine. Without it dbt fails while PARSING, which
        # the Tasks cell recorded as a dialect gap for months.
        env["LAKEHOUSE_ID"] = DATABASE
        return env

    ctx = provision()
    landed = land.partial(ctx=ctx).expand(vendor=VENDORS)
    bronze = to_bronze(landed)
    senv = silver_env(ctx)
    genv = gold_env(ctx)

    # THE CONTRACTS RUN, and that is what `AFTER_ALL` buys. The obvious choice
    # is AFTER_EACH -- one test task per model, which reads better in the UI --
    # and it DROPS EVERY SINGULAR TEST: they are attached to no model, so a
    # per-model test task has nowhere to hang them. The DAG renders clean, runs
    # clean, and never evaluates a guarantee the product publishes. That is G29,
    # found in the Fabric Airflow cell, and G41, found in the Tasks cell three
    # weeks later wearing a different costume.
    #
    # AFTER_ALL renders one whole-suite test task per group -- silver's 13 and
    # gold's 52 -- and each is a task with its own state, so there is no shared
    # run_results.json to misread.
    silver = DbtTaskGroup(
        group_id="silver",
        project_config=ProjectConfig(
            dbt_project_path=silver_dir(),
            manifest_path=_manifest("silver"),
            dbt_vars=BRONZE_NAMES,
        ),
        profile_config=ProfileConfig(
            profile_name="contoso_silver",
            target_name="dev",
            profiles_yml_filepath=DBT_DIR / "silver" / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
            virtualenv_dir=DBT_VENV,
        ),
        # `operator_args["env"]` is deprecated in favour of
        # `ProjectConfig.env_vars`, and it is used anyway ON PURPOSE: env_vars is
        # a dict evaluated at PARSE time, and the credential here does not exist
        # then -- the platform provisions it after the dag-processor has already
        # scanned this file. An XComArg can only be templated through
        # operator_args, so the deprecated path is the only one that resolves at
        # run time. Revisit when cosmos offers a templated env_vars.
        operator_args={
            "env": senv,
            "append_env": True,
            "py_requirements": DBT_REQUIREMENTS,
        },
        render_config=RenderConfig(
            # EXPLICIT, never AUTOMATIC: automatic falls back to the custom
            # parser when dbt is absent, which it always is here.
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_ALL,
            # NO COSMOS ASSETS. Left on, cosmos assigns each model task an
            # outlet of its own devising at RUN time, and in the Fabric Airflow
            # cell it assigned three concurrent gold tasks the SAME one. They
            # raced to create one AssetModel row; one won, and the API server
            # answered the others with "Error updating Task Instance state"
            # WHILE THEIR PAYLOAD SAID SUCCESS (G37). This product declares its
            # own target-neutral assets and emits them from `publish`, the task
            # that COUNTS the rows rather than the one that wrote them.
            emit_datasets=False,
        ),
        default_args={"retries": 0},
    )

    gold = DbtTaskGroup(
        group_id="gold",
        project_config=ProjectConfig(
            dbt_project_path=gold_dir(),
            manifest_path=_manifest("gold"),
        ),
        profile_config=ProfileConfig(
            profile_name="contoso_gold",
            target_name="dev",
            profiles_yml_filepath=DBT_DIR / "gold" / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
            virtualenv_dir=DBT_VENV,
        ),
        operator_args={
            "env": genv,
            "append_env": True,
            "py_requirements": DBT_REQUIREMENTS,
        },
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_ALL,
            emit_datasets=False,
        ),
        default_args={"retries": 0},
    )

    @task(outlets=GOLD_ASSETS)
    def publish() -> dict:
        """Read gold at money's own grain and write the snapshot."""
        from contoso_sf_airflow.provision import sql
        from contoso_sf_airflow.target import T

        t = T()

        def one(statement: str):
            out = sql(t, statement)
            if not out.get("success"):
                raise RuntimeError(f"{statement}: {out.get('message')}")
            return (out.get("data") or {}).get("rowset") or []

        counts = {m: int(one(f"SELECT count(*) FROM {m}")[0][0]) for m in GOLD_MODELS}
        empty = sorted(m for m, n in counts.items() if n == 0)
        if empty:
            raise RuntimeError(
                "gold models built but are empty, so a snapshot of them would "
                f"record nothing as a result: {', '.join(empty)}"
            )

        rows = one(
            "SELECT coalesce(sum(revenue_usd),0), coalesce(sum(cancelled_revenue_usd),0), "
            "coalesce(sum(sale_lines),0) FROM fct_revenue_summary"
        )

        # THE CONTRACTS THIS RUN ACTUALLY RENDERED, read from the manifest the
        # graph was built from -- not globbed off disk. A name on disk that no
        # task executed is exactly the stale-evidence defect the Tasks leaf was
        # fixed for (G41), and a snapshot is where it would be published.
        nodes = json.loads(_manifest("gold").read_text(encoding="utf-8"))["nodes"]
        tests = [k for k in nodes if k.startswith("test.")]
        shipped = sorted(p.stem for p in (gold_dir() / "tests").glob("*.sql"))
        contracts = sorted(
            c for c in shipped
            if any(t.endswith(f".{c}") or f".{c}." in t for t in tests)
        )
        missing = sorted(set(shipped) - set(contracts))
        if missing:
            raise RuntimeError(
                "core ships contracts the rendered graph does not contain, so "
                f"the gold test task never evaluated them: {', '.join(missing)}"
            )

        snapshot = {
            "revenue_usd": str(rows[0][0]),
            "cancelled_revenue_usd": str(rows[0][1]),
            "sale_lines": str(rows[0][2]),
            "contracts": contracts,
            "runtime": "snowflake-airflow3",
            "catalog": os.environ.get("SNOWFLAKE_DATABASE", "TEST_DB"),
            "engine": "duckdb",
            "gold": counts,
        }
        out = pathlib.Path(
            os.environ.get("PRODUCT_SNAPSHOT", "product_snapshot.json")
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(snapshot, indent=2))
        return snapshot

    bronze >> senv >> silver >> genv >> gold >> publish()


contoso_daily()
