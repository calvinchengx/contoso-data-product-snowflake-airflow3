"""Render the product's silver and gold projects to dbt manifests, for cosmos.

WHY A MANIFEST AND NOT `dbt ls`. Cosmos renders the DAG at PARSE time, and its
default way of doing that is to run `dbt ls` -- which needs dbt in whatever
environment the dag-processor runs in. It cannot be there: `dbt-snowflake` pins
`snowflake-connector-python` back below what the ingest and bronze steps need,
and pyproject.toml declares the two groups CONFLICTING rather than letting one
silently win. So dbt lives in a virtualenv that only the dbt TASKS enter, and
parse time has no dbt at all.

WHAT COSMOS DOES WHEN dbt IS MISSING is why this file exists rather than being
left to the default. `LoadMode.AUTOMATIC` catches the FileNotFoundError and
falls back -- silently -- to a deprecated custom parser that knows a subset of
what dbt knows. The DAG still appears, still runs, and is missing whatever the
subset dropped. A graph that renders successfully while omitting the contracts
is exactly the failure this family keeps meeting: a guard reporting success
while doing nothing (G29, G36, G41).

So the manifests are built by dbt itself and loaded with
`LoadMode.DBT_MANIFEST` -- no fallback, no subset.

STAMPED WITH THE PRODUCT VERSION THEY WERE BUILT FROM. A manifest describes a
PINNED package's SQL; if the pin moves and this does not, cosmos renders
yesterday's models against today's code and says nothing.
"""

from __future__ import annotations

import json
import os
import subprocess
from importlib.metadata import version
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DBT = ROOT / "dbt"

# Kept equal to pyproject.toml's `dbt` group deliberately: this runs
# `--isolated`, so it reads no lockfile, and a drift between the two would mean
# the manifest was rendered by a different dbt than the tasks run.
DBT_REQUIREMENTS = ("dbt-core>=1.9,<2", "dbt-snowflake>=1.8,<2")

# What bronze calls its tables here, against what the silver models ask for.
# The indirection is the PRODUCT's -- `source('bronze', var('bronze_pos_orders'))`
# -- and `var()` has no default, so parse fails without these.
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


def build(name: str, project: Path) -> tuple[int, int]:
    profiles = DBT / name
    target = profiles / "target"

    cmd = ["uv", "run", "--isolated", "--no-project"]
    for req in DBT_REQUIREMENTS:
        cmd += ["--with", req]
    cmd += [
        "dbt", "parse",
        "--project-dir", str(project),
        "--profiles-dir", str(profiles),
        "--target-path", str(target),
        "--vars", json.dumps(BRONZE_NAMES),
    ]

    # PARSE-TIME PLACEHOLDERS, satisfied rather than fixed upstream.
    #
    # Gold's sources.yml reads `env_var('DBT_SILVER_DATABASE')`, and `dbt parse`
    # does not connect, so these values only have to EXIST. The real ones reach
    # the tasks at run time.
    #
    # THE UPSTREAM FIX HAPPENED. This block used to also set LAKEHOUSE_ID -- a
    # Fabric name, on a Snowflake cell -- because gold's default was
    # `env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID'))` and Jinja
    # evaluates a default EAGERLY, making it mandatory everywhere. The comment
    # here argued that changing a shared project to suit one consumer's renderer
    # was the wrong direction, and that was right about the renderer and wrong
    # about the project: core v0.6.0 stopped nesting the default AND moved the
    # names, because Snowflake's dbt Projects refuse any key that is not
    # UPPERCASE and DBT_-prefixed -- so the old spelling could not run there at
    # all. The placeholder is gone because the thing needing it is gone.
    env = os.environ.copy()
    for k, v in {
        "DBT_SILVER_DATABASE": "TEST_DB",
        "DBT_SILVER_SCHEMA": "PUBLIC",
        "DBT_BRONZE_SCHEMA": "PUBLIC",
        "SNOWFLAKE_ACCOUNT": "test",
        "SNOWFLAKE_USER": "admin",
        "SNOWFLAKE_PASSWORD": "parse-time-only",
        "SNOWFLAKE_WAREHOUSE": "contoso_warehouse",
        "SNOWFLAKE_DATABASE": "TEST_DB",
        "SNOWFLAKE_SCHEMA": "PUBLIC",
        "SNOWFLAKE_HOST": "localhost",
        "SNOWFLAKE_PORT": "8448",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }.items():
        env.setdefault(k, v)

    print("==> " + " ".join(cmd), flush=True)
    # `dbt parse` resolves the profile, so profiles.yml is exercised here too:
    # a placeholder that lost its default fails at BUILD time rather than at
    # DAG-parse time inside the scheduler, where it reads as a broken DAG.
    subprocess.check_call(cmd, env=env)

    manifest = target / "manifest.json"
    if not manifest.exists():
        raise SystemExit(
            f"dbt parse succeeded but wrote no {manifest} -- refusing to stamp "
            f"a manifest that is not there."
        )

    # PROVE IT CONTAINS THE SINGULAR TESTS. The whole reason for building a
    # manifest rather than accepting the fallback parser is that the fallback
    # drops things quietly; a manifest that had dropped them is no better.
    nodes = json.loads(manifest.read_text(encoding="utf-8")).get("nodes", {})
    tests = sorted(k for k in nodes if k.startswith("test."))
    models = sorted(k for k in nodes if k.startswith("model."))
    expect = sorted(p.stem for p in (project / "tests").glob("*.sql"))
    # dbt names a SINGULAR test `test.<project>.<name>` and a GENERIC one
    # `test.<project>.<name>.<hash>`. Matching on `.<name>.` alone finds only
    # the generic ones, so every singular test reads as missing. Both shapes.
    missing = [
        c for c in expect
        if not any(t.endswith(f".{c}") or f".{c}." in t for t in tests)
    ]
    if missing:
        raise SystemExit(
            f"the {name} manifest is missing tests that exist as files, so the "
            f"DAG would render without them and every run would look clean: "
            + ", ".join(missing)
        )

    (target / "manifest.stamp.json").write_text(
        json.dumps(
            {
                "contoso_data_product": version("contoso-data-product"),
                "models": len(models),
                "tests": len(tests),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"{name}: {len(models)} models, {len(tests)} tests")
    return len(models), len(tests)


def main() -> int:
    from contoso_product import gold_dir, silver_dir

    for name, project in (("silver", silver_dir()), ("gold", gold_dir())):
        build(name, project)
    print(f"manifests built from contoso-data-product {version('contoso-data-product')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
