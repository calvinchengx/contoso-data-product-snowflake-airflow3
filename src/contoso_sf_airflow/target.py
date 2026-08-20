"""This product's policy on top of the published snowflake-target contract.

WHAT CHANGED FROM THE TASKS LEAF, and it is the only thing: the address comes
from a CONNECTION rather than from a default. There the steps run on the host
against a published port, so `http://127.0.0.1:18448` is a reasonable default
and being wrong about it fails immediately. Here the worker is a container on a
compose network, and the emulator's address is a property of the deployment --
so the platform states it and this module asks.

The names below are the product's own and are the SAME on both targets: under
`SNOWFLAKE_TARGET=real` `contoso_warehouse` addresses a real warehouse and
`TEST_DB` a real database. A name is the cross-target address; a host is not.
"""

from __future__ import annotations

import os

import snowflake_target

from contoso_sf_airflow.bindings import connection

WAREHOUSE = "contoso_warehouse"
DATABASE = "TEST_DB"
SCHEMA_GOLD = "gold"
SCHEMA_SILVER = "PUBLIC"
CONN_ID = "snowflake"


def config() -> dict:
    """Everything the warehouse connection carries, resolved once."""
    c = connection(CONN_ID)
    return {
        "url": c.get("url") or c.get("host") or "",
        "host": c.get("host", ""),
        "port": c.get("port"),
        "password": c.get("password", ""),
        "account": c.get("account", "test"),
        "user": c.get("user", "admin"),
        "warehouse": c.get("warehouse", WAREHOUSE),
        "database": c.get("database", DATABASE),
        "target": c.get("target", "emulator"),
    }


def T():
    """The snowflake-target switch, pointed at whatever the platform provisioned.

    `SNOWFLAKE_PASSWORD` is set from the connection rather than left to
    snowflake-target's own lookup, which reads `<data_dir>/admin.pat` -- a path
    that exists on the host in the Tasks cell and inside the EMULATOR's
    container here. A product reaching for a file in another container is the
    coupling the connection removes.
    """
    c = config()
    os.environ["SNOWFLAKE_TARGET"] = c["target"]
    os.environ.setdefault("SNOWFLAKE_EMULATOR_URL", c["url"])
    os.environ.setdefault("SNOWFLAKE_WAREHOUSE", c["warehouse"])
    os.environ.setdefault("SNOWFLAKE_DATABASE", c["database"])
    os.environ.setdefault("SNOWFLAKE_ACCOUNT", c["account"])
    if c["password"]:
        os.environ.setdefault("SNOWFLAKE_PASSWORD", c["password"])
    return snowflake_target.target()


def dbt_env(schema: str) -> dict[str, str]:
    """The environment both dbt projects read, built from the connection.

    dbt-snowflake wants a hostname and a port, not a URL. Splitting it here
    rather than in a profile keeps the profiles identical across targets.
    """
    c = config()
    host = c["host"] or c["url"]
    host = host.replace("https://", "").replace("http://", "")
    hostname, _, port = host.partition(":")
    return {
        "SNOWFLAKE_ACCOUNT": c["account"],
        "SNOWFLAKE_USER": c["user"],
        "SNOWFLAKE_PASSWORD": c["password"],
        "SNOWFLAKE_WAREHOUSE": c["warehouse"],
        "SNOWFLAKE_DATABASE": c["database"],
        "SNOWFLAKE_SCHEMA": schema,
        "SNOWFLAKE_HOST": hostname,
        "SNOWFLAKE_PORT": str(c["port"] or port or 443),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
