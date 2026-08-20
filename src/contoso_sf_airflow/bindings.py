"""How this product finds its targets: by CONNECTION NAME, and nothing else.

THE SEAM. The DAG says `snowflake` and `contoso_pos_api` and never a host, a
port or a credential. Those are provisioned by whichever platform is running
this product -- against the emulator locally, against a real Snowflake account
and the real vendors in production -- and not one line of code here changes
between the two. That is the whole reason a leaf and a platform are separate
repositories.

WHY THERE IS AN ENVIRONMENT FALLBACK, and why it is not a second configuration
system. These modules must be importable and testable OUTSIDE a DAG: `pytest`
in CI has no Airflow metadata database, and a witness that can only run inside
a scheduler cannot be checked by anything cheaper than a full stack. So the
lookup asks Airflow first and falls back to the environment, and the platform
sets the same values in both places.

IT DOES NOT INVENT A DEFAULT for anything that identifies a target. A missing
connection raises; a wrong-but-plausible default is how a product silently
talks to the wrong warehouse, and this family has paid for that class of bug
more than once.
"""

from __future__ import annotations

import json
import os
from typing import Any


class BindingError(RuntimeError):
    pass


def _airflow(conn_id: str):
    """The Airflow Connection, or None when there is no Airflow to ask.

    Import is deliberately inside the function: these modules are imported by
    tests and by `scripts/manifest.py`, neither of which has Airflow.
    """
    try:
        from airflow.sdk import BaseHook
    except Exception:  # noqa: BLE001 -- no Airflow here, which is normal
        try:
            from airflow.hooks.base import BaseHook  # type: ignore[no-redef]
        except Exception:  # noqa: BLE001
            return None
    try:
        return BaseHook.get_connection(conn_id)
    except Exception:  # noqa: BLE001 -- not provisioned, or no metadata DB
        return None


def connection(conn_id: str) -> dict[str, Any]:
    """host, password and extras for `conn_id`, from Airflow or the environment.

    The environment half is keyed by the connection name so a second vendor
    needs no code here: `CONTOSO_POS_API_URL` and `CONTOSO_POS_API_KEY` for
    `contoso_pos_api`.
    """
    conn = _airflow(conn_id)
    if conn is not None:
        extra: dict[str, Any] = {}
        raw = getattr(conn, "extra", None)
        if raw:
            try:
                extra = json.loads(raw)
            except json.JSONDecodeError:
                extra = {}
        host = conn.host or ""
        if conn.port and "://" in host and ":" not in host.split("://", 1)[1]:
            host = f"{host}:{conn.port}"
        return {"host": host, "password": conn.password or "", **extra}

    prefix = conn_id.upper()
    host = os.environ.get(f"{prefix}_URL") or os.environ.get(f"{prefix}_HOST") or ""
    key = os.environ.get(f"{prefix}_KEY") or os.environ.get(f"{prefix}_PASSWORD") or ""
    raw = os.environ.get(f"{prefix}_EXTRA")
    extra = json.loads(raw) if raw else {}
    if not host and not key and not extra:
        raise BindingError(
            f"no connection {conn_id!r}: Airflow does not have it and nothing "
            f"in the environment supplies {prefix}_URL / {prefix}_KEY. The "
            f"platform provisions these at start-up -- a task running before "
            f"that has finished is the usual cause."
        )
    return {"host": host, "password": key, **extra}
