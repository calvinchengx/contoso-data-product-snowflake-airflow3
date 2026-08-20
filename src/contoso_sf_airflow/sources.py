"""Where the source systems live: asked for by CONNECTION NAME.

Contoso POS, Web, Reference and ERP are NOT Snowflake. They are the vendors a
pipeline pulls from, and in production they are a real REST endpoint, a real
Postgres and a real Kafka broker.

WHAT CHANGED FROM THE TASKS LEAF. There the steps run on the host, so each
vendor had a default like `http://localhost:18290` -- this platform's PUBLISHED
port. Here the worker is a container on the compose network and reaches vendors
by SERVICE NAME on their own ports, which are exposed rather than published. A
published-port default would be wrong in a way that looks right: it would
resolve, connect to whatever else is on that port -- plausibly the SIBLING
Tasks stack's vendor -- and ingest bytes from another cell's fixtures.

So there is no host default here at all. The names come from
`contoso-sources/sources.yaml`, the platform provisions one connection per
declared vendor, and this module asks for them by those names.
"""

from __future__ import annotations

from contoso_sf_airflow.bindings import connection

# The connection each vendor is provisioned under, from the declaration's own
# `conn:` field. A vendor added over there needs a line here and nothing else.
POS_CONN = "contoso_pos"
WEB_CONN = "contoso_web"
REFERENCE_CONN = "contoso_reference"
ERP_CONN = "contoso_erp"

# The secret names, unchanged across every cell in the family: a NAME is the
# cross-target address, so under `real` the same name addresses the customer's
# own credential store.
POS_KEY_SECRET = "contoso-pos-api-key"
WEB_KEY_SECRET = "contoso-web-api-key"
REFERENCE_KEY_SECRET = "contoso-reference-api-key"


def api(conn_id: str) -> str:
    """The vendor's base URL, refusing rather than guessing."""
    host = connection(conn_id).get("host", "")
    if not host:
        raise RuntimeError(
            f"connection {conn_id!r} carries no host. Ingesting from a guessed "
            f"address is how a cell reads another cell's fixtures."
        )
    return host.rstrip("/")


def stream(conn_id: str = ERP_CONN) -> tuple[str, str]:
    """The ERP change stream: (bootstrap, topic).

    A stream vendor has no base URL, so its broker and topic ride in the
    connection's extra the way an HTTP vendor's URL rides in its host. Same
    seam, so production points this at the real ERP's stream unchanged.
    """
    c = connection(conn_id)
    bootstrap, topic = c.get("bootstrap", ""), c.get("topic", "")
    if not bootstrap or not topic:
        raise RuntimeError(
            f"connection {conn_id!r} carries no bootstrap/topic "
            f"(got {bootstrap!r}/{topic!r}); a CDC ingest cannot guess either."
        )
    return bootstrap, topic
