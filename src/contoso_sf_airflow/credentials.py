"""How an ingest step gets a vendor's API key. Never from this source tree.

THE PLATFORM PROVISIONS IT, and the product asks by connection name. Each
vendor publishes its own key to the customer it issued it to -- the same file
the vendor's own server reads to decide what to accept -- and the platform puts
that in the Connection at start-up. So a vendor rotating its key changes nothing
here, and production provisions the same names against the real vendors'
credentials.

WHY THERE IS NO FILE FALLBACK, unlike the Tasks leaf. There the steps run on the
host beside a `contoso-sources` checkout, so reading `_data/<vendor>/.api-key`
is reading a file that is genuinely there. Here the worker is a container, that
checkout is mounted read-only for the PLATFORM's benefit, and a product reaching
into it would be reaching into a platform. If the connection has no key, that is
a provisioning failure and it should say so rather than find one another way.
"""

from __future__ import annotations

from contoso_sf_airflow.bindings import connection

# The connection each vendor's key arrives in. The SECRET NAME stays the family's
# cross-target address; the connection is how this deployment delivers it.
CONN_OF = {
    "contoso-pos-api-key": "contoso_pos",
    "contoso-web-api-key": "contoso_web",
    "contoso-reference-api-key": "contoso_reference",
}


def env_name(secret: str) -> str:
    """The environment variable a job would receive this secret in."""
    return secret.upper().replace("-", "_")


def resolve(secret: str) -> str:
    """The vendor's key, from the connection the platform provisioned.

    RESOLVED PER CALL, never at import: a module-level lookup runs at DAG parse
    time, in the dag-processor, possibly before the platform has provisioned
    anything -- and a DAG that fails to parse for a missing credential reads
    like a broken DAG.
    """
    conn_id = CONN_OF.get(secret)
    if not conn_id:
        raise SystemExit(f"no vendor connection carries {secret!r}")
    key = connection(conn_id).get("password", "")
    if not key:
        raise SystemExit(
            f"connection {conn_id!r} carries no credential for {secret!r}. The "
            f"platform reads each vendor's own published key at start-up and "
            f"puts it here; an empty one is a provisioning failure, not "
            f"something this product should work around."
        )
    return key
