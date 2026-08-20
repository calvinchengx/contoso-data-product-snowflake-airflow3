"""The Contoso data product bound to Snowflake, orchestrated by Airflow 3.

The transforms are NOT here. Every silver model and every line of gold SQL comes
from `contoso-data-product` and is rendered by cosmos at parse time. What lives
here is the binding: four vendor ingests, a bronze built by `COPY INTO` from an
internal stage, and the profiles dbt needs.
"""

from contoso_sf_airflow.bindings import BindingError, connection

__all__ = ["BindingError", "connection"]
