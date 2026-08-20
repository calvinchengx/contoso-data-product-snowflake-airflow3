"""Leaf-boundary tests. No Docker, no emulator, no platform.

A leaf is the half of a cell a Snowflake team would actually write, and it has
to stand on its own: clone it, resolve it, run these. THE PIPELINE IS NOT RUN
HERE -- that needs the emulator, four vendors and an Airflow stack, and it
belongs to `snowflake-platform-airflow3`'s `make verify`.
"""

from __future__ import annotations

import json
import pathlib
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
PKG = ROOT / "src" / "contoso_sf_airflow"


def test_no_transform_sql_is_committed():
    """Every model, macro and test comes from `contoso-data-product` at run time.

    ASK GIT, not the filesystem: `dbt/*/target` holds artefacts a build puts
    there, and a tree walk would report the very thing this cell is supposed to
    fetch. The promise is that none of it is COMMITTED.
    """
    out = subprocess.run(
        ["git", "ls-files", "*.sql"], cwd=ROOT, capture_output=True, text=True, check=True
    )
    tracked = [x for x in out.stdout.splitlines() if x.strip()]
    assert tracked == [], f"transform SQL has been committed to the leaf: {tracked}"


def test_the_leaf_holds_no_infrastructure():
    forbidden = ["docker-compose.yml", "versions.env", "Dockerfile", "Dockerfile.worker"]
    present = [f for f in forbidden if (ROOT / f).exists()]
    assert present == [], f"infrastructure has arrived in the leaf: {present}"


def test_both_wheels_come_from_a_tagged_release():
    proj = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    for repo in ("contoso-data-product", "snowflake-emulator"):
        assert f"{repo}/releases/download/v" in proj, f"{repo} does not install from a tag"


def test_no_dependency_comes_from_a_sibling_checkout():
    proj = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    offenders = [
        line.strip()
        for line in proj.splitlines()
        if "path = " in line and "../" in line and not line.lstrip().startswith("#")
    ]
    assert not offenders, f"a lone clone could not build: {offenders}"


def test_no_vendor_address_has_a_host_default():
    """The trap this cell would otherwise walk into, and it is not hypothetical.

    The Tasks leaf defaults each vendor to `http://localhost:182xx` -- that
    platform's PUBLISHED port -- because its steps run on the host. Here the
    worker is a container on a compose network, and a published-port default
    would be wrong in a way that looks right: it would resolve, connect to
    whatever else is on that port, and on this machine that is plausibly the
    SIBLING Tasks stack's vendor. The cell would ingest another cell's bytes and
    report success.

    So: no host literals anywhere in the bindings.
    """
    import ast

    offenders = []
    for p in PKG.glob("*.py"):
        tree = ast.parse(p.read_text(encoding="utf-8"))
        # Docstrings are how this repository explains itself, and one of them
        # QUOTES the very literal this test forbids. So walk the AST and look at
        # string constants that are not docstrings -- the prose is documentation,
        # a default in code is a decision.
        docstrings = {
            id(node.body[0].value)
            for node in ast.walk(tree)
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            and node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)
        }
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in docstrings
                and ("localhost:" in node.value or "127.0.0.1:" in node.value)
            ):
                offenders.append(f"{p.name}:{node.lineno} {node.value!r}")
    assert offenders == [], (
        "a binding module carries a host literal; addresses come from the "
        "connection the platform provisions:\n  " + "\n  ".join(offenders)
    )


def test_the_stage_is_resolved_in_one_place():
    """Ingest writes it and the warehouse -- another container -- reads it.

    A step deriving the path itself would write where `COPY INTO` cannot look,
    and the symptom is an EMPTY BRONZE rather than an error.
    """
    assert (PKG / "stage.py").is_file()
    offenders = [
        p.name
        for p in PKG.glob("*.py")
        if p.name != "stage.py"
        and "STAGE" in p.read_text(encoding="utf-8")
        and "from contoso_sf_airflow.stage import STAGE" not in p.read_text(encoding="utf-8")
    ]
    assert offenders == [], f"these steps name a stage without importing it: {offenders}"


def test_the_dbt_requirements_match_the_group_that_builds_the_manifest():
    """Cosmos builds a venv from a LIST; the manifest is built from a LOCK.

    A drift between them means the graph was rendered by a different dbt than
    the tasks run -- which is a stale-manifest bug with extra steps.
    """
    dag = (ROOT / "dags" / "contoso_daily.py").read_text(encoding="utf-8")
    proj = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    for req in ("dbt-core>=1.9,<2", "dbt-snowflake>=1.8,<2"):
        assert req in dag, f"{req} missing from the DAG's DBT_REQUIREMENTS"
        assert req in proj, f"{req} missing from pyproject's dbt group"
    assert req in (ROOT / "scripts" / "manifest.py").read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# The rendered graph. These need the manifests, so they are marked and the
# platform's `make manifest` (or `scripts/manifest.py`) builds them first.
# --------------------------------------------------------------------------

def _dag():
    manifests = [ROOT / "dbt" / n / "target" / "manifest.json" for n in ("silver", "gold")]
    if not all(m.exists() for m in manifests):
        pytest.skip("manifests not built -- run `python scripts/manifest.py`")
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(ROOT / "dags"))
    assert not bag.import_errors, bag.import_errors
    # `.dags`, not `.get_dag()`: the latter queries the metadata database for
    # freshness, and there is no database here on purpose -- a test that needed
    # one could only run inside a stack, which is the opposite of what a leaf
    # test is for.
    dag = bag.dags.get("contoso_daily")
    assert dag is not None, f"no DAG called contoso_daily in {sorted(bag.dags)}"
    return dag


@pytest.mark.render
def test_every_singular_test_in_both_projects_has_a_task_to_run_it():
    """THE GUARD THIS FAMILY HAS PAID FOR TWICE.

    Cosmos's default `TestBehavior.AFTER_EACH` renders one test task per model
    and evaluates NO singular test -- a singular test is attached to no model, so
    a per-model task has nowhere to hang it. The DAG renders clean, runs clean,
    and publishes no guarantee. That is G29, found in the Fabric Airflow cell
    where core's one silver singular test had never run, and G41, found in the
    Tasks cell where five ODCS contracts were listed from a directory and
    evaluated by nothing.

    This asserts the SHAPE that makes singular tests possible -- a whole-suite
    test task in each group -- rather than the config value producing it today.
    """
    dag = _dag()
    ids = {t.task_id for t in dag.tasks}
    for group in ("silver", "gold"):
        suite = {i for i in ids if i.startswith(f"{group}.") and i.endswith("_test")}
        assert suite, (
            f"the {group} group renders no whole-suite test task, so its "
            f"singular tests are evaluated by nothing"
        )


@pytest.mark.render
def test_the_graph_runs_the_medallion_in_order():
    dag = _dag()
    ids = {t.task_id for t in dag.tasks}
    for expected in ("provision", "land", "to_bronze", "silver_env", "gold_env", "publish"):
        assert expected in ids, f"no {expected} task: {sorted(ids)[:12]}"
    # bronze must precede silver, and silver precede gold. Reading the edges
    # rather than trusting the source order they were written in.
    downstream = {t.task_id: set(t.downstream_task_ids) for t in dag.tasks}
    assert any(d.startswith("silver") for d in downstream["silver_env"])
    assert any(d.startswith("gold") for d in downstream["gold_env"])


@pytest.mark.render
def test_cosmos_emits_no_assets_of_its_own():
    """G37: cosmos assigned three concurrent gold tasks the SAME outlet.

    They raced to create one AssetModel row; one won, and the API server
    answered the others with "Error updating Task Instance state" WHILE THEIR
    PAYLOAD SAID SUCCESS. One run in two, on a model that built correctly.
    """
    dag = (ROOT / "dags" / "contoso_daily.py").read_text(encoding="utf-8")
    assert dag.count("emit_datasets=False") == 2, (
        "both DbtTaskGroups must disable cosmos's own asset emission"
    )
    rendered = _dag()
    for t in rendered.tasks:
        if t.task_id.startswith(("silver.", "gold.")):
            assert not getattr(t, "outlets", None), (
                f"{t.task_id} carries a cosmos-assigned outlet"
            )


@pytest.mark.render
def test_the_manifests_are_stamped_with_the_installed_product():
    import importlib.metadata

    installed = importlib.metadata.version("contoso-data-product")
    for n in ("silver", "gold"):
        stamp = ROOT / "dbt" / n / "target" / "manifest.stamp.json"
        if not stamp.exists():
            pytest.skip("manifests not built")
        assert json.loads(stamp.read_text())["contoso_data_product"] == installed


def test_the_readme_inventory_matches_the_pinned_core():
    """The README's product list must be what this leaf's pin actually contains.

    A generated list that falls behind is worse than none: a reader trusts it
    BECAUSE it looks generated. The check lives in the core, so all seven leaves
    ask the same question of their own pin, and it fails here, in the repository
    that has to fix it.

    Regenerate with:  python -m contoso_product.show --markdown
    """
    from pathlib import Path

    from contoso_product import show

    ok, message = show.check(Path(__file__).resolve().parent.parent / "README.md")
    assert ok, message
