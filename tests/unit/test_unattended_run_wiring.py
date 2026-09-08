"""Plumbing for the unattended monthly run.

None of this is pipeline logic; it is the wiring that decides whether a 09:00
scheduled run starts at all. Each check below stands in for a failure that has
no local symptom: a placeholder the launcher forgets to substitute ships the
literal ``@@TOKEN@@`` to the host, a .dockerignore rule silently drops the
scripts the host expects to lift out of the image, and a freshness gate that has
drifted from the build trigger either blocks a valid run or waves a stale one
through. All of them would first surface on the instance, an hour into the month.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
VIEW_DIR = ROOT / "sql" / "xpressfeed_views"
LAUNCH = ROOT / "scripts" / "aws" / "launch.sh"
USER_DATA = ROOT / "scripts" / "aws" / "user-data.sh"
PRODUCTION_RUN = ROOT / "scripts" / "production-run.sh"
DOCKERFILE = ROOT / "Dockerfile"
DOCKERIGNORE = ROOT / ".dockerignore"
WORKFLOW = ROOT / ".github" / "workflows" / "docker-publish.yml"

# Lifted out of the image with `docker cp` (the shell one) or executed inside it
# (the Python ones). All must survive the build context filter.
BAKED = (
    "scripts/check_source_ready.py",
    "scripts/production-run.sh",
    "sql/xpressfeed_views/load_ff_factors.py",
    "sql/xpressfeed_views/capture_sec_ids.py",
)


class TestCredentialLoading:
    """``load_env`` is the only credential path the two loaders have."""

    @pytest.fixture(scope="class")
    def gen_views(self):
        pytest.importorskip("psycopg")
        sys.path.insert(0, str(VIEW_DIR))
        import gen_views

        return gen_views

    def test_env_file_supplies_values(self, gen_views, tmp_path, monkeypatch):
        env = tmp_path / ".env"
        env.write_text('COMPUSTAT="postgresql://from-file"\nENV_USERNAME=file-user\n')
        monkeypatch.setattr(gen_views, "ENV", env)
        for key in gen_views.ENV_KEYS:
            monkeypatch.delenv(key, raising=False)

        loaded = gen_views.load_env()

        assert loaded["COMPUSTAT"] == "postgresql://from-file"
        assert loaded["ENV_USERNAME"] == "file-user"

    def test_environment_alone_is_enough(self, gen_views, tmp_path, monkeypatch):
        """The container form: the image ships no .env, only --env-file."""
        monkeypatch.setattr(gen_views, "ENV", tmp_path / "absent.env")
        monkeypatch.setenv("COMPUSTAT", "postgresql://from-env")
        monkeypatch.setenv("ENV_USERNAME", "env-user")
        monkeypatch.setenv("ENV_PASSWORD", "env-pass")

        loaded = gen_views.load_env()

        assert loaded == {
            "COMPUSTAT": "postgresql://from-env",
            "ENV_USERNAME": "env-user",
            "ENV_PASSWORD": "env-pass",
        }

    def test_environment_wins_over_the_file(self, gen_views, tmp_path, monkeypatch):
        env = tmp_path / ".env"
        env.write_text("COMPUSTAT=postgresql://from-file\n")
        monkeypatch.setattr(gen_views, "ENV", env)
        monkeypatch.setenv("COMPUSTAT", "postgresql://from-env")

        assert gen_views.load_env()["COMPUSTAT"] == "postgresql://from-env"

    def test_empty_environment_value_does_not_mask_the_file(self, gen_views, tmp_path, monkeypatch):
        """An exported-but-blank variable is absence, not an override."""
        env = tmp_path / ".env"
        env.write_text("COMPUSTAT=postgresql://from-file\n")
        monkeypatch.setattr(gen_views, "ENV", env)
        monkeypatch.setenv("COMPUSTAT", "")

        assert gen_views.load_env()["COMPUSTAT"] == "postgresql://from-file"

    def test_missing_file_and_environment_yields_nothing(self, gen_views, tmp_path, monkeypatch):
        monkeypatch.setattr(gen_views, "ENV", tmp_path / "absent.env")
        for key in gen_views.ENV_KEYS:
            monkeypatch.delenv(key, raising=False)

        assert gen_views.load_env() == {}


class TestUserDataPlaceholders:
    """launch.sh substitutes @@TOKEN@@; an unsubstituted one reaches the host."""

    @pytest.fixture(scope="class")
    def tokens(self):
        return set(re.findall(r"@@([A-Z_]+)@@", USER_DATA.read_text(encoding="utf-8")))

    @pytest.fixture(scope="class")
    def substituted(self):
        return set(re.findall(r"s\|@@([A-Z_]+)@@\|", LAUNCH.read_text(encoding="utf-8")))

    def test_every_placeholder_is_substituted(self, tokens, substituted):
        assert tokens - substituted == set()

    def test_no_substitution_is_left_over(self, tokens, substituted):
        """A rule for a token nobody uses means a rename went half-done."""
        assert substituted - tokens == set()

    @pytest.mark.parametrize("token", ["PARAM_EPHEMERAL", "UNATTENDED"])
    def test_unattended_tokens_are_wired(self, token, tokens, substituted):
        assert token in tokens and token in substituted


class TestImageContents:
    """The host lifts its operational scripts out of the image, not out of git."""

    @pytest.fixture(scope="class")
    def dockerfile(self):
        return DOCKERFILE.read_text(encoding="utf-8")

    def test_ops_scripts_are_copied_in(self, dockerfile):
        assert "COPY sql /opt/jkp/sql" in dockerfile
        assert "scripts/check_source_ready.py" in dockerfile
        assert "scripts/production-run.sh" in dockerfile

    def test_production_run_is_made_executable(self, dockerfile):
        """Git on Windows stores no exec bit, so the image has to add one."""
        assert re.search(r"chmod\s+0?755\s+/opt/jkp/scripts/production-run\.sh", dockerfile)

    def test_sql_is_not_excluded_from_the_build_context(self):
        lines = [
            line.strip()
            for line in DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        ]
        assert "sql" not in lines

    def test_psycopg_is_a_real_dependency(self):
        """The baked loaders import it; the image has no `uv run --with`."""
        assert "psycopg[binary]" in (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    def test_revision_label_is_stamped(self):
        assert "org.opencontainers.image.revision" in WORKFLOW.read_text(encoding="utf-8")


class TestFreshnessGateMatchesBuildTrigger:
    """The gate compares the image against the paths that rebuild the image.

    Drift in either direction is a silent failure: a path the workflow builds on
    but the gate ignores waves a stale image through, and the reverse blocks a
    valid run for a change that never triggered a build.
    """

    @pytest.fixture(scope="class")
    def trigger_paths(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("paths:", 1)[1].split("workflow_dispatch", 1)[0]
        return {p.strip("/*") for p in re.findall(r'-\s+"([^"]+)"', block)}

    @pytest.fixture(scope="class")
    def gate_paths(self):
        text = PRODUCTION_RUN.read_text(encoding="utf-8")
        block = text.split("git log -1 --format=%ct --", 1)[1].split("2>/dev/null", 1)[0]
        return {p.strip("/") for p in block.replace("\\", " ").split()}

    def test_gate_covers_every_build_trigger(self, trigger_paths, gate_paths):
        # The workflow file itself cannot change the image's contents, so the
        # gate has nothing to say about it.
        assert trigger_paths - gate_paths <= {".github/workflows/docker-publish.yml"}

    def test_gate_claims_nothing_extra(self, trigger_paths, gate_paths):
        assert gate_paths - trigger_paths == set()

    @pytest.mark.parametrize("path", BAKED)
    def test_baked_scripts_trigger_a_rebuild(self, path, trigger_paths):
        assert any(path == p or path.startswith(f"{p}/") for p in trigger_paths), (
            f"{path} ships in the image but no build trigger covers it"
        )


class TestProductionRunModes:
    @pytest.fixture(scope="class")
    def text(self):
        return PRODUCTION_RUN.read_text(encoding="utf-8")

    def test_unattended_does_not_launch(self, text):
        """It already runs on the host; launching would recurse into a second one."""
        prepared = text.index("sources prepared; user-data.sh takes it from here")
        launch = text.index('launch.sh" "$RUN_TAG"')
        assert prepared < launch
        assert "exit 0" in text[prepared : prepared + 200]

    def test_unattended_bounds_the_wait(self, text):
        assert "WAIT_MINUTES=60" in text
        assert "POLL_INTERVAL=10" in text
        assert "--wait-minutes" in text

    def test_unattended_excludes_itself_from_the_in_flight_check(self, text):
        assert "meta-data/instance-id" in text
        assert 'grep -vx "$SELF"' in text
