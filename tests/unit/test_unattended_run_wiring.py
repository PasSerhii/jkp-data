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

import base64
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
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


@pytest.fixture(scope="module")
def bash() -> str:
    git_bash = Path("C:/Program Files/Git/bin/bash.exe")
    executable = str(git_bash) if git_bash.exists() else shutil.which("bash")
    if not executable:
        pytest.skip("Bash is required")
    return executable


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

    def test_only_one_branch_publishes(self):
        """:production is what the scheduled run pulls; a second branch would overwrite it."""
        text = WORKFLOW.read_text(encoding="utf-8")
        assert re.findall(r"branches: \[([^\]]*)\]", text) == ["compustat_migration"]
        assert "if: github.ref == 'refs/heads/compustat_migration'" in text


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


class TestHostRetirement:
    """An unattended run has nobody to terminate the host, so it must retire itself."""

    @pytest.fixture(scope="class")
    def user_data(self):
        return USER_DATA.read_text(encoding="utf-8")

    def test_shutdown_terminates_the_instance(self):
        launch = LAUNCH.read_text(encoding="utf-8")
        call = launch[launch.index("aws ec2 run-instances") :]
        call = call[: call.index('--query "Instances[0].InstanceId"')]
        assert "--instance-initiated-shutdown-behavior terminate" in call

    def test_success_shuts_down_after_everything_is_in_s3(self, user_data):
        upload = user_data.index('aws s3 sync "$CSV_DIR" "$RUN_PREFIX/production/"')
        done = user_data.index("touch /mnt/jkp-data/DONE")
        success = user_data.index('if [ "$RC" -eq 0 ]; then')
        failure = user_data.index("else", success)
        shutdown = user_data.index("shutdown -h now")
        assert upload < done < success < shutdown < failure
        assert user_data.index("ship_setup_log", done) < shutdown

    def test_failures_keep_the_host_for_five_hours(self, user_data):
        assert "DIAGNOSIS_HOURS=5" in user_data
        fail_body = user_data[user_data.index("fail() {") :]
        fail_body = fail_body[: fail_body.index("\n}\n")]
        assert "retire_later" in fail_body and "ship_setup_log" in fail_body
        success = user_data.index('if [ "$RC" -eq 0 ]; then')
        failure_branch = user_data[user_data.index("else", success) :]
        assert "retire_later" in failure_branch and "shutdown -h now" not in failure_branch

    def test_retire_later_schedules_the_delay_in_minutes(self, user_data, tmp_path, bash):
        """Runs the real function with `shutdown` stubbed: +300 minutes, cancellable."""
        func = user_data[user_data.index("DIAGNOSIS_HOURS=") :]
        func = func[: func.index("\n}\n") + 3]
        record = tmp_path / "shutdown_args"
        script = (
            f'shutdown() {{ printf "%s\n" "$@" > "{record.as_posix()}"; }}\n{func}\nretire_later\n'
        )
        out = subprocess.run([bash, "-c", script], capture_output=True, text=True, check=True)
        assert record.read_text().splitlines()[:2] == ["-h", "+300"]
        assert out.stdout.strip().endswith("UTC")


FAKE_AWS = r"""#!/usr/bin/env bash
# Stand-in for the aws CLI: records every call, answers what launch.sh reads,
# keeps the launch template data it is handed, and refuses to start a host.
printf '%s\n' "$*" >> "$FAKE_AWS_LOG"
case "$1 $2" in
  "ssm get-parameter")
    case "$*" in *--with-decryption*)
      echo 'RESEARCH_UPDATE=mssql+pyodbc://u:p@h/research?driver=ODBC+Driver+18+for+SQL+Server' ;;
    esac ;;
  "ec2 describe-security-groups") echo sg-0fake ;;
  "ec2 describe-launch-templates") exit 254 ;;
  "ec2 create-launch-template")
    for a in "$@"; do case "$a" in file://*) cp "${a#file://}" "$FAKE_LT_DATA" ;; esac; done
    echo 1 ;;
  "ec2 run-instances") echo "run-instances must not be called" >&2; exit 1 ;;
esac
exit 0
"""


class TestLaunchTemplate:
    """``launch.sh --template`` writes the scheduled run; the host resolves the month."""

    def test_host_resolves_auto_run_tag_and_month_end(self, bash):
        text = USER_DATA.read_text(encoding="utf-8")
        block = text[text.index('RUN_TAG="@@RUN_TAG@@"') : text.index("COUNTRIES=")]
        block = block.replace("@@RUN_TAG@@", "auto").replace("@@END_DATE@@", "auto")
        script = "BUCKET=b\n" + block + 'printf "%s %s %s\n" "$RUN_TAG" "$END_DATE" "$RUN_PREFIX"\n'
        out = subprocess.run([bash, "-c", script], capture_output=True, text=True, check=True)
        today = datetime.now(UTC).date()
        month_end = today.replace(day=1) - timedelta(days=1)
        tag = f"prod-{today:%Y%m%d}"
        assert out.stdout.split() == [tag, f"{month_end:%Y-%m-%d}", f"s3://b/{tag}"]

    def test_explicit_run_tag_and_month_end_are_kept(self, bash):
        text = USER_DATA.read_text(encoding="utf-8")
        block = text[text.index('RUN_TAG="@@RUN_TAG@@"') : text.index("COUNTRIES=")]
        block = block.replace("@@RUN_TAG@@", "full-x").replace("@@END_DATE@@", "2026-09-30")
        script = "BUCKET=b\n" + block + 'printf "%s %s\n" "$RUN_TAG" "$END_DATE"\n'
        out = subprocess.run([bash, "-c", script], capture_output=True, text=True, check=True)
        assert out.stdout.split() == ["full-x", "2026-09-30"]

    @pytest.fixture(scope="class")
    def template(self, bash, tmp_path_factory):
        """Runs the real launcher in --template mode against the fake aws CLI."""
        work = tmp_path_factory.mktemp("lt")
        (work / "aws").write_text(FAKE_AWS, encoding="utf-8", newline="\n")
        (work / "aws").chmod(0o755)
        env = dict(os.environ)
        env.update(
            PATH=str(work) + os.pathsep + env["PATH"],
            FAKE_AWS_LOG=str(work / "calls.log"),
            FAKE_LT_DATA=str(work / "lt.json"),
            MSYS_NO_PATHCONV="1",
            # A template must ignore all of these.
            MARKET="spot",
            FULL_HISTORY="1",
            PRODUCTION_YEARS="0",
            DB_REPLACE="1",
            COUNTRIES="usa",
            UNATTENDED="0",
        )
        result = subprocess.run(
            [bash, LAUNCH.as_posix(), "--template"], env=env, capture_output=True, text=True
        )
        assert result.returncode == 0, result.stdout + result.stderr
        data = json.loads((work / "lt.json").read_text(encoding="utf-8"))
        user_data = base64.b64decode(data["UserData"]).decode("utf-8")
        calls = (work / "calls.log").read_text(encoding="utf-8")
        return data, user_data, calls, result.stdout

    def test_template_matches_a_manual_launch(self, template):
        data, _, _, _ = template
        assert data["InstanceType"] == "r7i.16xlarge"
        assert data["ImageId"].startswith("resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023")
        assert data["IamInstanceProfile"] == {"Name": "jkp-data-run-profile"}
        assert data["NetworkInterfaces"][0]["SubnetId"] == "subnet-0a74f7b1e230a2f12"
        assert data["NetworkInterfaces"][0]["Groups"] == ["sg-0fake"]
        ebs = data["BlockDeviceMappings"][0]["Ebs"]
        assert (ebs["VolumeSize"], ebs["Iops"], ebs["Throughput"]) == (750, 8000, 1000)
        assert ebs["DeleteOnTermination"] is True and ebs["Encrypted"] is True
        assert data["InstanceInitiatedShutdownBehavior"] == "terminate"
        assert data["MetadataOptions"]["HttpTokens"] == "required"
        tags = {t["Key"]: t["Value"] for t in data["TagSpecifications"][0]["Tags"]}
        assert tags["Purpose"] == "jkp-data-production"

    def test_template_is_always_the_monthly_delivery(self, template):
        _, user_data, _, _ = template
        assert "@@" not in user_data
        for line in (
            'RUN_TAG="auto"',
            'END_DATE="auto"',
            'UNATTENDED="1"',
            'MARKET="ondemand"',
            'DB_UPDATE="1"',
            'DB_REPLACE="0"',
            'FULL_HISTORY="0"',
            'PRODUCTION_YEARS=""',
            'COUNTRIES=""',
            'PARAM="/jkp-data/production/env"',
            'PARAM_EPHEMERAL="0"',
        ):
            assert line in user_data, line

    def test_template_tells_alpha_lab_after_a_full_database_update(self, template):
        _, user_data, _, _ = template
        assert "aws events put-events" in user_data
        assert "alpha.jkp-data" in user_data and "dataset.updated" in user_data
        assert "characteristicsproduction" in user_data
        assert "--query FailedEntryCount" in user_data
        gate = user_data.index('if [ -n "$DB_FLAG" ] && [ -z "$COUNTRIES" ]; then')
        success = user_data.index('if [ "$RC" -eq 0 ]; then')
        assert success < gate < user_data.index("publish_dataset_updated\n  fi")
        assert user_data.index("publish_dataset_updated\n  fi") < user_data.index("shutdown -h now")

    def test_template_user_data_fits_the_ec2_limit(self, template):
        _, user_data, _, _ = template
        assert len(user_data.encode("utf-8")) <= 16384

    def test_the_role_may_publish_to_the_default_bus_only(self):
        launch = LAUNCH.read_text(encoding="utf-8")
        assert (
            'events:PutEvents\\",\\"Resource\\":\\"arn:aws:events:$REGION:$ACCOUNT:event-bus/default'
            in launch
        )

    def test_template_starts_no_host_and_stages_no_secret(self, template):
        _, _, calls, _ = template
        assert "ec2 create-launch-template" in calls
        assert "run-instances" not in calls
        assert "ssm put-parameter" not in calls and "ssm delete-parameter" not in calls
