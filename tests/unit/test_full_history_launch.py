"""Exercise EC2 history arguments in Bash without invoking AWS or Docker."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[2]
LAUNCH = ROOT / "scripts/aws/launch.sh"
USER_DATA = ROOT / "scripts/aws/user-data.sh"


@pytest.fixture(scope="module")
def bash() -> str:
    git_bash = Path("C:/Program Files/Git/bin/bash.exe")
    executable = str(git_bash) if git_bash.exists() else shutil.which("bash")
    if not executable:
        pytest.skip("Bash is required for EC2 launcher tests")
    return executable


def run_bash(bash: str, script: str, overrides: dict[str, str]) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("DB_UPDATE", None)
    env.update(
        FULL_HISTORY="0",
        START_DATE="",
        PRODUCTION_YEARS="",
        END_DATE="2026-08-31",
        MSYS_NO_PATHCONV="1",
    )
    env.update(overrides)
    return subprocess.run(
        [bash, "-c", script], env=env, capture_output=True, text=True, check=False
    )


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, []),
        ({"START_DATE": "2003-08-01"}, ["--start-date", "2003-08-01"]),
        ({"PRODUCTION_YEARS": "5"}, ["--production-years", "5"]),
        (
            {"FULL_HISTORY": "1", "PRODUCTION_YEARS": "0"},
            ["--full-history", "--production-years", "0"],
        ),
    ],
)
def test_container_receives_history_options(bash, overrides, expected):
    source = USER_DATA.read_text(encoding="utf-8")
    history = "HISTORY_ARGS=()" + source.split("HISTORY_ARGS=()", 1)[1].split("START_EPOCH=", 1)[0]
    command = (
        "docker run --name jkp-run"
        + source.split("docker run --name jkp-run", 1)[1].split("> /mnt/jkp-data/container.log", 1)[
            0
        ]
    )
    script = (
        "set -eu\nIMAGE=test-image WORKERS= KEEP_FLAG= DB_FLAG=\n"
        'docker() { printf "%s\\n" "$@"; }\n' + history + command + "\n"
    )
    result = run_bash(bash, script, overrides)
    assert result.returncode == 0, result.stderr
    args = result.stdout.splitlines()
    start = args.index("--persistent-connection") + 1
    assert args[start : args.index("--end-date")] == expected
    assert "build" in args and "/data" in args


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"FULL_HISTORY": "yes"}, "FULL_HISTORY must be 0 or 1"),
        (
            {"FULL_HISTORY": "1", "START_DATE": "2003-08-01"},
            "cannot be combined with START_DATE",
        ),
        ({"PRODUCTION_YEARS": "-1"}, "PRODUCTION_YEARS must be a non-negative integer"),
        ({"PRODUCTION_YEARS": "0; echo bad"}, "PRODUCTION_YEARS must be a non-negative integer"),
    ],
)
def test_invalid_history_request_stops_before_aws(bash, overrides, message):
    # Execute the actual launcher prefix, stopping before any external action.
    script = LAUNCH.read_text(encoding="utf-8").split("\nHERE=", 1)[0]
    result = run_bash(bash, script + '\nprintf "VALIDATION_PASSED\\n"\n', overrides)
    assert result.returncode != 0
    assert message in result.stderr
    assert "VALIDATION_PASSED" not in result.stdout


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, "1"),
        ({"FULL_HISTORY": "1", "PRODUCTION_YEARS": "0"}, "0"),
        ({"FULL_HISTORY": "1", "DB_UPDATE": "1"}, "1"),
    ],
)
def test_full_history_defaults_to_no_incremental_upload(bash, overrides, expected):
    script = LAUNCH.read_text(encoding="utf-8").split("\nHERE=", 1)[0]
    result = run_bash(bash, script + '\nprintf "%s\\n" "$DB_UPDATE"\n', overrides)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected
