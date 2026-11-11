"""Sandbox safety rails: no Google ADC fallback in child processes; only disposable local databases."""
import subprocess
import sys

import pytest

from tests.sandbox.runtime import REPO_ROOT, Sandbox, assert_disposable_database_url, isolated_env


def test_child_processes_cannot_find_google_credentials(tmp_path, monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "would-be-key.json"))
    env = isolated_env(tmp_path)
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in env and env["ENVIRONMENT"] == "local"
    probe = ("import google.auth, google.auth.exceptions\n"
             "try:\n    google.auth.default()\n    print('FOUND')\n"
             "except google.auth.exceptions.DefaultCredentialsError:\n    print('NONE')\n")
    result = subprocess.run([sys.executable, "-c", probe], cwd=REPO_ROOT, env=env, capture_output=True, text=True,
                            timeout=60)
    assert result.stdout.strip().splitlines()[-1] == "NONE", result.stdout + result.stderr


@pytest.mark.parametrize("url", [
    "postgresql+psycopg2://u@localhost:5432/hospital_allocation",           # original/shared database
    "postgresql+psycopg2://u@10.0.0.5:5432/allocations_test",                # not local
])
def test_non_disposable_databases_are_refused(url, tmp_path):
    with pytest.raises(ValueError):
        assert_disposable_database_url(url)
    with pytest.raises(ValueError):
        Sandbox(postgres_url=url, database="hospital_allocation", firestore_host="localhost:1", log_dir=tmp_path)
