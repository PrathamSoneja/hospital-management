"""The release gate rejects the placeholder templates and accepts a fully resolved rendering."""
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.check_release_config import DEFAULT_DIRS, REPO_ROOT, check_rendered, render

RESOLVED = {
    "PLACEHOLDER_K8S_NAMESPACE": "hospital",
    "PLACEHOLDER_REGION": "us-central1",
    "PLACEHOLDER_PROJECT_ID": "example-project-123",
    "PLACEHOLDER_AR_REPOSITORY": "hospital-repo",
    "PLACEHOLDER_NAME_PREFIX": "hospital",
    "PLACEHOLDER_SQL_INSTANCE": "hospital-sql",
    "PLACEHOLDER_REVISION": "20261031-03",
}


@pytest.fixture(scope="module")
def kubectl():
    if not shutil.which("kubectl"):
        pytest.skip("kubectl is required to render kustomize directories")


def test_templates_are_blocked(kubectl):
    problems = [p for d in DEFAULT_DIRS for p in check_rendered(render(d), d)]
    assert any("unresolved placeholders" in p for p in problems)
    assert any("not an Artifact Registry digest" in p for p in problems)  # placeholder registry path
    assert any("namespace 'PLACEHOLDER_K8S_NAMESPACE'" in p for p in problems)


def test_all_zero_digest_is_blocked_even_with_a_valid_registry():
    rendered = ("""kind: Deployment
metadata: {name: bed-allocation-service, namespace: hospital}
spec:
  template:
    spec:
      containers:
        - name: bed-allocation
          image: us-central1-docker.pkg.dev/example-project-123/hospital-repo/bed-allocation@sha256:""" + "0" * 64)
    assert check_rendered(rendered, "x") == ["x: Deployment/bed-allocation-service image for bed-allocation has the all-zero template digest"]


def test_resolved_copy_passes_and_tags_are_rejected(kubectl, tmp_path):
    shutil.copytree(REPO_ROOT / "k8s", tmp_path / "k8s")
    digest = "sha256:" + "ab" * 32
    for path in (tmp_path / "k8s").rglob("*"):
        if path.is_file() and path.suffix in {".yaml", ".properties"}:
            text = path.read_text(encoding="utf-8")
            for token in sorted(RESOLVED, key=len, reverse=True):
                text = text.replace(token, RESOLVED[token])
            text = text.replace("sha256:" + "0" * 64, digest)
            # Remaining PLACEHOLDER_* (teammate endpoints/topics) become fictional resolved values.
            text = __import__("re").sub(r"PLACEHOLDER_[A-Z0-9_]+", "resolved-value", text)
            path.write_text(text, encoding="utf-8")

    def rendered(directory):
        return subprocess.run(["kubectl", "kustomize", str(tmp_path / directory)], capture_output=True, text=True,
                              check=True).stdout

    assert [p for d in DEFAULT_DIRS for p in check_rendered(rendered(d), d)] == []
    tagged = rendered(DEFAULT_DIRS[0]).replace(f"bed-allocation@{digest}", "bed-allocation:latest")
    assert any("mutable image reference" in p for p in check_rendered(tagged, "staging"))
