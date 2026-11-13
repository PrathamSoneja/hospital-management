"""
Release gate for GitOps manifests: fails while any deployable value is unresolved.

    python scripts/check_release_config.py [kustomize-dir ...]   # default: every path ArgoCD syncs in k8s/argocd

Template validation (kubeconform in CI) accepts placeholders so the templates stay
reviewable; this check is the separate pre-sync gate. It renders each directory with
`kubectl kustomize` (no cluster access) and rejects:
  * any PLACEHOLDER token;
  * images built from this repository (Allocation, Triage, Lock Coordinator) that are not <region>-docker.pkg.dev/<project>/<repo>/<name>@sha256:<64 hex>,
    or whose digest is all zeros;
  * mutable `:latest` tags on any container;
  * namespaces that are not valid DNS labels.
"""
import re
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIRS = ["k8s/overlays/staging", "k8s/jobs/allocation-migrate", "k8s/lock-coordinator/overlays/staging"]
OWN_IMAGES = {"bed-allocation", "triage", "lock-coordinator", "bed-registry", "resource-analytics"}
AR_DIGEST = re.compile(r"^[a-z0-9-]+-docker\.pkg\.dev/[a-z][a-z0-9-]{4,28}[a-z0-9]/[a-z0-9_-]+/"
                       r"(?P<name>[a-z0-9-]+)@sha256:(?P<digest>[0-9a-f]{64})$")
DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")


def _containers(document):
    spec = (document.get("spec") or {})
    template = spec.get("template") or {}
    pod = template.get("spec") or {}
    return (pod.get("containers") or []) + (pod.get("initContainers") or [])


def check_rendered(rendered: str, source: str = "") -> list:
    problems = []
    if "PLACEHOLDER" in rendered:
        tokens = sorted(set(re.findall(r"PLACEHOLDER_[A-Z0-9_]+", rendered)))
        problems.append(f"{source}: unresolved placeholders: {', '.join(tokens) or 'PLACEHOLDER'}")
    bad_namespaces = set()
    for document in yaml.safe_load_all(rendered):
        if not document:
            continue
        kind, name = document.get("kind"), (document.get("metadata") or {}).get("name")
        namespace = (document.get("metadata") or {}).get("namespace")
        if namespace and not DNS_LABEL.match(namespace):
            bad_namespaces.add(namespace)
        for container in _containers(document):
            image = container.get("image", "")
            if image.endswith(":latest") or (":" not in image.rsplit("/", 1)[-1] and "@" not in image):
                problems.append(f"{source}: {kind}/{name} container {container['name']} uses a mutable image reference")
            leaf = image.rsplit("/", 1)[-1].split("@")[0].split(":")[0]
            if leaf in OWN_IMAGES:
                match = AR_DIGEST.match(image)
                if not match:
                    problems.append(f"{source}: {kind}/{name} image for {leaf} is not an Artifact Registry digest reference")
                elif set(match["digest"]) == {"0"}:
                    problems.append(f"{source}: {kind}/{name} image for {leaf} has the all-zero template digest")
    problems += [f"{source}: namespace '{ns}' is not a DNS label" for ns in sorted(bad_namespaces)]
    return problems


def render(directory: str) -> str:
    result = subprocess.run(["kubectl", "kustomize", str(REPO_ROOT / directory)], capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"kustomize failed for {directory}: {result.stderr.strip()}")
    return result.stdout


def main(argv) -> int:
    problems = []
    for directory in argv or DEFAULT_DIRS:
        problems += check_rendered(render(directory), directory)
    for problem in problems:
        print(f"RELEASE BLOCKED: {problem}")
    if not problems:
        print("release configuration resolved: no placeholders, digest-pinned images")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
