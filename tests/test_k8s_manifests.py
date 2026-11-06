"""
Static consistency of the rendered GitOps manifests (no cluster access). These checks do not
prove deployment, scaling, or network enforcement on GKE.
"""
import shutil
import subprocess

import pytest
import yaml

from scripts.check_release_config import REPO_ROOT


def _render(directory):
    if not shutil.which("kubectl"):
        pytest.skip("kubectl is required to render kustomize directories")
    output = subprocess.run(["kubectl", "kustomize", str(REPO_ROOT / directory)], capture_output=True, text=True,
                            check=True).stdout
    return [d for d in yaml.safe_load_all(output) if d]


@pytest.fixture(scope="module")
def staging():
    return _render("k8s/overlays/staging")


def _by_kind(documents, kind):
    return {d["metadata"]["name"]: d for d in documents if d["kind"] == kind}


def test_hpa_owns_replicas_with_approved_ranges(staging):
    deployments, hpas = _by_kind(staging, "Deployment"), _by_kind(staging, "HorizontalPodAutoscaler")
    assert all("replicas" not in d["spec"] for d in deployments.values())  # never fight the HPA
    expected = {"bed-allocation-service": (2, 10), "triage-service": (2, 6)}
    for hpa in hpas.values():
        target = hpa["spec"]["scaleTargetRef"]["name"]
        assert target in deployments and hpa["apiVersion"] == "autoscaling/v2"
        assert (hpa["spec"]["minReplicas"], hpa["spec"]["maxReplicas"]) == expected.pop(target)
        [metric] = hpa["spec"]["metrics"]
        assert metric["resource"]["name"] == "cpu" and metric["resource"]["target"]["averageUtilization"] == 60
    assert expected == {}
    apps = list(yaml.safe_load_all((REPO_ROOT / "k8s/argocd/pratham-applications.yaml").read_text()))
    workload = next(a for a in apps if a["spec"]["source"]["path"] == "k8s/overlays/staging")
    assert {"group": "apps", "kind": "Deployment", "jsonPointers": ["/spec/replicas"]} in workload["spec"]["ignoreDifferences"]
    assert "RespectIgnoreDifferences=true" in workload["spec"]["syncPolicy"]["syncOptions"]
    assert all("automated" not in (a["spec"].get("syncPolicy") or {}) for a in apps)  # manual sync only


def test_containers_have_requests_probes_and_hardening(staging):
    for deployment in _by_kind(staging, "Deployment").values():
        pod = deployment["spec"]["template"]["spec"]
        assert pod["securityContext"]["runAsNonRoot"] is True and pod["automountServiceAccountToken"] is False
        for container in pod["containers"] + pod.get("initContainers", []):
            assert container["resources"]["requests"]["cpu"], container["name"]
            assert container["resources"]["limits"]["memory"], container["name"]
            assert container["securityContext"]["allowPrivilegeEscalation"] is False
        app = pod["containers"][0]
        assert app["livenessProbe"]["httpGet"]["path"] == "/healthz"   # process-only
        assert app["readinessProbe"]["httpGet"]["path"] == "/readyz"   # bounded dependency check
        assert app["readinessProbe"]["timeoutSeconds"] <= 5 and "startupProbe" in app


def test_secret_files_and_config_references_resolve(staging):
    config_maps = _by_kind(staging, "ConfigMap")
    providers = _by_kind(staging, "SecretProviderClass")
    for deployment in _by_kind(staging, "Deployment").values():
        pod = deployment["spec"]["template"]["spec"]
        volumes = {v["name"]: v for v in pod.get("volumes", [])}
        for container in pod["containers"] + pod.get("initContainers", []):
            for source in container.get("envFrom", []):
                assert source["configMapRef"]["name"] in config_maps
            for env in container.get("env", []):
                ref = (env.get("valueFrom") or {}).get("configMapKeyRef")
                if ref:
                    assert ref["key"] in config_maps[ref["name"]]["data"]
                if env["name"].endswith("_FILE"):
                    mount = next(m for m in container["volumeMounts"] if env["value"].startswith(m["mountPath"] + "/"))
                    provider = providers[volumes[mount["name"]]["csi"]["volumeAttributes"]["secretProviderClass"]]
                    paths = [s["path"] for s in yaml.safe_load(provider["spec"]["parameters"]["secrets"])]
                    assert env["value"].rsplit("/", 1)[1] in paths
    # No secret values or Kubernetes Secret objects are rendered into Git-managed manifests.
    assert not _by_kind(staging, "Secret")
    assert all("PASSWORD" not in key for cm in config_maps.values() for key in cm.get("data", {}))


def test_private_networking(staging):
    assert all(s["spec"]["type"] == "ClusterIP" for s in _by_kind(staging, "Service").values())
    policies = _by_kind(staging, "NetworkPolicy")
    triage = policies["triage-ingress"]["spec"]["ingress"]
    grpc_rule = next(rule for rule in triage if rule["ports"][0]["port"] == 50051)
    assert grpc_rule["from"] == [{"podSelector": {"matchLabels": {"app": "bed-allocation-service"}}}]


def test_migration_uses_separate_schema_owner_credential(staging):
    job_docs = _render("k8s/jobs/allocation-migrate")
    runtime = yaml.safe_load(_by_kind(staging, "SecretProviderClass")["allocation-secrets"]["spec"]["parameters"]["secrets"])
    migration = yaml.safe_load(_by_kind(job_docs, "SecretProviderClass")["allocation-migration-secrets"]["spec"]["parameters"]["secrets"])
    assert any("allocation-database-url" in s["resourceName"] for s in runtime)
    assert all("migration" not in s["resourceName"] for s in runtime)
    assert [s["resourceName"].split("/secrets/")[1].split("/")[0] for s in migration] == ["allocation-migration-database-url"]
    [job] = _by_kind(job_docs, "Job").values()
    assert job["spec"]["backoffLimit"] == 0
