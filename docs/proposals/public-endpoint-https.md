# Public web endpoint — proposal (separate from today's release; needs approval)

**Today:** Ingress `patient-registry-public` routes only `GET /healthz` of Patient Registry over HTTP. It proves
public reachability but shows no functionality, so the public-web-service requirement is **not** marked verified.

## 1. Meaningful read-only endpoint (prepared, not applied)

Patch: `docs/proposals/public-availability-endpoint.patch`. It applies cleanly, and the Bed Registry tests pass
20/20 with it applied.

- Adds `GET /api/v1/public/availability` to Bed Registry: per-pool counts
  `{pool_id, available, occupied, other, total}` only. It returns no bed, allocation, patient or staff
  identifiers, and only pools whose ID starts with `PUBLIC_AVAILABILITY_POOL_PREFIX` (default `DEMO-`, the
  synthetic demo pools).
- Adds Ingress `hospital-public` routing exactly that path to `bed-registry-service`. It is meant to
  **replace** the health-only Patient Registry Ingress, so there is still one load balancer: delete
  `k8s/overlays/gke/patient-registry/public-ingress.yaml` in the same change.
- Requires a Bed Registry image rebuild and push (approval), then ArgoCD sync.

Demo: `curl http://<LB-IP>/api/v1/public/availability` changes as beds are allocated and released in the demo UI.

## 2. HTTPS

Options, in order of preference:

| Option | Hostname | Certificate | Notes |
|---|---|---|---|
| A. Own domain (recommended) | e.g. `beds.<your-domain>` | Google-managed (`ManagedCertificate`) | Needs a domain you control (~$1–12/year) and an A record → static IP. Reliable. |
| B. `<ip>.nip.io` or `sslip.io` | wildcard DNS service | Google-managed | **Unverified**: issuance on shared public-suffix-style domains is not guaranteed (CAA and rate limits). Try it, and fall back to A if it stays `Provisioning`. |
| C. Self-signed | IP | Kubernetes TLS Secret | Works but browsers warn; acceptable only for a demo. |

Infrastructure changes for A/B, all in Git/IaC:
- Terraform: `google_compute_global_address` `hospital-public-ip`, a reserved static IP so DNS and the
  certificate stay valid across re-creation.
- GitOps: `ManagedCertificate` (networking.gke.io/v1) with the hostname; Ingress annotations
  `kubernetes.io/ingress.global-static-ip-name: hospital-public-ip` and
  `networking.gke.io/managed-certificates: hospital-public-cert`; and a `FrontendConfig` with
  `redirectToHttps`.
- Provisioning takes 15–60 min after DNS resolves to the IP.

Authentication for any **write** operation (not needed for the read-only endpoint): Identity-Aware Proxy on the
backend (`BackendConfig.iap`, OAuth consent screen, IAP-secured users). Keep writes private until that exists.

## 3. Cost (estimates; check the pricing page)

- External HTTP(S) load balancer forwarding rules: about $0.025/hour for the first 5 rules in the project, so
  **≈ $18/month (≈ $0.60/day)** whether one or two rules (HTTP + HTTPS) exist.
- Data processing: about $0.008/GB, negligible for a demo.
- Static IP while attached to a forwarding rule: no extra charge; unattached, it is billed hourly, so release it on cleanup.
- Managed certificate: free.

Cleanup (ask first): delete the Ingress in Git (ArgoCD prunes the load balancer), then release the static IP
through Terraform.
