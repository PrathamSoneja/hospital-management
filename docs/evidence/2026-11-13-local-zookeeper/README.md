# ZooKeeper failure demo — 2026-11-13 (local, docker compose)

Run on the 3-node ZooKeeper ensemble in `compose.yaml` with the real Lock Coordinator (REST on host port 8010),
using the demo helper `scripts/local/lock_demo.py` (called `lockdemo.py` in the screenshots). On GKE the same
ensemble runs as a 3-replica StatefulSet (`k8s/lock-coordinator/base/zookeeper.yaml`).

| File | Shows | Requirement |
|---|---|---|
| `01-three-nodes-one-leader.png` | `zkServer.sh status` on all three nodes: two followers, `zookeeper-3` is the leader | f (S7 concept) |
| `02-leader-stopped-new-leader-lock-granted.png` | Alice holds `WARDA-ICU` (token `4294967302`). The leader `zookeeper-3` is stopped, `zookeeper-2` is elected leader, `/readyz` stays `ready`, and Bob is granted `WARDB-GEN` with token `8589934595`: higher, because the new leader starts a new epoch | f |
| `03-quorum-lost-not-ready-acquire-504.png` | A second node is stopped (1 of 3 left, no quorum): `/readyz` returns `not_ready`, and Carol's Acquire fails with 504 `DEADLINE_EXCEEDED` after 6 s instead of being granted without a majority | f |

Not in a screenshot: after both nodes were restarted, a new Acquire succeeded; after stopping and starting all
three nodes, `ls /locks` still listed `[WARDA-ICU, WARDB-GEN, WARDB-HDU, WARDB-ISO]`, so the data persisted.
