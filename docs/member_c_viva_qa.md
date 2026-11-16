# Member C (Nisarg Vagh — 2026H1030066P) Viva Q&A & Architecture Defense Guide

My two tasks: **(f)** the ZooKeeper Lock Coordinator, and **(b)** Notification as a serverless function. The demo steps are in `docs/member_c_demo_runbook.md`.

---

## 1. ZooKeeper and the Lock Coordinator (task f)

### Q1: What problem does the Lock Coordinator solve?
> "Two allocation requests for the last ICU bed can run at the same moment. Both read 'free' and both assign it. The Lock Coordinator gives **one key per pool** (e.g. `WARDA-ICU`), so requests for a pool take turns. Different pools don't block each other. Bed Allocation and Triage call it over gRPC (`contracts/proto/lock.proto`): `Acquire`, `Renew`, `Release`, `Validate`."

### Q2: How is the lock built on ZooKeeper? Did you use a library recipe?
> "No recipe. It's hand-written on raw znodes in `zk_store.py`, using kazoo only for the connection.
> 1. Each request creates an **ephemeral sequential** znode, `/locks/WARDA-ICU/lock-0000000042`, holding owner, `acquire_id` and lease data.
> 2. The **lowest sequence number** holds the lock.
> 3. Everyone else watches **only the znode directly ahead** of them.
> 4. Release deletes the znode, which fires exactly one watch.
>
> Ephemeral means it disappears if my service's session dies. Sequential gives a fair, numbered queue."

### Q3: What is the herd effect, and how do you avoid it?
> "If every waiter watched the lock holder, each release would wake all 50 waiters, and all of them would hit ZooKeeper at once to check who's next. Instead, each waiter watches its predecessor, so one release wakes one waiter. After a watch fires I re-read the queue, because the one ahead may have left while someone further ahead still holds the key."

### Q4: What is a fencing token and why is it the `czxid`?
> "A lock alone doesn't stop a **stale holder**. A process can freeze in a GC pause, lose its lease, wake up and still write. Every grant carries a fencing token, and downstream services (Bed Registry, Triage) remember the highest token per pool and reject lower ones.
>
> I use the znode's `czxid`, the ZooKeeper transaction ID that created it. ZooKeeper assigns it, it only ever increases, and a lower queue position always has a lower `czxid`. It's (election epoch × 2³²) + a counter, so a leader change makes it jump. In my failure demo it went `4294967302` → `8589934595` → `12884901896`. It still increases, which is all fencing needs."

### Q5: Why do you need a lease if ephemeral znodes clean themselves up?
> "They cover different failures. If **my service** dies, its ZooKeeper session expires and its ephemeral znodes vanish. But if the **caller** (an allocator) freezes while my service stays healthy, the session is fine and the znode stays forever. So each grant has a lease (`lease_ms`, chosen by the caller: 5 s for the expiry demo, 10 s in staging). A **reaper** in each replica deletes expired locks every 0.5 s. The lease starts when the lock is **granted**, not when the request joins the queue, so a waiter is never 'expired'."

### Q6: How do you stop the reaper deleting a lease that was just renewed?
> "**Compare-and-swap.** The reaper reads the znode (version 3), sees it expired, and deletes with `version=3`. If a `Renew` landed in between, the znode is version 4, so ZooKeeper refuses the delete with `BadVersionError` and the live lease survives. Re-reading just before deleting would only shrink the gap. ZooKeeper's version check makes check-and-delete one indivisible step. The test `test_a_renew_beats_a_reaper_holding_an_old_read` proves it."

### Q7: You run 2 replicas. How do they agree?
> "All lock state lives **in ZooKeeper**, never in Python memory, so any replica can serve any call. `lock_id` contains the pool (`WARDA-ICU/lock-0000000042`), because `Renew` and `Release` don't send a pool, so any replica can find the znode from `lock_id` alone. Both replicas run reapers safely, because every delete is version-checked."

### Q8: What if a retried `Acquire` arrives at both replicas at the same instant?
> "Each request carries an `acquire_id` for idempotency. Joining the queue also creates a **claim** znode, `/acquires/<pool>/<hash(owner, acquire_id)>`, in the **same ZooKeeper transaction**: all or nothing. ZooKeeper refuses a second claim with the same name, so the loser finds the winner's place in line and returns the **same lease**. A test fires the same retry at two replicas 10 times and always finds exactly one queue entry. Before this fix, that test failed."

### Q9: Is `Release` safe to retry?
> "Yes. Release deletes the lock and writes a **tombstone** (who released it) in one transaction. If the reply is lost and the allocator retries, the tombstone makes the retry return `true`, so Allocation doesn't get stuck in RECOVERING. If the lease **expired and was reaped**, there's no tombstone and Release returns `false`, which tells Allocation it lost the lease. Tombstones are removed after 10 minutes. Per `docs/contracts.md`, `false` means 'not confirmed / lease not held', and `true` is not proof the lease stayed valid. Fencing tokens are."

### Q10: What happens when ZooKeeper nodes fail?
> "It's 3 nodes with quorum 2 (ZAB protocol).
> - **One down, even the leader:** the other two elect a new leader in seconds and keep accepting writes. My service's session moves to a surviving node before it times out, so existing locks survive.
> - **Two down:** one node can't form a majority. If it accepted writes alone, a network split could hand the same lock out twice (**split brain**). So ZooKeeper refuses. My `/readyz` returns 503 so Kubernetes stops routing calls, and Acquire returns 504.
> - **Restart:** data is on disk (a PersistentVolume per pod in Kubernetes), so `/locks` survives even a full 3-node restart.
>
> I demonstrated all of this with screenshots."

### Q11: Why a StatefulSet for ZooKeeper?
> "Each node needs a **stable identity and its own disk**. A StatefulSet gives pods fixed names (`zookeeper-0/1/2`), stable DNS through a headless Service, and a PersistentVolumeClaim each. The start command derives `ZOO_MY_ID` from the pod name's number + 1. `publishNotReadyAddresses` lets nodes find each other before any is 'ready'. The PodDisruptionBudget (`maxUnavailable: 1`) means maintenance can never take down two nodes and break quorum."

### Q12: What happens when a Lock Coordinator pod is stopped?
> "Kubernetes sends SIGTERM first. My server stops and **closes its ZooKeeper session**, so its ephemeral znodes are deleted immediately. If the pod is killed hard instead, ZooKeeper deletes them when the session times out, about 10 s later. Either way, no lock is held by a dead pod."

---

## 2. Notification as a serverless function (task b)

### Q13: Why is Notification a Cloud Run *function* and not a service?
> "The brief asks for a **serverless function** (e.g. Google Cloud Functions). Cloud Run functions, 2nd gen, is what Google Cloud Functions is now. It's deployed from **source code** with an entry point (`handle_bed_notification`) and an Eventarc trigger. There's no container image or server for us to manage, and it scales from 0 to 3 instances on demand. The same Python logic also runs as a FastAPI app for local tests."

### Q14: Walk through one event end to end.
> "Bed Allocation writes the event to its **outbox** in the same database transaction as the bed change, then publishes to Pub/Sub topic `bed-notifications`, with attributes `event_id`, `pool_id` and `stream_sequence`. Eventarc wraps the message in a **CloudEvent** and invokes my function. The function:
> 1. decodes the base64 payload;
> 2. validates it against `contracts/events/notification.schema.json`;
> 3. records it in Firestore;
> 4. logs a staff message labelled by `pool_id`, e.g. `[WARDA-ICU] Bed bed-warda-icu-01 in ward ICU-A allocated…`. `ward` is `null` for waitlisted events, so I never guess a ward."

### Q15: Pub/Sub is at-least-once. How do you avoid notifying staff twice?
> "Deduplication on `event_id`, in **Firestore**, inside **one transaction**: 'is this ID saved? if not, save it.' Firestore rather than a Python set, because up to 3 function instances run at once and a redelivery can land on a different one. A transaction rather than two separate calls, because otherwise two instances could both see 'not saved' and both alert. A test proves it with two separate instances."

### Q16: What do you do with missing or out-of-order messages?
> "I keep the highest `stream_sequence` per pool in `notification_streams`. A jump, e.g. 1 then 4, is logged as `notification_gap` with 2 missing, and counted. A lower number arriving late is **still delivered**, because the contract says never drop lower sequences. The first message for a pool sets the baseline, so it's never a false gap."

### Q17: How are failures and retries handled? Where's the DLQ?
> "There are two kinds of failure:
> - **Permanent** (malformed JSON, schema violation): retrying can never fix it, so the function logs `notification_rejected` and returns normally. Pub/Sub acknowledges it, and it's never retried.
> - **Temporary** (Firestore unavailable): the function **raises**, so Pub/Sub retries with backoff.
>
> After **10 failed attempts** the message moves to the dead-letter topic `bed-notifications-dlq`. Eventarc has no dead-letter setting of its own, so the policy lives on the Pub/Sub subscription Eventarc creates. Terraform **imports** that subscription to manage it, and grants the Pub/Sub service agent publisher rights on the DLQ and subscriber rights on the source subscription."

### Q18: How is it deployed? Is it all Infrastructure as Code?
> "Yes, Terraform in `modules/notifications`:
> - `archive_file` zips exactly the files the function imports, plus a root `main.py` and `requirements.txt`;
> - the zip is uploaded to a private GCS bucket, and its name includes the code's hash, so a code change redeploys;
> - `google_cloudfunctions2_function` (python311) builds with its own build account (`roles/run.builder`) and runs as the notification service account (`roles/datastore.user`);
> - its `event_trigger` subscribes to `bed-notifications` with retries;
> - Eventarc's account gets Cloud Run Invoker, because 2nd gen functions run on Cloud Run.
>
> The only manual step is a second apply to import the trigger's subscription for the DLQ."

---

## 3. Quick facts

| Thing | Value |
|---|---|
| Lock gRPC / REST ports | 50051 / 8000 (8010 on the host in compose) |
| ZooKeeper | 3 nodes, quorum 2, client 2181, metrics 7000 |
| Lock reaper interval | 0.5 s; tombstone window 10 min; abandoned-waiter grace 5 s |
| Notification | `hospital-notification-fn`, python311, 0–3 instances, `asia-south1` |
| DLQ | `bed-notifications-dlq` after 10 delivery attempts |
| Tests | Lock Coordinator 26; Notification 21 (14 service + 5 function + 2 Pub/Sub end to end) |
| Lock logs | JSON: `lock_granted` (with token and wait time), `lock_wait_timeout`, `lock_released`, `lock_release_not_confirmed`, `lock_renew_refused` |
