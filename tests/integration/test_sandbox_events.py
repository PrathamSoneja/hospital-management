"""
Outbox delivery through the real Pub/Sub and Kafka adapters to LOCAL brokers
(Pub/Sub emulator, single-node KRaft Kafka from tests/sandbox/compose.yaml).

Guarantees asserted are the documented ones: at-least-once delivery, stable event_id and
per-(destination, pool) stream_sequence, independent destinations. Exactly-once delivery
and strict broker ordering under claim expiry are NOT asserted. The local brokers are test
infrastructure, not the production Kafka-on-GKE or Pub/Sub/Eventarc design.
"""
import json
import os
import subprocess
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jsonschema
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from services.bed_allocation.db import OutboxEventModel
from services.bed_allocation.outbox.dispatcher import OutboxDispatcher
from services.bed_allocation.outbox.publishers import BrokerEventPublisher, KafkaPublisher, PubSubPublisher
from services.bed_allocation.service import _next_outbox_sequence
from tests.sandbox.fixtures import seed_pool
from tests.sandbox.runtime import REPO_ROOT, Sandbox

CONTRACTS = REPO_ROOT / "contracts" / "events"
SCHEMAS = {"kafka": json.loads((CONTRACTS / "bed-event.schema.json").read_text()),
           "pubsub": json.loads((CONTRACTS / "notification.schema.json").read_text())}
COMPOSE = ["docker", "compose", "-f", str(REPO_ROOT / "tests" / "sandbox" / "compose.yaml")]


def consume_kafka(bootstrap, topic, pool_id=None, expected=0, timeout=60):
    from confluent_kafka import Consumer
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"it-{uuid.uuid4().hex}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])
    messages, deadline = [], time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                if expected and len(messages) >= expected:
                    break
                continue
            payload = json.loads(message.value())
            if pool_id is None or payload.get("pool_id") == pool_id:
                messages.append({"key": message.key().decode(), "payload": payload, "partition": message.partition()})
    finally:
        consumer.close()
    return messages


def pull_pubsub(subscription, pool_id=None, expected=0, timeout=60):
    from google.cloud import pubsub_v1
    subscriber = pubsub_v1.SubscriberClient()
    messages, deadline = [], time.monotonic() + timeout
    while time.monotonic() < deadline and not (expected and len(messages) >= expected):
        response = subscriber.pull(subscription=subscription, max_messages=100, timeout=5)
        if response.received_messages:
            subscriber.acknowledge(subscription=subscription,
                                   ack_ids=[m.ack_id for m in response.received_messages])
        for received in response.received_messages:
            payload = json.loads(received.message.data)
            if pool_id is None or payload.get("pool_id") == pool_id:
                messages.append({"attributes": dict(received.message.attributes), "payload": payload})
    return messages


def outbox_rows(sandbox, pool_id):
    engine = create_engine(sandbox.database_url)
    try:
        with engine.connect() as connection:
            return connection.execute(text(
                "SELECT event_id, destination, stream_sequence, status, attempt_count, last_error, available_at "
                "FROM outbox_events WHERE pool_id = :p ORDER BY destination, stream_sequence"), {"p": pool_id}).mappings().all()
    finally:
        engine.dispose()


def wait_for(predicate, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


def _assert_stream(messages, rows, destination, pool_id):
    unique = {}
    for message in messages:
        payload = message["payload"]
        jsonschema.validate(payload, SCHEMAS[destination])
        if destination == "kafka":
            assert message["key"] == pool_id  # Kafka partition key is the pool
        else:
            assert message["attributes"]["event_id"] == payload["event_id"]
            assert message["attributes"]["pool_id"] == pool_id
            assert message["attributes"]["stream_sequence"] == str(payload["stream_sequence"])
        previous = unique.setdefault(payload["event_id"], payload)
        assert previous["stream_sequence"] == payload["stream_sequence"]  # duplicates keep their identity
    expected = {r["event_id"]: r["stream_sequence"] for r in rows if r["destination"] == destination}
    assert {e: p["stream_sequence"] for e, p in unique.items()} == expected
    assert sorted(expected.values()) == list(range(1, len(expected) + 1))


def test_outbox_delivers_schema_valid_events_to_both_brokers(sandbox, api, pool):
    admissions = [f"{pool}-adm-{n}" for n in range(1, 5)]
    seed_pool(sandbox, pool, beds=2, admissions=admissions, levels={a: 1 + n for n, a in enumerate(admissions)})
    created = [api.create(a, pool, f"{a}-k", allocator=n % 2)[1] for n, a in enumerate(admissions)]
    api.release(created[0]["allocation_id"], f"{pool}-rel")  # -1 and a promotion +1
    wait_for(lambda: outbox_rows(sandbox, pool) and all(r["status"] == "DELIVERED" for r in outbox_rows(sandbox, pool)),
             60, "outbox drained")
    rows = outbox_rows(sandbox, pool)
    kafka = consume_kafka(sandbox.kafka_bootstrap, sandbox.topics["kafka"], pool,
                          expected=sum(r["destination"] == "kafka" for r in rows))
    pubsub = pull_pubsub(sandbox.pubsub_subscription, pool, expected=sum(r["destination"] == "pubsub" for r in rows))
    _assert_stream(kafka, rows, "kafka", pool)
    _assert_stream(pubsub, rows, "pubsub", pool)
    deltas = [m["payload"]["occupancy_delta"] for m in sorted(kafka, key=lambda m: m["payload"]["stream_sequence"])]
    assert sum(deltas) == 2 and deltas.count(-1) == 1
    waitlisted = [m["payload"] for m in pubsub if m["payload"]["event_type"] == "ALLOCATION_WAITLISTED"]
    assert waitlisted and all(p["ward"] is None for p in waitlisted)


def test_kafka_outage_backs_off_without_blocking_pubsub_then_drains(sandbox, api, pool):
    admissions = [f"{pool}-adm-{n}" for n in range(1, 4)]
    seed_pool(sandbox, pool, beds=3, admissions=admissions)
    subprocess.run(COMPOSE + ["pause", "kafka"], check=True, capture_output=True)
    try:
        for admission in admissions:
            api.create(admission, pool, f"{admission}-k")

        def pubsub_done_kafka_retrying():
            rows = outbox_rows(sandbox, pool)
            kafka = [r for r in rows if r["destination"] == "kafka"]
            pubsub = [r for r in rows if r["destination"] == "pubsub"]
            return (len(pubsub) == 3 and all(r["status"] == "DELIVERED" for r in pubsub)
                    and any(r["attempt_count"] >= 1 and r["last_error"] for r in kafka))
        wait_for(pubsub_done_kafka_retrying, 90, "Pub/Sub delivery while Kafka is down")
        kafka_rows = [r for r in outbox_rows(sandbox, pool) if r["destination"] == "kafka"]
        assert all(r["status"] != "DELIVERED" for r in kafka_rows)  # ordered stream waits behind its head
        head = kafka_rows[0]
        assert head["last_error"] in {"TimeoutError", "BrokerPublishError"}  # bounded, redacted error name
    finally:
        subprocess.run(COMPOSE + ["unpause", "kafka"], check=True, capture_output=True)
    wait_for(lambda: all(r["status"] == "DELIVERED" for r in outbox_rows(sandbox, pool)), 120, "Kafka backlog drained")
    rows = outbox_rows(sandbox, pool)
    _assert_stream(consume_kafka(sandbox.kafka_bootstrap, sandbox.topics["kafka"], pool, expected=3), rows, "kafka", pool)


@pytest.fixture
def isolated_outbox(sandbox):
    """A separate disposable database and topics, so allocator workers cannot claim these rows."""
    helper = Sandbox(postgres_url=sandbox.postgres_url, database="hospital_sandbox_outbox_test",
                     firestore_host=sandbox.firestore_host, log_dir=sandbox.log_dir / "outbox-crash", allocators=0,
                     pubsub_host=sandbox.pubsub_host, kafka_bootstrap=sandbox.kafka_bootstrap,
                     firestore_project=sandbox.firestore_project)
    helper.reset_database()
    helper.ensure_brokers()
    engine = create_engine(helper.database_url)
    yield helper, sessionmaker(bind=engine, autoflush=False, autocommit=False)
    engine.dispose()


def test_publish_then_crash_before_ack_redelivers_same_identity(isolated_outbox):
    helper, sessions = isolated_outbox
    pool_id = "P-CRASH"
    with sessions() as session:
        for destination, topic in (("kafka", "bed-events"), ("pubsub", "bed-notifications")):
            sequence = _next_outbox_sequence(session, destination, pool_id)
            event_id = str(uuid.uuid4())
            session.add(OutboxEventModel(
                event_id=event_id, destination=destination, pool_id=pool_id, stream_sequence=sequence, topic=topic,
                message_key=pool_id, available_at=datetime.now(timezone.utc) - timedelta(seconds=1),
                payload=json.dumps({"event_id": event_id, "pool_id": pool_id, "stream_sequence": sequence})))
        session.commit()
    publisher = BrokerEventPublisher(
        PubSubPublisher(helper.firestore_project, timeout_seconds=10),
        KafkaPublisher({"bootstrap.servers": helper.kafka_bootstrap}, timeout_seconds=10),
        {"bed-events": helper.topics["kafka"], "bed-notifications": helper.topics["pubsub"]})

    crashed = OutboxDispatcher(sessions, publisher, owner_id="crashed", claim_seconds=2)
    claims = crashed._claim(10)
    assert len(claims) == 2
    for claim in claims:
        assert crashed._publish(claim) is True  # broker acknowledged; the process dies before recording it
    time.sleep(2.5)  # the claim expires
    restarted = OutboxDispatcher(sessions, publisher, owner_id="restarted")
    assert restarted.process_pending() == 2
    for claim in claims:  # the crashed worker's late ACK cannot overwrite the newer claim
        assert crashed._finish_claim(claim, True, None) is False

    kafka = consume_kafka(helper.kafka_bootstrap, helper.topics["kafka"], expected=2, timeout=30)
    pubsub = pull_pubsub(helper.pubsub_subscription, expected=2, timeout=30)
    for messages in (kafka, pubsub):
        identities = [(m["payload"]["event_id"], m["payload"]["stream_sequence"]) for m in messages]
        assert len(identities) == 2 and len(set(identities)) == 1  # duplicate with identical identity
    with sessions() as session:
        assert {r.status for r in session.query(OutboxEventModel)} == {"DELIVERED"}
