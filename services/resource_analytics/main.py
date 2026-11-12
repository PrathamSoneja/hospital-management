"""
Resource Analytics Application Entrypoint.
Owned by Member D (Jay Patel).
"""
import os
import threading
import json
import logging
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI

from services.resource_analytics.models import BedEvent
from services.resource_analytics.storage import create_storage, BaseAnalyticsStorage
from services.resource_analytics.aggregator import StreamAggregatorEngine
from services.resource_analytics.rest import router as analytics_router, init_services
from shared.observability.errors import register_error_handlers
from shared.observability.logging import setup_logger
from shared.observability.middleware import setup_observability_middleware

logger = setup_logger("resource-analytics-service")

def start_kafka_consumer_thread(storage: BaseAnalyticsStorage, aggregator: StreamAggregatorEngine, stop_event: threading.Event):
    """Background Kafka consumer thread for topic 'bed-events' or 'bed-stats'."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        logger.info("KAFKA_BOOTSTRAP_SERVERS not set; skipping live Kafka consumer.")
        return

    try:
        from confluent_kafka import Consumer
        conf = {
            'bootstrap.servers': bootstrap,
            'group.id': 'resource-analytics-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        # Add SASL/SSL credentials if configured
        sasl_user = os.environ.get("KAFKA_SASL_USERNAME", "")
        sasl_pass = os.environ.get("KAFKA_SASL_PASSWORD", "")
        if sasl_user and sasl_pass:
            conf['security.protocol'] = 'SASL_SSL'
            conf['sasl.mechanisms'] = 'PLAIN'
            conf['sasl.username'] = sasl_user
            conf['sasl.password'] = sasl_pass

        consumer = Consumer(conf)
        topic = os.environ.get("KAFKA_INPUT_TOPIC", "bed-events")
        consumer.subscribe([topic])
        logger.info(f"Subscribed to Kafka topic: {topic}")

        while not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka consumer error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                event = BedEvent.model_validate(payload)
                closed = aggregator.process_event(event)
                for stats in closed:
                    storage.save_window_stats(stats)
            except Exception as e:
                logger.error(f"Failed to process Kafka message: {e}")

        consumer.close()
        logger.info("Kafka consumer stopped gracefully.")
    except Exception as e:
        logger.warning(f"Could not start Kafka consumer: {e}")

def build_app(storage_backend: str | None = None) -> FastAPI:
    if storage_backend:
        os.environ["ANALYTICS_STORAGE_BACKEND"] = storage_backend

    storage = create_storage()
    aggregator = StreamAggregatorEngine()
    init_services(storage, aggregator)

    stop_event = threading.Event()
    consumer_thread = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal consumer_thread
        consumer_thread = threading.Thread(
            target=start_kafka_consumer_thread,
            args=(storage, aggregator, stop_event),
            daemon=True
        )
        consumer_thread.start()
        yield
        stop_event.set()
        if consumer_thread and consumer_thread.is_alive():
            consumer_thread.join(timeout=2.0)

    app = FastAPI(
        title="Hospital Resource Analytics Service",
        version="1.0.0",
        lifespan=lifespan
    )

    register_error_handlers(app, "resource-analytics-service")
    setup_observability_middleware(app, "resource-analytics-service", ready_check=storage.check_ready)
    app.include_router(analytics_router)
    return app

app = build_app()

if __name__ == "__main__":
    port = int(os.environ.get("ANALYTICS_REST_PORT", 8008))
    host = os.environ.get("ANALYTICS_REST_HOST", "0.0.0.0")
    uvicorn.run("services.resource_analytics.main:app", host=host, port=port, reload=False)

