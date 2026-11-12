"""
Runs the real Patient Registry app (services.patient_registry) for the local sandbox.

Only document storage is swapped for the in-memory store its own tests use: production needs a
GCS bucket, and documents are not on Allocation's path. Schema comes from the service models.
TEST-ONLY; production starts services.patient_registry.main.
"""
import os

import uvicorn

from services.patient_registry.main import create_app
from tests.test_patient_registry import MemoryDocumentStorage

app = create_app(document_storage=MemoryDocumentStorage(), create_schema=True)

if __name__ == "__main__":
    uvicorn.run(app, host=os.environ.get("PATIENT_REGISTRY_REST_HOST", "127.0.0.1"),
                port=int(os.environ["PATIENT_REGISTRY_REST_PORT"]), log_level="warning")
