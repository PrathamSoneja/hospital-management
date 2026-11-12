"""
Firestore storage adapter for Resource Analytics.
Owned by Member D (Jay Patel) - CS/SS G527 Task (g).

Persists 1-minute windowed analytics statistics in Google Cloud Firestore:
- Collection: pool_stats
- Documents: {pool_id}_{window_start} for historical trend analysis
- Document: {pool_id}_latest for real-time dashboard lookups
"""
import os
import logging
from typing import Dict, List, Optional
from services.resource_analytics.models import BedStats

logger = logging.getLogger("resource-analytics-storage")

class BaseAnalyticsStorage:
    def save_window_stats(self, stats: BedStats):
        raise NotImplementedError

    def get_latest_stats(self, pool_id: str) -> Optional[BedStats]:
        raise NotImplementedError

    def get_stats_history(self, pool_id: str, limit: int = 60) -> List[BedStats]:
        raise NotImplementedError

    def get_all_pools_latest(self) -> List[BedStats]:
        raise NotImplementedError

    def check_ready(self) -> bool:
        return True

class InMemoryAnalyticsStorage(BaseAnalyticsStorage):
    """Hermetic in-memory storage used for local testing and unit tests."""
    def __init__(self):
        # pool_id -> list of BedStats
        self.history: Dict[str, List[BedStats]] = {}
        # pool_id -> latest BedStats
        self.latest: Dict[str, BedStats] = {}

    def save_window_stats(self, stats: BedStats):
        if stats.pool_id not in self.history:
            self.history[stats.pool_id] = []
        self.history[stats.pool_id].append(stats)
        self.latest[stats.pool_id] = stats

    def get_latest_stats(self, pool_id: str) -> Optional[BedStats]:
        return self.latest.get(pool_id)

    def get_stats_history(self, pool_id: str, limit: int = 60) -> List[BedStats]:
        items = self.history.get(pool_id, [])
        return items[-limit:]

    def get_all_pools_latest(self) -> List[BedStats]:
        return list(self.latest.values())

    def check_ready(self) -> bool:
        return True

class FirestoreAnalyticsStorage(BaseAnalyticsStorage):
    """Production Firestore storage for GCP Cloud Firestore and local Firestore emulator."""
    def __init__(self, project_id: str | None = None):
        from google.cloud import firestore
        self.project_id = project_id or os.environ.get("FIRESTORE_PROJECT_ID", "hospital-bed-mgmt-dev")
        self.client = firestore.Client(project=self.project_id)
        self.collection = self.client.collection("pool_stats")
        logger.info(f"Initialized FirestoreAnalyticsStorage (project={self.project_id})")

    def _doc_id(self, pool_id: str, window_start: str) -> str:
        safe_time = window_start.replace(":", "-").replace("+", "_")
        return f"{pool_id}_{safe_time}"

    def save_window_stats(self, stats: BedStats):
        doc_id = self._doc_id(stats.pool_id, stats.window_start)
        data = stats.model_dump()

        # Save historical point
        self.collection.document(doc_id).set(data)
        # Update latest pointer document
        self.collection.document(f"{stats.pool_id}_latest").set(data)
        logger.info(f"Saved window stats to Firestore: {doc_id}")

    def get_latest_stats(self, pool_id: str) -> Optional[BedStats]:
        doc = self.collection.document(f"{pool_id}_latest").get()
        if doc.exists:
            return BedStats.model_validate(doc.to_dict())
        return None

    def get_stats_history(self, pool_id: str, limit: int = 60) -> List[BedStats]:
        # Query where pool_id == pool_id ordered by window_start descending
        query = (
            self.collection
            .where("pool_id", "==", pool_id)
            .order_by("window_start")
            .limit(limit)
        )
        docs = query.stream()
        results = [BedStats.model_validate(d.to_dict()) for d in docs]
        return results

    def get_all_pools_latest(self) -> List[BedStats]:
        # Stream all documents ending with _latest
        docs = self.collection.stream()
        results = []
        for d in docs:
            if d.id.endswith("_latest"):
                results.append(BedStats.model_validate(d.to_dict()))
        return results

    def check_ready(self) -> bool:
        try:
            self.collection.limit(1).get()
            return True
        except Exception:
            return False

def create_storage() -> BaseAnalyticsStorage:
    backend = os.environ.get("ANALYTICS_STORAGE_BACKEND", "").strip().lower()
    if backend == "memory" or os.environ.get("ENVIRONMENT", "").strip().lower() == "test":
        return InMemoryAnalyticsStorage()

    try:
        from google.cloud import firestore
        return FirestoreAnalyticsStorage()
    except Exception as e:
        logger.warning(f"Could not connect to Firestore ({e}), using InMemoryAnalyticsStorage.")
        return InMemoryAnalyticsStorage()

