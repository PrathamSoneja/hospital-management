"""REST side of the Lock Coordinator: health checks, metrics and the lock inspector."""
from fastapi import FastAPI, HTTPException
from kazoo.exceptions import NoNodeError

from services.lock_coordinator import zk_store
from services.lock_coordinator.metrics import SERVICE
from shared.observability.middleware import setup_observability_middleware


def describe_pool(zk, pool_id):
    """List everyone in a pool's line, first in line first."""
    entries = []
    for name in zk_store.queue(zk, pool_id):
        path = f"{zk_store.LOCK_ROOT}/{pool_id}/{name}"
        data, stat = zk_store.read_lock(zk, path)
        if data is None:
            continue  # It left while we were looking.
        if data["expires_at"] is None:
            state = "WAITING"
        elif data["expires_at"] <= zk_store.now_ms():
            state = "EXPIRED"  # The reaper will remove it shortly.
        else:
            state = "HOLDING"
        entries.append({
            "position": len(entries),
            "lock_id": zk_store.lock_id_for(path),
            "owner_id": data["owner_id"],
            "state": state,
            "fencing_token": str(stat.czxid),  # The team sends tokens as decimal strings in JSON.
            "expires_at": data["expires_at"],
        })
    return entries


def build_app(zk):
    app = FastAPI(title="Hospital Lock Coordinator", version="1.0.0")
    setup_observability_middleware(app, SERVICE, ready_check=lambda: zk.connected)

    @app.get("/api/v1/locks")
    def list_locks():
        try:
            pools = zk.get_children(zk_store.LOCK_ROOT)
        except NoNodeError:
            pools = []
        return {"pools": {pool_id: describe_pool(zk, pool_id) for pool_id in sorted(pools)}}

    @app.get("/api/v1/locks/{pool_id}")
    def get_pool(pool_id: str):
        if not zk_store.POOL_ID_PATTERN.fullmatch(pool_id):
            raise HTTPException(status_code=404, detail="pool not found")
        try:
            return {"pool_id": pool_id, "entries": describe_pool(zk, pool_id)}
        except NoNodeError:
            raise HTTPException(status_code=404, detail="pool not found")

    return app