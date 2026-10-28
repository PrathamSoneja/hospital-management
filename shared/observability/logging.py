"""
Structured JSON logging conforming to Google Cloud Logging format.
Excludes sensitive patient medical data, credentials, and raw tokens.
"""
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

class JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": record.levelname,
            "service": self.service_name,
            "event": getattr(record, "event", record.getMessage()),
            "message": record.getMessage(),
        }
        
        # Attach structured contextual fields if present
        for field in ["request_id", "allocation_id", "pool_id", "duration_ms", "operation_id"]:
            val = getattr(record, field, None)
            if val is not None:
                log_payload[field] = val
                
        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_payload)

def setup_logger(service_name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Avoid duplicate handlers on re-configuration
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service_name))
        logger.addHandler(handler)
        
    logger.propagate = False
    return logger
