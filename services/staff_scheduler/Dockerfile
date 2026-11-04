FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    STAFF_SCHEDULER_REST_HOST=0.0.0.0 \
    STAFF_SCHEDULER_REST_PORT=8000 \
    STAFF_SCHEDULER_GRPC_HOST=0.0.0.0 \
    STAFF_SCHEDULER_GRPC_PORT=50051 \
    START_GRPC=true

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY services/triage/ services/triage/
COPY services/staff_scheduler/ services/staff_scheduler/

RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
USER 10001

EXPOSE 8000 50051

CMD ["python", "-m", "services.staff_scheduler.main"]
