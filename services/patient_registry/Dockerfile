FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PATIENT_REGISTRY_REST_HOST=0.0.0.0 \
    PATIENT_REGISTRY_REST_PORT=8000

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY services/patient_registry/ services/patient_registry/

EXPOSE 8000

CMD ["python", "-m", "services.patient_registry.main"]
