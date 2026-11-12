FROM python:3.11-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source tree
COPY contracts/ contracts/
COPY shared/ shared/
COPY scripts/ scripts/
COPY services/bed_registry/ services/bed_registry/

# Compile Protocol Buffers
RUN python scripts/compile_proto.py

ENV PYTHONPATH=/app
ENV BED_REGISTRY_REST_HOST=0.0.0.0
ENV BED_REGISTRY_REST_PORT=8000
ENV BED_REGISTRY_GRPC_PORT=50053
ENV START_GRPC=true

# Run as an unprivileged user (matches runAsNonRoot/runAsUser in k8s manifests)
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
USER 10001

EXPOSE 8000 50053

CMD ["python", "-m", "services.bed_registry.main"]

