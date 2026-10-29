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
COPY services/triage/ services/triage/

# Compile Protocol Buffers
RUN python scripts/compile_proto.py

ENV PYTHONPATH=/app
ENV TRIAGE_REST_HOST=0.0.0.0
ENV TRIAGE_REST_PORT=8000
ENV TRIAGE_GRPC_PORT=50051
ENV START_GRPC=true

EXPOSE 8000 50051

CMD ["python", "-m", "services.triage.main"]
