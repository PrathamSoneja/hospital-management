FROM python:3.11-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source tree
COPY contracts/ contracts/
COPY shared/ shared/
COPY scripts/ scripts/
COPY services/triage/ services/triage/
COPY services/bed_allocation/ services/bed_allocation/

# Compile Protocol Buffers
RUN python scripts/compile_proto.py

ENV PYTHONPATH=/app
ENV ALLOCATION_REST_HOST=0.0.0.0
ENV ALLOCATION_REST_PORT=8000

EXPOSE 8000

CMD ["python", "-m", "services.bed_allocation.main"]
