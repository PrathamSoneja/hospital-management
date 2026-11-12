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
COPY services/resource_analytics/ services/resource_analytics/

ENV PYTHONPATH=/app
ENV ANALYTICS_REST_HOST=0.0.0.0
ENV ANALYTICS_REST_PORT=8008

# Run as unprivileged user
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
USER 10001

EXPOSE 8008

CMD ["python", "-m", "services.resource_analytics.main"]

