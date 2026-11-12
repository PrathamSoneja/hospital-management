FROM python:3.11-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source tree (contracts/ holds the event schema the handler checks against)
COPY contracts/ contracts/
COPY shared/ shared/
COPY services/notification/ services/notification/

ENV PYTHONPATH=/app
# Cloud Run sets PORT itself; 8080 matches the Terraform container_port.
ENV PORT=8080

# Run as an unprivileged user.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
USER 10001

EXPOSE 8080

CMD ["python", "-m", "services.notification.main"]