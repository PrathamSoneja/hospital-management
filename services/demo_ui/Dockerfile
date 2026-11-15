FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Only what the console needs: shared observability + the console itself (no protobufs, no database).
COPY shared/ shared/
COPY services/demo_ui/ services/demo_ui/

ENV PYTHONPATH=/app
ENV DEMO_UI_HOST=0.0.0.0
ENV DEMO_UI_PORT=8080

RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
USER 10001

EXPOSE 8080

CMD ["python", "-m", "services.demo_ui.main"]
