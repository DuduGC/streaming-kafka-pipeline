FROM python:3.13.15-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
COPY consumer/requirements.txt ./consumer-requirements.txt
RUN pip install --no-cache-dir \
        --requirement requirements.txt \
        --requirement consumer-requirements.txt \
    && useradd --create-home --uid 10001 appuser

COPY --chown=appuser:appuser consumer ./consumer

USER appuser

CMD ["python", "-m", "consumer.consumer"]
