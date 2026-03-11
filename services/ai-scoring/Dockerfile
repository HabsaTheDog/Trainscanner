# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN addgroup --system app \
  && adduser --system --ingroup app --home /app app

WORKDIR /app

RUN --mount=type=bind,source=requirements.txt,target=/tmp/requirements.txt \
  install -o app -g app -m 0444 /tmp/requirements.txt ./requirements.txt \
  && pip install --no-cache-dir -r requirements.txt

RUN --mount=type=bind,source=main.py,target=/tmp/main.py \
  --mount=type=bind,source=pyproject.toml,target=/tmp/pyproject.toml \
  install -o app -g app -m 0444 /tmp/main.py ./main.py \
  && install -o app -g app -m 0444 /tmp/pyproject.toml ./pyproject.toml

USER app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
