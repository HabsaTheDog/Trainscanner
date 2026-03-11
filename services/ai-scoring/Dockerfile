FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN addgroup --system app \
  && adduser --system --ingroup app --home /app app

WORKDIR /app

COPY --chown=app:app requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app main.py ./main.py
COPY --chown=app:app pyproject.toml ./pyproject.toml

USER app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
