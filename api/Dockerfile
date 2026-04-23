FROM python:3.11-slim

WORKDIR /srv

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /srv/api
# Pre-install delta extension so the API does not need network at first request.
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL delta; LOAD delta;')" || true

ENV PYTHONPATH=/srv
EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
