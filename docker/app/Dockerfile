FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git libssl3 liblz4-1 \
    && rm -rf /var/lib/apt/lists/*
# Match the native client to the server, independently of Python package imports.
RUN curl -fsSL https://github.com/apple/foundationdb/releases/download/8.0.0/libfdb_c.x86_64.so -o /usr/local/lib/libfdb_c.so \
    && echo '91ebcb6ca043b11cb840c95037699604674bce584dd71bd7b4e8bfcdacbed022  /usr/local/lib/libfdb_c.so' | sha256sum -c - \
    && ldconfig
WORKDIR /app
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .
COPY demo ./demo
COPY scripts ./scripts
COPY tests ./tests
COPY docker/clickhouse/init.sql ./docker/clickhouse/init.sql
ENV PYTHONUNBUFFERED=1
CMD ["foremen", "--help"]
