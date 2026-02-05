FROM python:3.13-slim-trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    pkg-config \
    default-libmysqlclient-dev

WORKDIR /app
COPY dist/chameleon_usage-0.1.0-py3-none-any.whl /app/
RUN pip install --no-cache-dir --prefix=/install '/app/chameleon_usage-0.1.0-py3-none-any.whl[s3]'

FROM python:3.13-slim-trixie

RUN apt-get update && apt-get install -y --no-install-recommends \
    libmariadb3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
ENTRYPOINT ["chameleon-usage"]
