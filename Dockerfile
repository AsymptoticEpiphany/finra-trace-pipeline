# ---------------------------------------------------------------------------
# Multi-stage build for the FINRA TRACE pipeline
# ---------------------------------------------------------------------------

# Stage 1: Build
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    cmake \
    make \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY CMakeLists.txt .
COPY src/ src/
COPY include/ include/
COPY utils/ utils/
COPY tests/ tests/

RUN mkdir build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc) main

# Stage 2: Runtime
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/build/main /usr/local/bin/trace_pipeline

CMD ["trace_pipeline"]
