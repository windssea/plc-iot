# syntax=docker/dockerfile:1
ARG PYTHON_IMAGE=python:3.12.14-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254

# amd64/arm64: install manylinux wheels on the build host, never run target binaries.
FROM --platform=$BUILDPLATFORM ${PYTHON_IMAGE} AS dependencies-cross
ARG TARGETARCH
ARG TARGETVARIANT
WORKDIR /build
COPY requirements-runtime.lock .
COPY deploy/container/dependencies.py .
RUN python dependencies.py "$TARGETARCH" "$TARGETVARIANT"

# armv7: cffi has no PyPI armv7l wheel. Install on the target platform so pip
# can use armv7 wheels (rpds, cryptography) and compile cffi from source.
FROM ${PYTHON_IMAGE} AS dependencies-native
WORKDIR /build
COPY requirements-runtime.lock .
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev libffi-dev \
    && python -m pip install --no-cache-dir --no-compile --target /dependencies -r requirements-runtime.lock \
    && mkdir -m 700 /data \
    && apt-get purge -y gcc libc6-dev libffi-dev \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

FROM dependencies-cross AS dependencies-amd64
FROM dependencies-cross AS dependencies-arm64
FROM dependencies-native AS dependencies-arm
FROM dependencies-${TARGETARCH} AS dependencies

FROM ${PYTHON_IMAGE} AS runtime
ARG VERSION=dev
LABEL org.opencontainers.image.title="PLCnext IoT child-device collector" \
      org.opencontainers.image.version=$VERSION
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/opt/dependencies:/app/src
WORKDIR /app
COPY --from=dependencies /dependencies /opt/dependencies
COPY --from=dependencies --chown=10001:10001 /data /var/lib/plcnext-iot
COPY src/ ./src/
COPY contracts/v1/ ./contracts/v1/
COPY tools/agent.py tools/appliance.py tools/_source_path.py tools/preflight.py ./tools/
USER 10001:10001
STOPSIGNAL SIGTERM
EXPOSE 8080
ENTRYPOINT ["python", "-m", "tools.appliance"]
CMD ["--listen", "0.0.0.0"]
