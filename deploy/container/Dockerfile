# syntax=docker/dockerfile:1
ARG PYTHON_IMAGE=python:3.12.14-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254
FROM --platform=$BUILDPLATFORM ${PYTHON_IMAGE} AS dependencies
ARG TARGETARCH
ARG TARGETVARIANT
WORKDIR /build
COPY requirements-runtime.lock .
COPY deploy/container/dependencies.py .
RUN python dependencies.py "$TARGETARCH" "$TARGETVARIANT"

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
