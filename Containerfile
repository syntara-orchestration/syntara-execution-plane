ARG BUILDER_IMAGE=registry.access.redhat.com/ubi9/python-312:latest

FROM ${BUILDER_IMAGE} AS builder

USER 0
WORKDIR /opt/app-root/src

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN pip3.12 install --no-cache-dir uv==0.12.3

COPY pyproject.toml uv.lock README.md alembic.ini ./
COPY src/ ./src/

RUN uv sync --frozen --no-dev --no-editable

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest@sha256:5ed244b62bbf4095080144d9d35eb8fcd3d39a9801f94aadd63b9d10978a01ae AS runtime

USER 0

RUN microdnf install --setopt=install_weak_deps=0 --nodocs -y python3.12 python3.12-pip python3.12-cffi \
    && microdnf clean all

RUN alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

WORKDIR /opt/app-root/src
COPY LICENSE /licenses/LICENSE
COPY --from=builder /opt/app-root/src/alembic.ini ./alembic.ini
COPY --from=builder /opt/app-root/src/src/execution_plane/migrations ./src/execution_plane/migrations
COPY --from=builder /opt/app-root/src/.venv /opt/app-root/src/.venv
COPY --from=builder /opt/app-root/src/alembic.ini /opt/app-root/src/alembic.ini
COPY --from=builder /opt/app-root/src/src /opt/app-root/src/src

ENV PATH="/opt/app-root/src/.venv/bin:/opt/app-root/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN chown -R 1001:0 /opt/app-root && chmod -R g=u /opt/app-root

USER 1001

CMD ["python3", "-m", "execution_plane.worker"]
