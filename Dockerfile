ARG IMAGE=python
ARG TAG=3.14-bookworm

FROM golang:1.25-bookworm AS autoscaler-build
WORKDIR /build
COPY pkg/premiscale/cluster-autoscaler/go.mod pkg/premiscale/cluster-autoscaler/go.sum ./
RUN go mod download
COPY pkg/premiscale/cluster-autoscaler/ ./
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -o /premiscale-autoscaler ./cmd/premiscale-autoscaler

FROM ${IMAGE}:${TAG} AS base

SHELL [ "/bin/bash", "-c" ]

ENV PREMISCALE_TOKEN="" \
    PREMISCALE_AUTOSCALER_BINARY=/usr/local/bin/premiscale-autoscaler \
    PREMISCALE_CONFIG_PATH=/opt/premiscale/config.yaml \
    PREMISCALE_PID_FILE=/opt/premiscale/premiscale.pid \
    PREMISCALE_LOG_LEVEL=info \
    PREMISCALE_PLATFORM=app.premiscale.com \
    PREMISCALE_CACERT="" \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1

# https://github.com/opencontainers/image-spec/blob/main/annotations.md#pre-defined-annotation-keys
LABEL org.opencontainers.image.description "© PremiScale, Inc. 2024"
LABEL org.opencontainers.image.licenses "BSL 1.1"
LABEL org.opencontainers.image.authors "Emma Doyle <emma@premiscale.com>"
LABEL org.opencontainers.image.documentation "https://premiscale.com"

USER root

COPY --from=autoscaler-build /premiscale-autoscaler /usr/local/bin/premiscale-autoscaler

RUN apt update && apt install -y libvirt-dev tini \
    && rm -rf /var/apt/lists/* \
    && groupadd premiscale --gid 1001 \
    && useradd -rm -d /opt/premiscale -s /bin/bash -g premiscale -u 1001 premiscale

ENV PATH=/opt/premiscale/.local/bin:/opt/premiscale/bin:${PATH}
WORKDIR /opt/premiscale

RUN chown -R premiscale:premiscale .

USER premiscale

RUN mkdir -p "$HOME"/.ssh/ "$HOME"/.local/bin \
    && touch "$HOME"/.ssh/config

## Build the installable package from this checkout with Poetry.

FROM base AS wheel-build
RUN pip install --no-cache-dir poetry==2.5.1
COPY --chown=premiscale:premiscale pkg/ ./pkg/
COPY --chown=premiscale:premiscale README.md LICENSE poetry.lock pyproject.toml ./
RUN poetry build --format wheel

## Production image

FROM base AS production

COPY --from=wheel-build /opt/premiscale/dist/*.whl /tmp/premiscale-dist/
RUN pip install --no-cache-dir --no-input /tmp/premiscale-dist/*.whl \
    && premiscale --version

ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD [ "premiscale", "--log-stdout" ]

## Development image

FROM base AS develop

ENV POETRY_VIRTUALENVS_CREATE=true \
    POETRY_VERSION=2.5.1 \
    POETRY_CACHE_DIR=/opt/premiscale/poetry-cache

COPY --chown=premiscale:premiscale pkg/ ./pkg/
COPY --chown=premiscale:premiscale integrations/minikube/minikube.sh integrations/minikube/smoke.py ./integrations/minikube/
COPY --chown=premiscale:premiscale .config/minikube/ ./.config/minikube/
COPY --chown=premiscale:premiscale .config/keda/ ./.config/keda/
COPY --chown=premiscale:premiscale README.md LICENSE poetry.lock pyproject.toml requirements.txt ./

RUN mkdir -p ${POETRY_CACHE_DIR}

ADD --chown=premiscale:premiscale https://install.python-poetry.org ./install-poetry.py
RUN --mount=type=cache,target=${HOME}/.local/bin,uid=1001,gid=1001 python install-poetry.py \
    && rm install-poetry.py
RUN --mount=type=cache,target=${POETRY_CACHE_DIR},uid=1001,gid=1001 poetry install --without=dev --with=profile \
    && poetry run premiscale --version
RUN poetry install --without=dev

ENTRYPOINT [ "/usr/bin/tini", "--" ]
# The CLI reads PREMISCALE_LOG_LEVEL directly from the environment.
CMD [ "poetry", "run", "premiscale", "--log-stdout" ]
