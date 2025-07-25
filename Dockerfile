ARG PLATFORM=linux/arm64

FROM --platform=$PLATFORM debian:12-slim AS base

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  curl ca-certificates \
  postgresql-client \
  gcc g++ libc6-dev \
  && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-login app

WORKDIR /app

# Install Java and Trino

ENV JAVA_HOME /opt/jdk-24
ENV PATH $JAVA_HOME/bin:$PATH

RUN \
  ARCH=$(dpkg --print-architecture) && case "$ARCH" in \
    "amd64") \
      JDK_URL="https://download.oracle.com/java/24/archive/jdk-24.0.1_linux-x64_bin.tar.gz"; \
      ;; \
    "arm64") \
      JDK_URL="https://download.oracle.com/java/24/archive/jdk-24.0.1_linux-aarch64_bin.tar.gz"; \
      ;; \
    *) \
      echo "Unsupported architecture: $ARCH"; \
      exit 1; \
      ;; \
  esac \
  && curl -L "$JDK_URL" -o jdk.tar.gz \
  && tar -xzf jdk.tar.gz \
  && rm jdk.tar.gz \
  && mkdir -p /opt \
  && mv jdk-24.0.1 /opt/jdk-24 \
  \
  && curl -L https://repo1.maven.org/maven2/io/trino/trino-server/476/trino-server-476.tar.gz -o trino-server-476.tar.gz \
  && tar -xzf trino-server-476.tar.gz \
  && rm trino-server-476.tar.gz \
  && mv trino-server-476 /app/trino \
  && mkdir -p /data/trino \
  && chown -R "app:app" /app/trino /data/trino

# Set up Trino

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s \
  CMD /app/trino/bin/health-check.sh

COPY --chown=app:app docker/trino/etc /etc/trino
COPY --chown=app:app docker/trino/bin /app/trino/bin
COPY --chown=app:app docker/trino/configure.sql /app/trino/

# Set up syncers and server ########################################################################

FROM base AS compile

# Install Go

ENV GOROOT /usr/local/go
ENV GOPATH /go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

RUN \
  ARCH=$(dpkg --print-architecture) \
  && curl -L "https://go.dev/dl/go1.24.4.linux-$ARCH.tar.gz" -o go.tar.gz \
  && tar -C /usr/local -xzf go.tar.gz \
  && rm go.tar.gz \
  && mkdir -p "$GOPATH/src" "$GOPATH/bin" \
  && chmod -R 777 "$GOPATH"

# Compile syncers and server

COPY --chown=app:app src/syncer-common/go.mod src/syncer-common/go.sum /app/src/syncer-common/
RUN cd /app/src/syncer-common && go mod download

COPY --chown=app:app src/syncer-postgres/go.mod src/syncer-postgres/go.sum /app/src/syncer-postgres/
RUN cd /app/src/syncer-postgres && go mod download

COPY --chown=app:app src/syncer-amplitude/go.mod src/syncer-amplitude/go.sum /app/src/syncer-amplitude/
RUN cd /app/src/syncer-amplitude && go mod download

COPY --chown=app:app src/server/go.mod src/server/go.sum /app/src/server/
RUN cd /app/src/server && go mod download

COPY --chown=app:app src/syncer-common /app/src/syncer-common
COPY --chown=app:app src/syncer-postgres /app/src/syncer-postgres
COPY --chown=app:app src/syncer-amplitude /app/src/syncer-amplitude
COPY --chown=app:app src/server /app/src/server

RUN ARCH=$(dpkg --print-architecture) \
  && cd /app/src/syncer-postgres && CGO_ENABLED=1 GOOS=linux GOARCH=$ARCH go build -o /app/bin/syncer-postgres \
    && cd /app/src/syncer-amplitude && CGO_ENABLED=1 GOOS=linux GOARCH=$ARCH go build -o /app/bin/syncer-amplitude \
    && cd /app/src/server && CGO_ENABLED=1 GOOS=linux GOARCH=$ARCH go build -o /app/bin/server

# Prepare final image ##############################################################################

FROM base AS final

COPY --chown=app:app --from=compile /app/bin/syncer-postgres /app/bin/syncer-amplitude /app/bin/server /app/bin/
COPY --chown=app:app docker/bin /app/bin/

USER app

ENTRYPOINT ["/app/bin/run.sh"]
