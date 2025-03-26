ARG PLATFORM
ARG GOOS
ARG GOARCH

FROM --platform=$PLATFORM golang:1.23

WORKDIR /app

COPY src/go.mod src/go.sum ./
RUN go mod download

COPY src/ .
RUN CGO_ENABLED=1 GOOS=$GOOS GOARCH=$GOARCH go build -o /app/bemidb

ENTRYPOINT [ "/app/bemidb" ]
CMD [ "start" ]