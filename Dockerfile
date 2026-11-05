# Builds one ModelGate binary. Usage:
#   docker build --build-arg CMD=manager  -t ghcr.io/modelgate/modelgate-controller:dev .
#   docker build --build-arg CMD=analyzer -t ghcr.io/modelgate/modelgate-analyzer:dev .
#   docker build --build-arg CMD=sidecar  -t ghcr.io/modelgate/modelgate-sidecar:dev .
FROM golang:1.26 AS build
ARG CMD
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER 65532:65532
ENTRYPOINT ["/app"]
