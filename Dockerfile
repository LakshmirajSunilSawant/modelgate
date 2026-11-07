# Builds one ModelGate binary. Usage:
#   docker build --build-arg CMD=manager  -t ghcr.io/modelgate/modelgate-controller:dev .
#   docker build --build-arg CMD=analyzer -t ghcr.io/modelgate/modelgate-analyzer:dev .
#   docker build --build-arg CMD=sidecar  -t ghcr.io/modelgate/modelgate-sidecar:dev .
# Builds natively on the build host and cross-compiles for TARGETPLATFORM, so
# multi-arch release builds do not run the Go toolchain under emulation.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG CMD
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER 65532:65532
ENTRYPOINT ["/app"]
