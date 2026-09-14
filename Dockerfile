# syntax=docker/dockerfile:1

FROM golang:1.22-bookworm AS build
WORKDIR /src

# Requires generated protobuf code (proto/kvpb, proto/raftpb) and a
# populated go.sum to already exist in the build context - see
# scripts/generate.sh and README "Setup". Both need network access, which
# this Dockerfile's build step does not perform on your behalf.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/node ./cmd/node
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/benchmark ./cmd/benchmark

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/node /usr/local/bin/node
COPY --from=build /out/benchmark /usr/local/bin/benchmark
ENTRYPOINT ["/usr/local/bin/node"]
