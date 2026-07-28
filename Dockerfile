# Assembles the release image. Deliberately compiles nothing.
#
# Both inputs are produced ahead of this build:
#   - cryo comes from a prebuilt multi-arch image (see Dockerfile.cryo and
#     .github/workflows/cryo-image.yml), so the Rust toolchain never runs here.
#   - the server binary is cross-compiled natively by goreleaser with
#     CGO_ENABLED=0 and placed in the build context.
#
# That keeps the arm64 image free of emulated compilation, which is what
# previously pushed releases past goreleaser's timeout.
#
# Bumping cryo: change CRYO_SHA here, then let .github/workflows/cryo-image.yml
# publish the matching image before tagging a release. This ARG is the single
# source of truth - the workflow reads the pin back out of this file.
ARG CRYO_SHA=559b65455d7ef6b03e8e9e96a0e50fd4fe8a9c86
ARG CRYO_IMAGE=ethpandaops/cryo

FROM ${CRYO_IMAGE}:${CRYO_SHA} AS cryo

FROM alpine:latest

RUN apk --no-cache add ca-certificates && \
    addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

WORKDIR /app

COPY --from=cryo /usr/local/bin/cryo /usr/local/bin/cryo

# Produced by the matching goreleaser build id, not compiled here.
COPY server /app/execution-processor

RUN chown -R appuser:appuser /app

USER appuser

# Metrics (default), API, health check, pprof (all configurable)
EXPOSE 9090 8080 9191 6060

ENTRYPOINT ["/app/execution-processor"]
