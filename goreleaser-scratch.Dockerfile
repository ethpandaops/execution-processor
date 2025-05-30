FROM gcr.io/distroless/static-debian11:latest
COPY execution-processor* /execution-processor
ENTRYPOINT ["/execution-processor"]
