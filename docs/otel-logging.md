# Execution Plane OTEL logging

Execution Plane writes structured JSON application logs to stdout by default. Set `EP_LOG_OUTPUT_FORMAT=text` for plain local output and `EP_LOG_LEVEL` to a standard Python logging level (`INFO` by default).

Set `EP_OTEL_ENABLED=true` to attach an OTLP HTTP handler to the root logger. Logs continue to appear locally; no OTLP request is made while OTEL is disabled.

## Required identification

- `EP_OTEL_SERVICE_NAME` — service.name resource attribute (default: `execution-plane`)
- `EP_OTEL_SERVICE_INSTANCE_ID` — service.instance.id resource attribute (default: hostname)

The Kubernetes Operator should set the instance ID to the pod name so each replica remains distinguishable.

## Collector

- `EP_OTEL_ENDPOINT` — OTLP HTTP logs endpoint, for example `https://collector.example.com:4318/v1/logs`

HTTPS is required for remote collectors. HTTP is accepted only for loopback or Kubernetes service DNS (`http://<service>.<namespace>.svc[.cluster.local]:4318/v1/logs`).

Authentication is optional but recommended:

- `EP_OTEL_API_KEY` — sent as `Authorization: Bearer <value>`
- `EP_OTEL_AUTH_HEADER_NAME` — override the API-key header (default: `Authorization`)
- `EP_OTEL_CLIENT_CERT_FILE` and `EP_OTEL_CLIENT_KEY_FILE` — mTLS client material (both are required together)
- `EP_OTEL_CA_CERT_FILE` — custom CA bundle for server verification

On shutdown, Execution Plane flushes its OTLP batch processor before removing the root handlers.
