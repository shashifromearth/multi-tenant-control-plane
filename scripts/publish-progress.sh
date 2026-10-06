#!/usr/bin/env bash
# Publish a hand-crafted progress event straight to RabbitMQ via its management HTTP API
# (broker tooling only -- no project code involved).
#
#   ./scripts/publish-progress.sh <task_id> <in_progress|done|failed> [event_id]
#   ./scripts/publish-progress.sh --raw '<any body, e.g. not json>'
#
# Re-running with the same event_id demonstrates idempotency; sending in_progress after
# done demonstrates order tolerance; --raw demonstrates poison handling (-> DLQ).
set -euo pipefail

RABBIT_API="${RABBIT_API:-http://localhost:15672/api}"
RABBIT_USER="${RABBITMQ_USER:-controlplane}"
RABBIT_PASS="${RABBITMQ_PASSWORD:-controlplane}"
PREFIX="${BROKER_PREFIX:-cp}"
EXCHANGE="${PREFIX}.task-progress"

uuid() { cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen | tr 'A-Z' 'a-z'; }

if [[ "${1:-}" == "--raw" ]]; then
  body="${2:?raw body required}"
  routing_key="task.progress.raw"
else
  task_id="${1:?usage: $0 <task_id> <status> [event_id]}"
  status="${2:?status required: in_progress|done|failed}"
  event_id="${3:-$(uuid)}"
  body=$(printf '{"event_id":"%s","task_id":"%s","status":"%s","timestamp":"%s"}' \
    "$event_id" "$task_id" "$status" "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)")
  routing_key="task.progress.${status}"
  echo "event_id=${event_id}" >&2
fi

payload=$(python3 -c 'import json,sys; print(json.dumps({"properties":{"content_type":"application/json","delivery_mode":2},"routing_key":sys.argv[1],"payload":sys.argv[2],"payload_encoding":"string"}))' "$routing_key" "$body" 2>/dev/null \
  || printf '{"properties":{"content_type":"application/json","delivery_mode":2},"routing_key":"%s","payload":%s,"payload_encoding":"string"}' "$routing_key" "$(printf '%s' "$body" | sed 's/\\/\\\\/g; s/"/\\"/g; s/^/"/; s/$/"/')")

curl -fsS -u "${RABBIT_USER}:${RABBIT_PASS}" -H 'content-type: application/json' \
  -X POST "${RABBIT_API}/exchanges/%2F/${EXCHANGE}/publish" -d "$payload"
echo
