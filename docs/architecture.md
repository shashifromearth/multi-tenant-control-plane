# Architecture notes

The README covers the overview, the patterns and the main trade-offs. This document adds
the remaining sequence diagrams and the reasoning behind the race-safety arguments.

## Tenant lifecycle

```mermaid
stateDiagram-v2
    [*] --> provisioning : POST /tenants (deploy task)
    provisioning --> active : deploy done
    provisioning --> failed : deploy failed
    active --> updating : PATCH (update task)
    updating --> active : update done
    updating --> failed : update failed
    active --> destroying : DELETE (destroy task)
    failed --> destroying : DELETE (destroy task)
    destroying --> destroyed : destroy done
    destroying --> failed : destroy failed
    destroyed --> [*]
```

```mermaid
stateDiagram-v2
    [*] --> accepted : created with its tenant operation
    accepted --> in_progress : worker
    in_progress --> done : worker
    in_progress --> failed : worker
    done --> [*]
    failed --> [*]
```

Both are encoded as transition tables in
`src/control_plane/domain/services/state_machine.py`, and an exhaustive parametrized test
checks every `(from, to)` pair of both against the specification.

## PATCH with optimistic locking: the race

```mermaid
sequenceDiagram
    autonumber
    participant A as Client A
    participant B as Client B
    participant API as API (2 threads)
    participant DB as PostgreSQL

    A->>API: PATCH {version: 2, name: "A"}
    B->>API: PATCH {version: 2, name: "B"}
    API->>DB: [A] SELECT tenant -> v2, active
    API->>DB: [B] SELECT tenant -> v2, active
    API->>DB: [A] UPDATE … SET version=3 WHERE id=… AND version=2
    Note over DB: A holds the row lock
    API->>DB: [B] UPDATE … WHERE id=… AND version=2 (blocks)
    API->>DB: [A] INSERT task, INSERT outbox, COMMIT
    Note over DB: B's UPDATE re-checks WHERE on the committed row: version is 3, 0 rows
    API-->>A: 202 {tenant v3 updating, task}
    API-->>B: 409 tenant_version_conflict (transaction rolled back, no task, no event)
```

The early `tenant.version != expected` check in `TenantService.update` only makes the common
"stale client" case fail fast. Correctness comes from the single-statement compare-and-swap.

## Duplicate slug: the race

```mermaid
sequenceDiagram
    participant A as Request A
    participant B as Request B
    participant DB as PostgreSQL
    A->>DB: INSERT tenant slug='acme' (takes unique-index entry)
    B->>DB: INSERT tenant slug='acme' (waits on A's index entry)
    A->>DB: INSERT task, outbox, COMMIT
    DB-->>B: unique_violation on uq_tenants_slug
    Note over B: repository maps constraint name to TenantAlreadyExists -> 409
```

There is no `SELECT … WHERE slug = ?` pre-check, because it could never be race-free. The
unique index is the single source of truth.

## Worker redelivery

```mermaid
sequenceDiagram
    participant MQ as RabbitMQ
    participant W as Worker
    participant K as Consumer
    MQ->>W: task T (delivery 1)
    W->>MQ: progress(event_id = uuid5(T, in_progress))
    Note over W: crash before ack
    MQ->>W: task T (redelivery)
    W->>MQ: progress(event_id = uuid5(T, in_progress))  same id
    W->>MQ: progress(event_id = uuid5(T, done))
    W->>MQ: ack
    MQ->>K: in_progress (applied)
    MQ->>K: in_progress (duplicate: inbox hit, no-op)
    MQ->>K: done (applied, tenant -> active)
```

Even if the redelivered run picks a *different* random outcome (for example `failed` after a
`done`), the first terminal outcome wins: terminal task states are immutable, and later
reports are recorded as `stale`.

## Why each process loop is safe to kill at any instruction

| Process | Kill point | Result |
|---|---|---|
| API | Anywhere before `COMMIT` | Transaction rolled back; no outbox row means no event. |
| API | After `COMMIT`, before the response | Tenant exists; a client retry gets `tenant_already_exists`, or `version_conflict` for PATCH. Visible via GET. |
| Relay | After publish, before `COMMIT` | Row still pending and re-published (duplicate); the worker's deterministic ids keep the effects single. |
| Consumer | Before `COMMIT` | Message unacked, so redelivered; the inbox row was rolled back, so it is processed fresh. |
| Consumer | After `COMMIT`, before ack | Redelivered; the inbox hit makes it a no-op. |
| Consumer | After retry/DLQ publish, before ack | Message both requeued and redelivered; the inbox makes the second processing a no-op. |
| Worker | Before final ack | Task redelivered; the same event ids are re-emitted. |
