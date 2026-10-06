"""Builds the outbound integration event for a task (the task *is* the event)."""

from __future__ import annotations

from contracts.messages import TaskMessage, TenantRef
from contracts.topology import TASK_CREATED_EVENT, Topology
from control_plane.application.dto import OutboxMessage
from control_plane.correlation import get_correlation_id
from control_plane.domain.entities import Task, Tenant


def build_task_event(task: Task, tenant: Tenant, topology: Topology) -> OutboxMessage:
    message = TaskMessage(
        id=task.id,
        type=task.type.value,
        tenant_id=task.tenant_id,
        status=task.status.value,
        created_at=task.created_at,
        updated_at=task.updated_at,
        tenant=TenantRef(id=tenant.id, slug=tenant.slug, name=tenant.name),
    )
    return OutboxMessage(
        id=task.id,
        event_type=TASK_CREATED_EVENT,
        exchange=topology.tasks_exchange,
        routing_key=topology.task_routing_key(task.type.value),
        payload=message.model_dump(mode="json"),
        correlation_id=get_correlation_id(),
    )
