"""Business exceptions.

Every exception carries a stable, machine-readable ``code`` that is part of the public
API contract. HTTP status mapping deliberately lives in the presentation layer: the
domain knows *what* went wrong, not *how* it is rendered.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any
from uuid import UUID


class DomainError(Exception):
    """Base class for all expected business failures."""

    code: str = "domain_error"

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class ValidationFailed(DomainError):
    code = "validation_error"


class TenantNotFound(DomainError):
    code = "tenant_not_found"

    def __init__(self, tenant_id: UUID) -> None:
        super().__init__(f"Tenant '{tenant_id}' was not found", tenant_id=str(tenant_id))


class TenantAlreadyExists(DomainError):
    code = "tenant_already_exists"

    def __init__(self, slug: str) -> None:
        super().__init__(f"A tenant with slug '{slug}' already exists", slug=slug)


class TenantUpdateNotAllowed(DomainError):
    """A mutating operation is not permitted in the tenant's current status."""

    code = "tenant_update_not_allowed"

    def __init__(self, tenant_id: UUID, status: StrEnum, operation: str) -> None:
        super().__init__(
            f"Cannot {operation} tenant '{tenant_id}' while it is '{status}'",
            tenant_id=str(tenant_id),
            status=str(status),
            operation=operation,
        )


class TenantVersionConflict(DomainError):
    code = "tenant_version_conflict"

    def __init__(self, tenant_id: UUID, expected_version: int) -> None:
        super().__init__(
            f"Version conflict for tenant '{tenant_id}': "
            f"version {expected_version} is no longer current",
            tenant_id=str(tenant_id),
            expected_version=expected_version,
        )


class TaskNotFound(DomainError):
    code = "task_not_found"

    def __init__(self, task_id: UUID) -> None:
        super().__init__(f"Task '{task_id}' was not found", task_id=str(task_id))


class InvalidStateTransition(DomainError):
    """Raised by the state machines. Internal invariant violation, not a client error."""

    code = "invalid_state_transition"

    def __init__(self, entity: str, current: StrEnum, target: StrEnum) -> None:
        super().__init__(
            f"Illegal {entity} transition '{current}' -> '{target}'",
            entity=entity,
            current=str(current),
            target=str(target),
        )
        self.current = current
        self.target = target


__all__ = [
    "DomainError",
    "InvalidStateTransition",
    "TaskNotFound",
    "TenantAlreadyExists",
    "TenantNotFound",
    "TenantUpdateNotAllowed",
    "TenantVersionConflict",
    "ValidationFailed",
]
