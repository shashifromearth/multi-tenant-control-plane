"""Domain enumerations. Values are the wire/DB representation, so never rename them."""

from enum import StrEnum


class TenantStatus(StrEnum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    UPDATING = "updating"
    DESTROYING = "destroying"
    FAILED = "failed"
    DESTROYED = "destroyed"


class TaskStatus(StrEnum):
    ACCEPTED = "accepted"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in (TaskStatus.DONE, TaskStatus.FAILED)

    @property
    def is_open(self) -> bool:
        return not self.is_terminal


class TaskType(StrEnum):
    DEPLOY = "deploy"
    UPDATE = "update"
    DESTROY = "destroy"


__all__ = ["TaskStatus", "TaskType", "TenantStatus"]
