"""Domain invariants for tenant attributes (shared with the API schemas to stay DRY)."""

import re

from control_plane.domain.exceptions import ValidationFailed

# Kubernetes-namespace compatible, 3-28 chars, see assignment spec.
SLUG_PATTERN = r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$"
_SLUG_RE = re.compile(SLUG_PATTERN)
NAME_MAX_LENGTH = 255


def validate_slug(slug: str) -> str:
    if not _SLUG_RE.fullmatch(slug):
        raise ValidationFailed(f"Invalid slug '{slug}': must match {SLUG_PATTERN}", field="slug")
    return slug


def validate_name(name: str) -> str:
    cleaned = name.strip()
    if not cleaned:
        raise ValidationFailed("Tenant name must not be empty", field="name")
    if len(cleaned) > NAME_MAX_LENGTH:
        raise ValidationFailed(
            f"Tenant name must be at most {NAME_MAX_LENGTH} characters", field="name"
        )
    return cleaned
