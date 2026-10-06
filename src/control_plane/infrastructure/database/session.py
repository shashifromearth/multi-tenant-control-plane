"""Engine / session factory construction."""

from __future__ import annotations

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from control_plane.config import Settings


def build_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,  # survive DB restarts transparently
        # READ COMMITTED (Postgres default) is what makes the versioned UPDATE correct:
        # a blocked UPDATE re-evaluates its WHERE clause against the committed row.
        isolation_level="READ COMMITTED",
        connect_args={"options": "-c timezone=UTC"},
    )


def build_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
