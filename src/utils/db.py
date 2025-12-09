# app/db.py
from collections.abc import Generator
from contextlib import contextmanager

from sqlmodel import Session, SQLModel, create_engine

from src.utils.config import config
from src.utils.models import *  # noqa: F401,E402
from src.utils.logger import get_logger

logger = get_logger("glacier_watch.db")

logger.debug("Connecting to database")

print(  # noqa: T201
    f"Database URL: {config.database_url}"
)
engine = create_engine(config.database_url, echo=False, pool_pre_ping=True)


def _init_db() -> None:
    """Only for local dev/tests, migrations handle prod schema."""
    logger.info("Initializing the database schema")
    SQLModel.metadata.create_all(engine)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session


if __name__ == "__main__":
    from src.utils.models import Scene  # noqa: F401

    _init_db()
    logger.info("Initialized the database.")
