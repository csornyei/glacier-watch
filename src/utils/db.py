# app/db.py
from collections.abc import Generator
from contextlib import contextmanager


from sqlalchemy import create_engine
from sqlalchemy.orm import Session


from src.utils.config import config
from src.utils.logger import get_logger

logger = get_logger("glacier_watch.db")

logger.debug("Connecting to database")

engine = create_engine(config.database_url, echo=False, pool_pre_ping=True, future=True)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session
