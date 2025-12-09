from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from sqlmodel import SQLModel
from src.utils.config import config as app_config

from alembic import context
from geoalchemy2 import alembic_helpers

from src.utils.models import Project, Scene  # noqa: F401

config = context.config

config.set_main_option("sqlalchemy.url", app_config.database_url)


if config.config_file_name is not None:
    fileConfig(config.config_file_name)


target_metadata = SQLModel.metadata


def include_object(object, name, type_, reflected, compare_to):
    # Filter out PostGIS/tiger/topology/internal tables
    if type_ == "table":
        schema = getattr(object, "schema", None)

        # Ignore tiger / topology schemas if they exist
        if schema in ("tiger", "tiger_data", "topology"):
            return False

        # Ignore common PostGIS metadata tables in public schema
        if name in (
            "spatial_ref_sys",
            "geometry_columns",
            "geography_columns",
            "raster_columns",
            "raster_overviews",
        ):
            return False

    # Delegate to geoalchemy2’s helper for geometry handling
    return alembic_helpers.include_object(object, name, type_, reflected, compare_to)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=alembic_helpers.include_object,
        process_revision_directives=alembic_helpers.writer,
        render_item=alembic_helpers.render_item,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=alembic_helpers.include_object,
            process_revision_directives=alembic_helpers.writer,
            render_item=alembic_helpers.render_item,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
