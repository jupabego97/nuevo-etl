"""Entorno Alembic con esquema aislado."""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from alegra_etl.config import get_settings
from alegra_etl.db.models import Base
from alegra_etl.db.models.base import configure_schema

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

settings = get_settings()
config.set_main_option("sqlalchemy.url", settings.database_url)
configure_schema(settings.db_schema)
target_metadata = Base.metadata


def include_name(name, type_, parent_names):
    if type_ == "schema":
        return name in (settings.db_schema, None)
    return True


def run_migrations_offline() -> None:
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_name=include_name,
        version_table_schema=settings.db_schema,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{settings.db_schema}"'))
        connection.commit()
        connection.execute(text(f'SET search_path TO "{settings.db_schema}", public'))
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_name=include_name,
            version_table_schema=settings.db_schema,
        )
        print(f"[alembic] search_path={settings.db_schema}", flush=True)
        with context.begin_transaction():
            context.run_migrations()
        print("[alembic] upgrade finalizado", flush=True)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
