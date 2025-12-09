"""create scenes and project tables

Revision ID: b9cd3d641b0a
Revises:
Create Date: 2025-12-09 21:43:06.903804

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql
import sqlmodel

# revision identifiers, used by Alembic.
revision: str = "b9cd3d641b0a"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    op.create_table(
        "project",
        sa.Column("project_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("description", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("project_id"),
    )
    op.create_table(
        "scene",
        sa.Column("scene_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("project_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("stac_href", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("acquisition_date", sa.DateTime(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "discovered",
                "queued_for_download",
                "downloading",
                "downloaded",
                "failed_download",
                "queued_for_processing",
                "processing",
                "processed",
                "failed_processing",
                name="scenestatusenum",
            ),
            nullable=False,
        ),
        sa.Column("download_path", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("result_path", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("attempts_download", sa.Integer(), nullable=False),
        sa.Column("attempts_processing", sa.Integer(), nullable=False),
        sa.Column("last_error", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["project.project_id"],
        ),
        sa.PrimaryKeyConstraint("scene_id"),
    )
    op.create_index(op.f("ix_scene_project_id"), "scene", ["project_id"], unique=False)
    op.create_index(op.f("ix_scene_status"), "scene", ["status"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_index(op.f("ix_scene_status"), table_name="scene")
    op.drop_index(op.f("ix_scene_project_id"), table_name="scene")
    op.drop_table("scene")
    op.drop_table("project")
