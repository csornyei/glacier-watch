"""add aoi to project

Revision ID: 037e08bdaff9
Revises: b9cd3d641b0a
Create Date: 2025-12-10 19:09:17.278731

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry

# revision identifiers, used by Alembic.
revision: str = "037e08bdaff9"
down_revision: Union[str, Sequence[str], None] = "b9cd3d641b0a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_geospatial_column(
        "project",
        sa.Column(
            "area_of_interest",
            Geometry(
                geometry_type="MULTIPOLYGON",
                srid=4326,
                dimension=2,
                spatial_index=False,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=True,
        ),
    )
    op.create_geospatial_index(
        "idx_project_area_of_interest",
        "project",
        ["area_of_interest"],
        unique=False,
        postgresql_using="gist",
        postgresql_ops={},
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_geospatial_index(
        "idx_project_area_of_interest",
        table_name="project",
        postgresql_using="gist",
        column_name="area_of_interest",
    )
    op.drop_geospatial_column("project", "area_of_interest")
