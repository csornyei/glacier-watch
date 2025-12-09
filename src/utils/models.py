# app/models.py
import enum
from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel
from geoalchemy2 import Geometry


class SceneStatusEnum(str, enum.Enum):
    discovered = "discovered"
    queued_for_download = "queued_for_download"
    downloading = "downloading"
    downloaded = "downloaded"
    failed_download = "failed_download"
    queued_for_processing = "queued_for_processing"
    processing = "processing"
    processed = "processed"
    failed_processing = "failed_processing"


class Project(SQLModel, table=True):
    project_id: str = Field(primary_key=True)
    name: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)


class Scene(SQLModel, table=True):
    """Circuit breaker entry for a single satellite scene."""

    scene_id: str = Field(primary_key=True)
    project_id: str = Field(foreign_key="project.project_id", index=True)
    stac_href: str
    acquisition_date: datetime

    status: SceneStatusEnum = Field(index=True, default=SceneStatusEnum.discovered)

    download_path: Optional[str] = None
    result_path: Optional[str] = None

    attempts_download: int = Field(default=0)
    attempts_processing: int = Field(default=0)
    last_error: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


#     geometry: Geometry = Column(
#           Geometry(
#               geometry_type="MULTIPOLYGON",
#               srid=4326,
#               spatial_index=True,
#       )
#   )
