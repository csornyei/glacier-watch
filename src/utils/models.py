import enum
from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import Column, DateTime, Enum, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


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


class Project(Base):
    __tablename__ = "project"
    project_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)

    area_of_interest = Column(
        Geometry(geometry_type="MULTIPOLYGON", srid=4326, spatial_index=True),
        nullable=True,
    )


class Scene(Base):
    """Circuit breaker entry for a single satellite scene."""

    __tablename__ = "scene"

    scene_id = Column(String, primary_key=True)
    project_id = Column(String, ForeignKey("project.project_id"), index=True)
    stac_href = Column(String)
    acquisition_date = Column(DateTime)

    status = Column(
        Enum(
            SceneStatusEnum,
            name="scenestatusenum",
            native_enum=False,
        ),
        nullable=False,
        index=True,
        default=SceneStatusEnum.discovered,
    )
    download_path = Column(String, nullable=True)
    result_path = Column(String, nullable=True)

    attempts_download = Column(default=0)
    attempts_processing = Column(default=0)
    last_error = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.now, nullable=False, onupdate=datetime.now
    )


class Glacier(Base):
    __tablename__ = "glacier"
    glacier_id = Column(String, primary_key=True)
    name = Column(String, nullable=True)
    geometry = Column(
        Geometry(geometry_type="MULTIPOLYGON", srid=4326, spatial_index=True),
        nullable=False,
    )
    area_m2 = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.now, nullable=False)

    snow_data = relationship(
        "GlacierSnowData", back_populates="glacier", cascade="all, delete-orphan"
    )


class GlacierSnowData(Base):
    __tablename__ = "glacier_snow_data"
    id = Column(String, primary_key=True)
    analysis_id = Column(String, ForeignKey("glacier_analysis_result.id"), index=True)
    glacier_id = Column(String, ForeignKey("glacier.glacier_id"), index=True)
    scene_id = Column(String, ForeignKey("scene.scene_id"), index=True)
    snow_area_m2 = Column(Integer)
    snowline_elevation_m = Column(Integer)

    created_at = Column(DateTime, default=datetime.now, nullable=False)

    glacier = relationship("Glacier", back_populates="snow_data")
    analysis = relationship("GlaciersAnalysisResult", back_populates="glaciers")


class GlaciersAnalysisResult(Base):
    __tablename__ = "glacier_analysis_result"
    id = Column(String, primary_key=True)
    scene_id = Column(String, ForeignKey("scene.scene_id"), index=True)
    analysis_date = Column(DateTime, default=datetime.now, nullable=False)
    snow_area_m2 = Column(Float, nullable=False)
    total_glacier_snow_area_m2 = Column(Float, nullable=False)

    created_at = Column(DateTime, default=datetime.now, nullable=False)

    glaciers = relationship(
        "GlacierSnowData", back_populates="analysis", cascade="all, delete-orphan"
    )
