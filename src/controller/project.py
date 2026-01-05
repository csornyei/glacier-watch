from typing import Optional

from sqlalchemy import func
from sqlalchemy.sql import select, update

from src.utils.db import get_session
from src.utils.models import Glacier, Project


class ProjectController:
    @staticmethod
    def get_project_by_id(project_id: str) -> Optional[Project]:
        with get_session() as session:
            project = session.execute(
                select(Project).where(Project.project_id == project_id)
            ).scalar_one_or_none()
            return project

    @staticmethod
    def update_project_area_of_interest(project_id: str, geometry) -> None:
        with get_session() as session:
            session.execute(
                update(Project)
                .where(Project.project_id == project_id)
                .values(area_of_interest=geometry)
            )
            session.commit()

    @staticmethod
    def get_glaciers_in_project(project_id: str):
        with get_session() as session:
            subq = (
                session.query(Project.area_of_interest)
                .filter(Project.project_id == project_id)
                .scalar_subquery()
            )

            glaciers = (
                session.query(Glacier)
                .filter(func.ST_WITHIN(Glacier.geometry, subq))
                .all()
            )

            return glaciers
