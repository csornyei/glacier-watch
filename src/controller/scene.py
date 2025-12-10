from typing import List, Optional
from logging import Logger


from sqlalchemy.sql import text, select, update

from src.utils.db import get_session
from src.utils.models import Scene, SceneStatusEnum


class SceneController:
    @staticmethod
    def get_scene(status: SceneStatusEnum) -> Optional[Scene]:
        with get_session() as session:
            select_stmt = (
                select(Scene)
                .where(Scene.status == status)
                .order_by(Scene.acquisition_date.desc())
                .limit(1)
            )
            scene = session.execute(select_stmt).scalar_one_or_none()
            return scene

    @staticmethod
    def get_scenes_by_ids(scene_ids: List[str]) -> List[Scene]:
        with get_session() as session:
            scenes = select(Scene).where(Scene.scene_id.in_(scene_ids))
            scenes = session.execute(scenes).scalars().all()
            return scenes

    @staticmethod
    def add_scenes(scenes: List[Scene]):
        with get_session() as session:
            session.add_all(scenes)
            session.commit()

    @staticmethod
    def lock_and_get_scene(
        start_status: SceneStatusEnum, end_status: SceneStatusEnum, logger: Logger
    ) -> Optional[Scene]:
        with get_session() as session:
            row = session.execute(
                text(
                    """
            SELECT * FROM scene
            WHERE status = :status
            ORDER BY acquisition_date DESC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """,
                ),
                params={"status": start_status},
            ).first()

        if row is None:
            logger.info(f"No scenes queued for {start_status}.")
            return None

        logger.info(f"Locking scene: {row.scene_id}")

        session.execute(
            text(
                """
            UPDATE scene
            SET status = :status, updated_at = CURRENT_TIMESTAMP
            WHERE scene_id = :scene_id
            """
            ),
            params={"scene_id": row.scene_id, "status": end_status},
        )

        session.commit()

        logger.info(f"Scene {row.scene_id} status updated to {end_status}.")

        return row

    @staticmethod
    def update_scene_status(
        scene: Scene,
        new_status: SceneStatusEnum,
        session: Optional[object] = None,
        **kwargs,
    ):
        """
        Based on new_status, update the scene status in the database.

        The different statuses require different additional parameters:
            - processed: requires result_path (Path)
            - queued_for_processing: requires download_path (Path)
            - failed_processing: requires error_message (str)
            - failed_download: requires error_message (str)
        """

        if session is not None:
            # Use the provided session
            update_stmt = (
                update(Scene)
                .where(Scene.scene_id == scene.scene_id)
                .values(status=new_status, updated_at=text("CURRENT_TIMESTAMP"))
            )
            match new_status:
                case SceneStatusEnum.processed:
                    update_stmt = update_stmt.values(
                        result_path=str(kwargs["result_path"])
                    )
                case SceneStatusEnum.queued_for_processing | SceneStatusEnum.downloaded:
                    update_stmt = update_stmt.values(
                        download_path=str(kwargs["download_path"])
                    )
                case SceneStatusEnum.failed_processing:
                    update_stmt = update_stmt.values(
                        last_error=kwargs["error_message"],
                        attempts_processing=Scene.attempts_processing + 1,
                    )
                case SceneStatusEnum.failed_download:
                    update_stmt = update_stmt.values(
                        last_error=kwargs["error_message"],
                        attempts_download=Scene.attempts_download + 1,
                    )
            session.execute(update_stmt)
        else:
            with get_session() as session:
                update_stmt = (
                    update(Scene)
                    .where(Scene.scene_id == scene.scene_id)
                    .values(status=new_status, updated_at=text("CURRENT_TIMESTAMP"))
                )
                match new_status:
                    case SceneStatusEnum.processed:
                        update_stmt = update_stmt.values(
                            result_path=str(kwargs["result_path"])
                        )
                    case (
                        SceneStatusEnum.queued_for_processing
                        | SceneStatusEnum.downloaded
                    ):
                        update_stmt = update_stmt.values(
                            download_path=str(kwargs["download_path"])
                        )
                    case SceneStatusEnum.failed_processing:
                        update_stmt = update_stmt.values(
                            last_error=kwargs["error_message"],
                            attempts_processing=Scene.attempts_processing + 1,
                        )
                    case SceneStatusEnum.failed_download:
                        update_stmt = update_stmt.values(
                            last_error=kwargs["error_message"],
                            attempts_download=Scene.attempts_download + 1,
                        )
                session.execute(update_stmt)
                session.commit()
