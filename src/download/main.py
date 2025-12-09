import json
from pathlib import Path
from time import sleep
from typing import Optional
from sqlmodel import text
from tqdm import tqdm

from src.utils.db import get_session
from src.utils.models import Scene
from src.utils.stac import Stac
from src.utils.logger import get_logger, add_log_context
from src.utils.models import SceneStatusEnum
from src.utils.file import prepare_folder

logger = get_logger("glacier_watch.download")


def lock_and_get_scene() -> Optional[Scene]:
    with get_session() as session:
        row = session.exec(
            text(
                """
            SELECT * FROM scene
            WHERE status = 'queued_for_download'
            ORDER BY acquisition_date desc
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
            )
        ).first()

        if row is None:
            logger.info("No scenes queued for download.")
            return None

        logger.info(f"Downloading scene: {row.scene_id}")

        session.exec(
            text(
                """
            UPDATE scene
            SET status = :status, updated_at = CURRENT_TIMESTAMP
            WHERE scene_id = :scene_id
            """
            ),
            params={"scene_id": row.scene_id, "status": SceneStatusEnum.downloading},
        )

        session.commit()

    return Scene.model_validate(row)


def download_item_assets(stac: Stac, stac_href: str, download_path: Path):
    items = json.loads(stac_href)

    for band_name, href in tqdm(items.items(), desc="Downloading assets"):
        stac.download_item_assets(
            asset_href=href, download_path=download_path / f"{band_name}.jp2"
        )


def mark_success(scene: Scene, download_path: Path):
    with get_session() as session:
        session.exec(
            text(
                """
            UPDATE scene
            SET status = :status, updated_at = CURRENT_TIMESTAMP, download_path = :download_path
            WHERE scene_id = :scene_id
            """
            ),
            params={
                "scene_id": scene.scene_id,
                "status": SceneStatusEnum.downloaded,
                "download_path": str(download_path),
            },
        )
        session.commit()


def mark_fail(scene: Scene, error: str):
    with get_session() as session:
        session.exec(
            text(
                """
            UPDATE scene
            SET status = :status, updated_at = CURRENT_TIMESTAMP, attempts_download = :attempts, last_error = :last_error
            WHERE scene_id = :scene_id
            """
            ),
            params={
                "scene_id": scene.scene_id,
                "status": SceneStatusEnum.failed_download,
                "attempts": scene.attempts_download + 1,
                "last_error": error,
            },
        )
        session.commit()


def download_scene(stac: Stac) -> bool:
    try:
        scene = lock_and_get_scene()

        if scene is None:
            logger.info("No scenes to download.")
            return False

        add_log_context(scene_id=scene.scene_id, project_id=scene.project_id)

        raw_folder_path = prepare_folder(
            project_id=scene.project_id, scene_id=scene.scene_id, folder_type="raw"
        )

        download_item_assets(stac, scene.stac_href, raw_folder_path)

        mark_success(scene, download_path=raw_folder_path)

        return True
    except Exception as e:
        logger.error("Error downloading scene: {e}")
        mark_fail(scene, str(e))


if __name__ == "__main__":
    stac = Stac(logger=logger)

    while True:
        res = download_scene(stac)
        if not res:
            logger.info("No more scenes to download. Waiting for new scenes...")
            sleep(30)
