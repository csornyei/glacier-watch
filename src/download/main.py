import json
from pathlib import Path
from time import sleep

from src.controller.scene import SceneController
from src.utils.file import prepare_folder
from src.utils.logger import add_log_context, get_logger
from src.utils.models import SceneStatusEnum
from src.utils.stac import Stac

logger = get_logger("glacier_watch.download")


def download_item_assets(stac: Stac, stac_href: str, download_path: Path):
    items = json.loads(stac_href)

    for band_name, href in items.items():
        logger.info(f"Downloading asset {band_name} from {href} to {download_path}")
        stac.download_item_assets(
            asset_href=href, download_path=download_path / f"{band_name}.jp2"
        )


def download_scene(stac: Stac) -> bool:
    try:
        scene = SceneController.lock_and_get_scene(
            SceneStatusEnum.queued_for_download, SceneStatusEnum.downloading, logger
        )

        if scene is None:
            logger.info("No scenes to download, checking for reattempts.")
            scene = SceneController.reattempt_failed_scene(
                SceneStatusEnum.failed_download, logger=logger, max_attempts=5
            )

        if scene is None:
            logger.info("No scenes to download.")
            return False

        add_log_context(scene_id=scene.scene_id, project_id=scene.project_id)

        raw_folder_path = prepare_folder(
            project_id=scene.project_id, scene_id=scene.scene_id, folder_type="raw"
        )

        download_item_assets(stac, scene.stac_href, raw_folder_path)

        SceneController.update_scene_status(
            scene,
            SceneStatusEnum.queued_for_processing,
            download_path=raw_folder_path,
        )

        return True
    except Exception as e:
        logger.error(f"Error downloading scene: {e}")
        SceneController.update_scene_status(
            scene, SceneStatusEnum.failed_download, error_message=str(e)
        )


if __name__ == "__main__":
    stac = Stac(logger=logger)

    while True:
        res = download_scene(stac)
        if not res:
            logger.info("No more scenes to download. Waiting for new scenes...")
            sleep(30)
