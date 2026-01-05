import argparse
from dataclasses import dataclass

from src.controller.scene import SceneController
from src.download.main import download_item_assets, prepare_folder
from src.utils.logger import add_log_context, get_logger
from src.utils.stac import Stac


@dataclass
class Args:
    log_level: str
    scene_id: str


def parse_args() -> Args:
    parser = argparse.ArgumentParser(description="Glacier Data Downloader")
    parser.add_argument(
        "--log_level",
        type=str,
        help="Logging level",
        default="INFO",
    )
    parser.add_argument(
        "--scene_id", type=str, help="Scene ID for logging context", required=True
    )

    parsed_args = parser.parse_args()

    return Args(log_level=parsed_args.log_level, scene_id=parsed_args.scene_id)


if __name__ == "__main__":
    args = parse_args()

    logger = get_logger("glacier_watch.download", log_level=args.log_level)

    stac = Stac(logger=logger)

    scene = SceneController.get_scene_by_id(args.scene_id)

    if not scene:
        logger.error(f"Scene {args.scene_id} not found.")
        raise ValueError(f"Scene {args.scene_id} not found.")

    add_log_context(scene_id=scene.scene_id, project_id=scene.project_id)

    raw_folder_path = prepare_folder(
        project_id=scene.project_id, scene_id=scene.scene_id, folder_type="raw"
    )

    download_item_assets(stac, scene.stac_href, raw_folder_path)
