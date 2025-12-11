import argparse
from datetime import date, timedelta
import json

from shapely import MultiPolygon
from geoalchemy2.shape import to_shape
from shapely.geometry import shape

from src.controller.project import ProjectController
from src.utils.config import load_project_config
from src.utils.logger import get_logger
from src.utils.models import Scene
from src.utils.stac import Stac
from src.controller.scene import SceneController

logger = get_logger("glacier_watch.discover")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Glacier Data Discoverer")
    parser.add_argument(
        "--date_from",
        type=lambda s: date.fromisoformat(s),
        help="Start date for data discovery (YYYY-MM-DD)",
        default=date.today() - timedelta(days=30),
    )
    parser.add_argument(
        "--date_to",
        type=lambda s: date.fromisoformat(s),
        help="End date for data discovery (YYYY-MM-DD)",
        default=date.today(),
    )
    parser.add_argument(
        "--project_id",
        type=str,
        help="Project identifier for data discovery",
        required=True,
    )

    args = parser.parse_args()

    project_config = load_project_config(args.project_id)

    stac = Stac(logger=logger)

    project = ProjectController.get_project_by_id(args.project_id)
    if not project:
        logger.error(f"Project {args.project_id} not found.")
        raise ValueError(f"Project {args.project_id} not found.")

    aoi_geometry = MultiPolygon(to_shape(project.area_of_interest))

    files = stac.search_sentinel2_data(
        polygon=aoi_geometry,
        date_from=args.date_from,
        date_to=args.date_to,
    )

    scenes = []

    scene_ids = [scene.id for scene in files]

    existing_scenes = SceneController.get_scenes_by_ids(scene_ids)
    existing_scene_ids = {scene.scene_id for scene in existing_scenes}

    files = [scene for scene in files if scene.id not in existing_scene_ids]

    for scene_info in files:
        scene_id = scene_info.id
        scene_geometry = shape(scene_info.geometry)

        if not aoi_geometry.within(scene_geometry):
            logger.warning(f"Scene {scene_id} does not fully cover the AOI. Skipping.")
            continue

        props = scene_info.properties

        items = {
            asset_key: Stac.parse_asset_href(asset)
            for asset_key, asset in scene_info.assets.items()
            if asset_key in project_config["bands"]
        }
        logger.info(f"Assets to download: {items}")

        new_scene = Scene(
            scene_id=scene_id,
            project_id=args.project_id,
            stac_href=json.dumps(items),
            acquisition_date=props.get("datetime"),
            status="queued_for_download",
        )
        scenes.append(new_scene)
        logger.info(f"Added scene {scene_id} to the database.")

    SceneController.add_scenes(scenes)
