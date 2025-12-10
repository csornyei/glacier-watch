import argparse
from datetime import date, timedelta
import json
from pathlib import Path
from shapely.geometry import shape

from src.utils.config import load_project_config
from src.utils.file import load_feature_from_geojson
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

    aoi_geojson_path = Path(f"data/{args.project_id}/aoi.geojson")

    if not aoi_geojson_path.is_file():
        logger.error(f"GeoJSON file not found: {aoi_geojson_path}")
        raise FileNotFoundError(f"GeoJSON file not found: {aoi_geojson_path}")

    logger.info(f"Fetching data between {args.date_from} and {args.date_to}!")

    polygon = load_feature_from_geojson(aoi_geojson_path, "EPSG:4326")

    files = stac.search_sentinel2_data(
        polygon=polygon,
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

        if not polygon.within(scene_geometry):
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
