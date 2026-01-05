import argparse
import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Set, Tuple

from geoalchemy2.shape import to_shape
from shapely import MultiPolygon
from shapely.geometry import shape

from src.controller.project import ProjectController
from src.controller.scene import SceneController
from src.utils.config import load_project_config
from src.utils.logger import get_logger
from src.utils.models import Glacier, Scene
from src.utils.stac import Stac

logger = get_logger("glacier_watch.discover")


@dataclass
class Args:
    date_from: date
    date_to: date
    project_id: str
    dry_run: bool = False
    log_level: str = "INFO"
    limit: Optional[int] = None


def parse_args() -> Args:
    parser = argparse.ArgumentParser(description="Glacier Data Discoverer")
    parser.add_argument(
        "--date_from",
        type=lambda s: date.fromisoformat(s),
        help="Start date for data discovery (YYYY-MM-DD)",
        default=(date.today() - timedelta(days=30)).isoformat(),
    )
    parser.add_argument(
        "--date_to",
        type=lambda s: date.fromisoformat(s),
        help="End date for data discovery (YYYY-MM-DD)",
        default=date.today().isoformat(),
    )
    parser.add_argument(
        "--project_id",
        type=str,
        help="Project identifier for data discovery",
        required=True,
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="If set, the script will not modify the database",
        default=False,
    )
    parser.add_argument(
        "--log_level",
        type=str,
        help="Logging level",
        default="INFO",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of scenes to process",
        default=None,
    )

    args = parser.parse_args()

    return Args(**vars(args))


def filter_existing_scenes(scene_ids: Set[str]) -> Set[str]:
    """
    Filter out scene IDs that already exist in the database.

    scene_ids: list of scene IDs to check
    Returns a set of existing scene IDs.
    """
    existing_scenes = SceneController.get_scenes_by_ids(scene_ids)
    existing_scene_ids = {scene.scene_id for scene in existing_scenes}

    return scene_ids - existing_scene_ids


def filter_not_intersecting_scenes(scene_geometry, aoi_geometry, scene_id) -> bool:
    """
    Check if the scene geometry intersects with the AOI geometry.

    scene_geometry: shapely geometry of the scene
    aoi_geometry: shapely geometry of the AOI
    Returns True if they intersect, False otherwise.
    """
    if not aoi_geometry.within(scene_geometry):
        logger.warning(f"Scene {scene_id} does not fully cover the AOI. Skipping.")

    if not aoi_geometry.intersects(scene_geometry):
        logger.warning(f"Scene {scene_id} does not intersect the AOI. Skipping.")
        return False

    return True


def calculate_aoi_coverage(scene_geometry, aoi_geometry) -> float:
    """
    Calculate the coverage of the AOI by the scene geometry.

    scene_geometry: shapely geometry of the scene
    aoi_geometry: shapely geometry of the AOI
    Returns coverage percentage as float.
    """

    intersection_area = aoi_geometry.intersection(scene_geometry).area
    aoi_area = aoi_geometry.area
    coverage = intersection_area / aoi_area * 100
    return coverage


def calculate_glaciers_coverage(
    scene_geometry, glaciers: List[Glacier]
) -> List[Tuple[str, float]]:
    """
    Calculate the coverage of each glacier by the scene geometry.

    scene_geometry: shapely geometry of the scene
    glaciers: list of Glacier objects
    Returns a list of tuples (glacier_name, coverage_percentage).
    """

    glacier_coverages = []

    for glacier in glaciers:
        if not glacier.name or not glacier.geometry:
            # skip unnamed glaciers or glaciers without geometry
            continue

        glacier_geometry = to_shape(glacier.geometry)
        if scene_geometry.intersects(glacier_geometry):
            intersection_area = scene_geometry.intersection(glacier_geometry).area
            glacier_area = glacier_geometry.area
            glacier_coverage = intersection_area / glacier_area * 100
            glacier_coverages.append((glacier.name, glacier_coverage))

    return glacier_coverages


def get_scene_from_stac_item(
    item, scene_id: str, project_id: str, project_config: Dict
) -> Scene:
    """
    Create a Scene object from a STAC item.

    item: pystac Item object
    project_id: ID of the project the scene belongs to
    Returns a Scene object.
    """
    props = item.properties

    logger.debug(f"Scene {scene_id} properties: {props}")

    logger.debug(f"Scene {scene_id} assets: {item.assets}")

    items = {
        asset_key: Stac.parse_asset_href(asset)
        for asset_key, asset in item.assets.items()
        if asset_key in project_config["bands"]
    }
    logger.info(f"Assets to download: {items}")

    return Scene(
        scene_id=scene_id,
        project_id=project_id,
        stac_href=json.dumps(items),
        acquisition_date=props.get("datetime"),
        status="queued_for_download",
    )


def save_scenes(scenes: List[Scene], dry_run: bool):
    """
    Save discovered scenes to the database or to a JSON file in dry run mode.

    scenes: list of Scene objects to save
    dry_run: if True, save to JSON file instead of database
    """
    if dry_run:
        with open("scratch/discovered_scenes.json", "w") as f:
            json.dump(
                {
                    "scenes": [
                        {
                            "scene_id": scene.scene_id,
                            "project_id": scene.project_id,
                            "stac_href": scene.stac_href,
                            "acquisition_date": scene.acquisition_date,
                            "status": scene.status,
                        }
                        for scene in scenes
                    ]
                },
                f,
                indent=4,
            )
    else:
        SceneController.add_scenes(scenes)


def main(args: Args):
    logger = get_logger("glacier_watch.discover", args.log_level)

    project_config = load_project_config(args.project_id)

    logger.info(
        f"Discovering scenes for project {args.project_id} from {args.date_from} to {args.date_to}"
    )

    stac = Stac(logger=logger)

    project = ProjectController.get_project_by_id(args.project_id)

    if not project:
        logger.error(f"Project {args.project_id} not found.")
        raise ValueError(f"Project {args.project_id} not found.")

    aoi_geometry = MultiPolygon(to_shape(project.area_of_interest))

    logger.info(f"Using project AOI: {aoi_geometry.wkt}")

    files = stac.search_sentinel2_data(
        polygon=aoi_geometry,
        date_from=args.date_from,
        date_to=args.date_to,
    )

    logger.info(f"Found {len(files)} scenes from STAC API.")

    scenes = []

    if args.dry_run:
        new_scene_ids = {scene.id for scene in files}
    else:
        new_scene_ids = filter_existing_scenes({scene.id for scene in files})

    files = [scene for scene in files if scene.id in new_scene_ids]

    logger.info(f"{len(files)} new scenes to process after filtering existing scenes.")

    count = 0

    project_glaciers = ProjectController.get_glaciers_in_project(project.project_id)

    coverage_by_scene = {}

    for scene_info in files:
        try:
            scene_id = scene_info.id

            coverage_by_scene[scene_id] = {}
            scene_geometry = shape(scene_info.geometry)

            if not filter_not_intersecting_scenes(
                scene_geometry, aoi_geometry, scene_id
            ):
                continue

            aoi_coverage = calculate_aoi_coverage(scene_geometry, aoi_geometry)

            coverage_by_scene[scene_id]["aoi_coverage_percent"] = aoi_coverage

            glacier_coverages = calculate_glaciers_coverage(
                scene_geometry, project_glaciers
            )

            if glacier_coverages:
                for glacier_name, glacier_coverage in glacier_coverages:
                    logger.debug(
                        f"Scene {scene_id} covers {glacier_coverage:.2f}% of glacier {glacier_name}."
                    )
                coverage_by_scene[scene_id]["glaciers"] = glacier_coverages
            else:
                logger.info(
                    f"Scene {scene_id} does not cover any glaciers in the project."
                )
                continue

            new_scene = get_scene_from_stac_item(
                item=scene_info,
                scene_id=scene_id,
                project_id=project.project_id,
                project_config=project_config,
            )

            scenes.append(new_scene)
            logger.info(f"Added scene {scene_id} to the database.")

            count += 1
            if args.limit and count >= args.limit:
                break
        except Exception as e:
            logger.error(f"Error processing scene {scene_info.id}: {e}")
            continue

    # TODO: save this coverage data for later? use it for downloading or processing prioritization?
    logger.info("Coverage summary by scene:")
    for scene_id, coverage in coverage_by_scene.items():
        logger.info(f"Scene {scene_id}:")
        logger.info(f"  AOI coverage: {coverage.get('aoi_coverage_percent', 0):.2f}%")
        glaciers = coverage.get("glaciers", [])
        for glacier_name, glacier_coverage in glaciers:
            logger.info(f"  Glacier {glacier_name} coverage: {glacier_coverage:.2f}%")

    logger.info(f"Total new scenes to add: {len(scenes)}")
    logger.debug(f"Scenes: {[scene.scene_id for scene in scenes]}")

    save_scenes(scenes, dry_run=args.dry_run)


if __name__ == "__main__":
    args = parse_args()

    try:
        main(args)
    except Exception as e:
        logger.error(f"Error in discovery process: {e}")
        raise
