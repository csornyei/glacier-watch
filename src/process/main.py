from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Literal, Optional, Tuple

from geoalchemy2.shape import to_shape
from pyproj import CRS
from rasterio.warp import transform_geom
from shapely import MultiPolygon, box
from shapely.geometry import shape
from shapely.ops import unary_union

from src.controller.project import ProjectController
from src.controller.scene import SceneController
from src.process.processing import (
    analyze_glacier_snow_area,
    clip_raster,
    compute_ndsi,
    create_mask,
    stack_bands,
)
from src.utils.config import load_project_config
from src.utils.db import get_session
from src.utils.file import (
    cleanup_temp_folder,
    load_raster,
    prepare_folder,
    prepare_temp_folder,
)
from src.utils.logger import add_log_context, get_logger
from src.utils.models import Glacier, GlaciersAnalysisResult, Scene, SceneStatusEnum

logger = get_logger("glacier_watch.process")

PROJECT_CRS = CRS.from_epsg(4326)


@dataclass
class Args:
    scene_id: Optional[str]
    log_level: str
    dry_run: bool = False


def parse_args() -> Args:
    parser = ArgumentParser(description="Process glacier watch scenes.")
    parser.add_argument(
        "--scene-id",
        type=str,
        help="Specify a particular scene ID to process.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        help="Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
        default="INFO",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the process without making any changes.",
    )
    args = parser.parse_args()
    return Args(
        scene_id=args.scene_id,
        log_level=args.log_level,
        dry_run=args.dry_run,
    )


def lock_and_get_scene(dry_run: bool, scene_id: Optional[str]) -> Optional[Scene]:
    if dry_run:
        logger.info("Dry run mode enabled. Fetching a scene without locking.")
        if not scene_id:
            scene = SceneController.get_scene(SceneStatusEnum.queued_for_processing)
        else:
            scene = SceneController.get_scene_by_id(scene_id)

        if not scene:
            logger.info("No scenes to process. Exiting.")
            raise SystemExit(0)
    else:
        logger.info("Starting scene processing.")
        scene = SceneController.lock_and_get_scene(
            SceneStatusEnum.queued_for_processing,
            SceneStatusEnum.processing,
            logger,
        )

        if scene is None:
            logger.info("No new scenes to process, checking for reattempts.")
            scene = SceneController.reattempt_failed_scene(
                SceneStatusEnum.failed_processing, logger=logger, max_attempts=3
            )

    return scene


def validate_file_paths(
    scene: Scene, project_config: Dict
) -> Tuple[Path, Dict[str, Path]]:
    dem_file_path = Path("data", scene.project_id, "dem.tif")

    if not dem_file_path.is_file():
        raise FileNotFoundError(f"DEM file not found: {dem_file_path}")

    project_bands = {
        band: Path(scene.download_path) / f"{band}.jp2"
        for band in project_config.get("bands", [])
    }

    for _, band_path in project_bands.items():
        if not band_path.is_file():
            raise FileNotFoundError(f"Band file not found: {band_path}")

    return dem_file_path, project_bands


def get_scene_glaciers(
    glaciers: List[Glacier], raster_path: Path
) -> List[Tuple[str, MultiPolygon]]:
    raster = load_raster(raster_path)
    crs = raster.rio.crs

    transformed_glaciers = [
        (
            glacier.glacier_id,
            transform_geom(
                PROJECT_CRS.to_string(),
                crs.to_string(),
                geom=to_shape(glacier.geometry).__geo_interface__,
            ),
        )
        for glacier in glaciers
    ]

    raster_bounds_geom = box(*raster.rio.bounds())
    filtered_glaciers = []
    for glacier_id, glacier_geom in transformed_glaciers:
        glacier_shp = MultiPolygon(shape(glacier_geom))

        if glacier_shp.within(raster_bounds_geom):
            filtered_glaciers.append((glacier_id, glacier_shp))

    return filtered_glaciers


def clip_rasters_to_glaciers(
    raster_in_path: Path, raster_out_path: Path, glaciers_geom: MultiPolygon
):
    raster = load_raster(raster_in_path)

    crs = raster.rio.crs

    clipped_raster = clip_raster(raster, glaciers_geom, crs)

    clipped_raster.rio.to_raster(raster_out_path)


def get_band_path(project_bands: Dict[str, Path], band_name: str) -> Optional[Path]:
    """
    Get the file path for a specific band from the project bands dictionary. As band resolutions may vary, it matches the start of the band name.
    So "B11" can match "B11_20m" or "B11_10m".

    :param project_bands: Dictionary of band names and their file paths
    :type project_bands: Dict[str, Path]
    :param band_name: The band name to search for
    :type band_name: str
    :return: The file path of the matching band
    """

    for name, path in project_bands.items():
        if name.startswith(band_name):
            return path


def main(args: Args) -> Literal["success", "failure", "no_scene"]:
    try:
        scene = lock_and_get_scene(args.dry_run, args.scene_id)

        if not scene:
            logger.info("No scenes to process.")
            return "no_scene"

        add_log_context(scene_id=scene.scene_id, project_id=scene.project_id)

        project = ProjectController.get_project_by_id(scene.project_id)
        if not project:
            raise ValueError(f"Project {scene.project_id} not found.")

        logger.info(f"Processing scene {scene.scene_id} from project {project.name}.")

        project_config = load_project_config(project.project_id)

        dem_file_path, project_bands = validate_file_paths(scene, project_config)

        project_glaciers = ProjectController.get_glaciers_in_project(scene.project_id)

        filtered_glaciers = get_scene_glaciers(
            project_glaciers, list(project_bands.values())[0]
        )

        glacier_geometries = unary_union(
            [glacier_geom for _, glacier_geom in filtered_glaciers]
        )
        buffered_glacier_geometries = glacier_geometries.buffer(200)

        clipped_bands = {}

        prepare_temp_folder()

        for band_name, file_path in project_bands.items():
            output_path = Path("data/temp") / file_path.name

            clip_rasters_to_glaciers(
                raster_in_path=file_path,
                raster_out_path=output_path,
                glaciers_geom=buffered_glacier_geometries,
            )

            clipped_bands[band_name] = output_path

        results_folder_path = prepare_folder(
            project_id=scene.project_id, scene_id=scene.scene_id, folder_type="result"
        )

        logger.info("Processing clipped bands to compute results.")

        rgb_stacked = stack_bands(
            [
                load_raster(get_band_path(clipped_bands, band_name))
                for band_name in ["B04", "B03", "B02"]
            ]
        ).assign_coords(band=["red", "green", "blue"])

        rgb_stacked.rio.to_raster(results_folder_path / "true_color.tif")

        logger.info(
            f"RGB true color image saved to {results_folder_path / 'true_color.tif'}"
        )

        ndsi = compute_ndsi(
            band_swir=load_raster(get_band_path(clipped_bands, "B11")),
            band_green=load_raster(get_band_path(clipped_bands, "B03")),
        )

        ndsi.rio.to_raster(results_folder_path / "ndsi.tif")

        logger.info(f"NDSI raster saved to {results_folder_path / 'ndsi.tif'}")

        ndsi_mask = create_mask(ndsi, threshold=0.4)
        ndsi_mask.rio.to_raster(results_folder_path / "ndsi_mask.tif")

        logger.info(
            f"NDSI mask raster saved to {results_folder_path / 'ndsi_mask.tif'}"
        )

        transform = ndsi_mask.rio.transform()
        pixel_w = transform.a
        pixel_h = -transform.e
        pixel_area = pixel_w * pixel_h

        dem = load_raster(dem_file_path)

        dem = dem.rio.reproject_match(ndsi_mask)

        analysis_results = []
        now = datetime.now()

        analyis = GlaciersAnalysisResult(
            id=f"{scene.scene_id}_{now.strftime('%Y%m%d%H%M%S')}",
            scene_id=scene.scene_id,
            analysis_date=now,
            snow_area_m2=0.0,
            total_glacier_snow_area_m2=0.0,
        )

        for glacier_id, glacier_geom in filtered_glaciers:
            try:
                glacier_snow_data = analyze_glacier_snow_area(
                    glacier_id=glacier_id,
                    glacier_geom=glacier_geom,
                    dem=dem,
                    ndsi_mask=ndsi_mask,
                    pixel_area=pixel_area,
                    scene_id=scene.scene_id,
                    analysis_id=analyis.id,
                )

                analyis.snow_area_m2 += glacier_snow_data.snow_area_m2
                analysis_results.append(glacier_snow_data)
            except Exception as e:
                logger.warning(f"Error processing glacier {glacier_id}: {e}. Skipping.")
                continue
        analyis.total_glacier_snow_area_m2 = analyis.snow_area_m2

        if not args.dry_run:
            with get_session() as session:
                session.add(analyis)
                session.add_all(analysis_results)
                SceneController.update_scene_status(
                    scene,
                    SceneStatusEnum.processed,
                    session=session,
                    result_path=results_folder_path,
                )
                session.commit()
        else:
            logger.info(f"Total glacier snow area: {analyis.snow_area_m2} m2")
            with open(results_folder_path / "results.txt", "w") as f:
                f.write(f"Total glacier snow area: {analyis.snow_area_m2} m2\n")
                for glacier_data in analysis_results:
                    f.write(
                        f"Glacier {glacier_data.glacier_id}: "
                        f"Snow area: {glacier_data.snow_area_m2} m2, "
                        f"Snowline elevation: {glacier_data.snowline_elevation_m} m\n"
                    )
        logger.info(f"Finished processing scene {scene.scene_id}.")

        cleanup_temp_folder()
        return "success"

    except Exception as e:
        add_log_context(error_traceback=e.__traceback__)
        logger.error(f"Error during processing: {e}")

        if not args.dry_run and scene:
            SceneController.update_scene_status(
                scene,
                SceneStatusEnum.failed_processing,
                error_message=str(e),
            )
        cleanup_temp_folder()
        return "failure"


if __name__ == "__main__":
    args = parse_args()
    logger.setLevel(args.log_level.upper())

    while True:
        success = main(args)
        if args.dry_run:
            break
        match success:
            case "success":
                sleep(5)
            case "no_scene":
                logger.info(
                    "No scenes available for processing. Waiting before retrying."
                )
                sleep(60)
            case "failure":
                logger.info("Processing failed. Waiting before retrying.")
                sleep(10)
