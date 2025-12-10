from pathlib import Path
from argparse import ArgumentParser
from datetime import datetime

import geopandas as gpd
from shapely import MultiPolygon
from geoalchemy2.shape import to_shape

from src.process.processing import (
    process_bands,
    snow_area_by_glaciers,
    snowline_calculation,
)
from src.utils.db import get_session
from src.utils.file import load_raster, prepare_folder
from src.utils.config import CRS
from src.utils.logger import get_logger, add_log_context
from src.utils.models import SceneStatusEnum, GlaciersAnalysisResult, GlacierSnowData
from src.controller.scene import SceneController
from src.controller.project import ProjectController

logger = get_logger("glacier_watch.process")


def main(dry_run: bool = False):
    try:
        if dry_run:
            logger.info("Dry run mode enabled. Fetching a scene without locking.")
            scene = SceneController.get_scene(SceneStatusEnum.queued_for_processing)
        else:
            logger.info("Starting scene processing.")
            scene = SceneController.lock_and_get_scene(
                SceneStatusEnum.queued_for_processing,
                SceneStatusEnum.processing,
                logger,
            )

        if not scene:
            logger.info("No scenes to process. Exiting.")
            exit(0)

        add_log_context(scene_id=scene.scene_id, project_id=scene.project_id)

        project = ProjectController.get_project_by_id(scene.project_id)
        if not project:
            raise ValueError(f"Project {scene.project_id} not found.")

        logger.info(f"Processing scene {scene.scene_id} from project {project.name}.")

        dem_file_path = Path("data", scene.project_id, "dem.tif")

        aoi_geometry = MultiPolygon(to_shape(project.area_of_interest))

        aoi_geometry = (
            gpd.GeoSeries([aoi_geometry], crs="EPSG:4326").to_crs(CRS).iloc[0]
        )

        stacked, ndsi, ndsi_mask = process_bands(
            {
                "B02": Path(scene.download_path) / "B02_20m.jp2",
                "B03": Path(scene.download_path) / "B03_20m.jp2",
                "B04": Path(scene.download_path) / "B04_20m.jp2",
                "B11": Path(scene.download_path) / "B11_20m.jp2",
            },
            aoi_geometry=aoi_geometry,
        )

        results_folder_path = prepare_folder(
            project_id=scene.project_id, scene_id=scene.scene_id, folder_type="result"
        )

        stacked.rio.to_raster(results_folder_path / "stacked_sentinel2.tif")
        ndsi.rio.to_raster(results_folder_path / "ndsi_sentinel2.tif")
        ndsi_mask.rio.to_raster(results_folder_path / "ndsi_mask_sentinel2.tif")

        glaciers = ProjectController.get_glaciers_in_project(scene.project_id)
        if not glaciers:
            raise ValueError(
                f"No glaciers found in project {scene.project_id} area of interest."
            )

        gdf_glaciers = gpd.GeoDataFrame(
            [
                {
                    "glacier_id": glacier.glacier_id,
                    "name": glacier.name,
                    "geometry": to_shape(glacier.geometry),
                    "area_m2": glacier.area_m2,
                }
                for glacier in glaciers
            ],
            crs="EPSG:4326",
        ).to_crs(CRS)

        logger.info(f"Number of glaciers in project area: {len(gdf_glaciers)}")

        total_snow_area, glacier_snow = snow_area_by_glaciers(ndsi_mask, gdf_glaciers)

        dem = load_raster(dem_file_path)
        # reproject DEM to match NDVI mask CRS
        dem = dem.rio.reproject_match(ndsi_mask)

        snowline_elevations = snowline_calculation(gdf_glaciers, ndsi_mask, dem)

        now = datetime.now()
        result = GlaciersAnalysisResult(
            id=f"{scene.scene_id}_{now.strftime('%Y%m%d%H%M%S')}",
            scene_id=scene.scene_id,
            analysis_date=now,
            snow_area_m2=total_snow_area,
            total_glacier_snow_area_m2=sum(
                info["snow_area"] for info in glacier_snow.values()
            ),
        )

        glacier_snow_data_entries = []
        for glacier_id, glacier_info in glacier_snow.items():
            entry = GlacierSnowData(
                id=f"{scene.scene_id}_{glacier_id}_{now.strftime('%Y%m%d%H%M%S')}",
                analysis_id=result.id,
                glacier_id=glacier_id,
                scene_id=scene.scene_id,
                snow_area_m2=glacier_info["snow_area"],
                snowline_elevation_m=snowline_elevations.get(glacier_id, None),
            )
            glacier_snow_data_entries.append(entry)

        if not dry_run:
            with get_session() as session:
                session.add(result)
                session.add_all(glacier_snow_data_entries)
                SceneController.update_scene_status(
                    scene,
                    SceneStatusEnum.processed,
                    session=session,
                    result_path=results_folder_path,
                )
                session.commit()
        else:
            logger.info(f"Total snow area: {total_snow_area} m2")
            glacier_snow_area = 0
            glacier_snow_json = {}
            for glacier_id, glacier_info in glacier_snow.items():
                snow_cover_fraction = float(
                    glacier_info["snow_area"] / glacier_info["original_area"] * 100
                )
                glacier_snow_area += float(glacier_info["snow_area"])
                logger.info(
                    f"Glacier {glacier_info['name']} ({glacier_id}): "
                    f"\n\tSnow area: {glacier_info['snow_area']} m2, "
                    f"\n\tOriginal area: {glacier_info['original_area']} m2, "
                    f"\n\tSnow cover fraction: {snow_cover_fraction:.2f}%, "
                    f"\n\t20th percentile snowline elevation: {snowline_elevations.get(glacier_id, float('nan')):.2f} m"
                )
                glacier_snow_json[glacier_id] = {
                    "name": glacier_info["name"],
                    "snow_area_m2": float(glacier_info["snow_area"]),
                    "original_area_m2": float(glacier_info["original_area"]),
                    "snow_cover_fraction_percent": snow_cover_fraction,
                    "snowline_elevation_m": float(
                        snowline_elevations.get(glacier_id, float("nan"))
                    ),
                }
            logger.info(f"Total glacier snow area: {glacier_snow_area} m2")
            logger.info(
                f"Snow outside glaciers: {total_snow_area - glacier_snow_area} m2"
            )

    except Exception as e:
        add_log_context(error_traceback=e.__traceback__)
        if dry_run:
            logger.error(f"Error during dry run: {e}")
        else:
            logger.error(f"Error locking scene for processing: {e}")

            if scene:
                SceneController.update_scene_status(
                    scene,
                    SceneStatusEnum.failed_processing,
                    error_message=str(e),
                )


if __name__ == "__main__":
    parser = ArgumentParser(description="Process glacier watch scenes.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the process without making any changes.",
    )
    args = parser.parse_args()
    dry_run = args.dry_run

    main(dry_run=dry_run)
