from pathlib import Path
from typing import Optional

import geopandas as gpd
from sqlmodel import text

from src.process.processing import (
    process_bands,
    snow_area_by_glaciers,
    snowline_calculation,
)
from src.utils.file import load_feature_from_geojson, load_raster, prepare_folder
from src.utils.config import CRS
from src.utils.db import get_session
from src.utils.models import Scene
from src.utils.logger import get_logger, add_log_context
from src.utils.models import SceneStatusEnum

logger = get_logger("glacier_watch.process")


def lock_and_get_scene() -> Optional[Scene]:
    with get_session() as session:
        row = session.exec(
            text(
                """
            SELECT * FROM scene
            WHERE status = :status
            ORDER BY acquisition_date
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """,
            ),
            params={"status": SceneStatusEnum.downloaded},
        ).first()

        if row is None:
            logger.info("No scenes queued for processing.")
            return None

        logger.info(f"Processing scene: {row.scene_id}")

        session.exec(
            text(
                """
            UPDATE scene
            SET status = :status, updated_at = CURRENT_TIMESTAMP
            WHERE scene_id = :scene_id
            """
            ),
            params={"scene_id": row.scene_id, "status": SceneStatusEnum.processing},
        )

        session.commit()

    return Scene.model_validate(row)


def mark_success(scene: Scene, result_path: Path):
    with get_session() as session:
        session.exec(
            text(
                """
            UPDATE scene
            SET status = :status, updated_at = CURRENT_TIMESTAMP, result_path = :result_path
            WHERE scene_id = :scene_id
            """
            ),
            params={
                "scene_id": scene.scene_id,
                "status": SceneStatusEnum.processed,
                "result_path": str(result_path),
            },
        )
        session.commit()


def mark_fail(scene: Scene, error: str):
    with get_session() as session:
        session.exec(
            text(
                """
            UPDATE scene
            SET status = :status, updated_at = CURRENT_TIMESTAMP, attempts_processing = :attempts, last_error = :last_error
            WHERE scene_id = :scene_id
            """
            ),
            params={
                "scene_id": scene.scene_id,
                "status": SceneStatusEnum.failed_processing,
                "attempts": scene.attempts_processing + 1,
                "last_error": error,
            },
        )
        session.commit()


if __name__ == "__main__":
    try:
        scene = lock_and_get_scene()

        if not scene:
            logger.info("No scenes to process. Exiting.")
            exit(0)

        add_log_context(scene_id=scene.scene_id, project_id=scene.project_id)

        aoi_geojson_path = Path("data", scene.project_id, "aoi.geojson")
        glaciers_geojson_path = Path("data", scene.project_id, "glaciers.geojson")
        dem_file_path = Path("data", scene.project_id, "dem.tif")

        geometry = load_feature_from_geojson(aoi_geojson_path)

        stacked, ndsi, ndsi_mask = process_bands(
            {
                "B02": Path(scene.download_path) / "B02_20m.jp2",
                "B03": Path(scene.download_path) / "B03_20m.jp2",
                "B04": Path(scene.download_path) / "B04_20m.jp2",
                "B11": Path(scene.download_path) / "B11_20m.jp2",
            },
            aoi_geometry=geometry,
        )

        results_folder_path = prepare_folder(
            project_id=scene.project_id, scene_id=scene.scene_id, folder_type="result"
        )

        stacked.rio.to_raster(results_folder_path / "stacked_sentinel2.tif")
        ndsi.rio.to_raster(results_folder_path / "ndsi_sentinel2.tif")
        ndsi_mask.rio.to_raster(results_folder_path / "ndsi_mask_sentinel2.tif")
        glaciers = gpd.read_file(glaciers_geojson_path).to_crs(CRS)
        total_snow_area, glacier_snow = snow_area_by_glaciers(ndsi_mask, glaciers)

        dem = load_raster(dem_file_path)
        # reproject DEM to match NDVI mask CRS
        dem = dem.rio.reproject_match(ndsi_mask)

        snowline_elevations = snowline_calculation(glaciers, ndsi_mask, dem)

        print(f"Total snow area: {total_snow_area} m2")
        glacier_snow_area = 0
        glacier_snow_json = {}
        for glacier_id, glacier_info in glacier_snow.items():
            snow_cover_fraction = float(
                glacier_info["snow_area"] / glacier_info["original_area"] * 100
            )
            glacier_snow_area += float(glacier_info["snow_area"])
            print(
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
        print(f"Total glacier snow area: {glacier_snow_area} m2")
        print(f"Snow outside glaciers: {total_snow_area - glacier_snow_area} m2")

        results = {
            "total_snow_area_m2": float(total_snow_area),
            "glacier_snow_area_m2": float(glacier_snow_area),
            "snow_outside_glaciers_m2": float(total_snow_area - glacier_snow_area),
            "glacier_snow_details": glacier_snow_json,
        }

        with open(results_folder_path / "results.json", "w") as f:
            import json

            json.dump(results, f, indent=4)

        mark_success(scene, result_path=results_folder_path)
        logger.info("Processing completed successfully.")
    except Exception as e:
        logger.error(f"Error processing scene: {e}")
        mark_fail(scene, str(e))
