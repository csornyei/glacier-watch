import argparse
from pathlib import Path
from tempfile import TemporaryDirectory

import rioxarray as rxr
from geoalchemy2.shape import to_shape
from shapely.geometry import mapping

from src.controller.project import ProjectController
from src.dem.utils import clip_remote_geotiff_vsicurl, mosaic_clipped_tifs
from src.utils.cog import is_cog
from src.utils.dem_stac import DemStac
from src.utils.geo import reproject_geom
from src.utils.logger import get_logger

logger = get_logger("glacier_watch.discover")


def cut_dem_to_aoi(output_path: Path, dem_path: str, aoi_geometry) -> str:
    dem = rxr.open_rasterio(dem_path, masked=True).squeeze("band", drop=True)

    dem_crs = dem.rio.crs

    if dem_crs is None:
        raise ValueError("DEM has no CRS; cannot clip/reproject AOI.")

    aoi_reprojected = reproject_geom(
        [mapping(aoi_geometry)],
        source_crs="EPSG:4326",
        target_crs=dem_crs.to_string(),
    )

    cropped_dem = dem.rio.clip(
        [aoi_reprojected], all_touched=True, drop=True, crs=dem_crs
    )

    cropped_dem.rio.to_raster(
        output_path,
        compress="deflate",
        tiled=True,
        predictor=3,
    )

    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Glacier Data DEM Downloader")
    parser.add_argument(
        "--project_id",
        type=str,
        help="Project identifier for DEM downloading",
        required=True,
    )

    args = parser.parse_args()

    logger.info(f"Starting DEM download for project {args.project_id}")

    logger.debug(f"Creating output directory for project {args.project_id}")

    output_dir = Path(f"data/{args.project_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    project = ProjectController.get_project_by_id(args.project_id)
    if not project:
        logger.error(f"Project {args.project_id} not found.")
        raise ValueError(f"Project {args.project_id} not found.")

    logger.info(f"Project found: {project.name}")

    aoi_geometry = to_shape(project.area_of_interest)

    logger.debug(f"AOI Geometry: {aoi_geometry.wkt}")

    dem_stac = DemStac(logger)
    dem_items = dem_stac.search_dem_data(aoi_geometry)

    with TemporaryDirectory() as temp_dir:
        clipped = []
        for dem_item in dem_items:
            logger.info(f"Downloading DEM item {dem_item.id}")
            href = dem_item.assets["dem"].href
            logger.info(f"  Href: {href}")
            is_cog_flag, info = is_cog(logger, href)
            if not is_cog_flag:
                logger.warning(f"  DEM item {dem_item.id} is not a COG. Info: {info}")
            logger.info(f"  Is COG: {is_cog_flag}, Info: {info}")

            temp_output_path = Path(temp_dir) / f"{dem_item.id}.tif"
            clipped_path = clip_remote_geotiff_vsicurl(
                href,
                aoi_geometry,
                temp_output_path,
                all_touched=True,
                pad_pixels=2,
            )
            if clipped_path:
                logger.info(f"  Clipped DEM saved to {clipped_path}")
                clipped.append(clipped_path)

        if not clipped:
            logger.error("No DEM data could be clipped to the AOI.")
            raise ValueError("No DEM data could be clipped to the AOI.")

        final_output_path = output_dir / "dem.tif"

        mosaic_clipped_tifs(clipped, final_output_path)
        logger.info(f"Mosaic DEM saved to {final_output_path}")
