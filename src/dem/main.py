import argparse
from pathlib import Path
from tempfile import TemporaryDirectory

from geoalchemy2.shape import to_shape
from shapely.geometry import mapping
import rioxarray as rxr

from src.controller.project import ProjectController
from src.utils.logger import get_logger
from src.utils.stac import DemStac
from src.utils.geo import reproject_geom

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

    output_dir = Path(f"data/{args.project_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    project = ProjectController.get_project_by_id(args.project_id)
    if not project:
        logger.error(f"Project {args.project_id} not found.")
        raise ValueError(f"Project {args.project_id} not found.")

    aoi_geometry = to_shape(project.area_of_interest)

    dem_stac = DemStac(logger)
    dem_item = dem_stac.search_dem_data(aoi_geometry)

    with TemporaryDirectory() as tmpdir:
        dem_path = dem_stac.download_dem_asset(
            dem_item, download_path=f"{tmpdir}/dem.tif", asset_key="dem"
        )

        cut_dem_path = cut_dem_to_aoi(
            output_path=f"data/{project.project_id}/dem.tif",
            dem_path=dem_path,
            aoi_geometry=aoi_geometry,
        )
