from pathlib import Path
from typing import Literal
import geopandas as gpd
import rioxarray as rxr
import xarray as xr
from shapely.geometry import Polygon

from src.utils.config import CRS


def load_feature_from_geojson(file_path: str, crs: str = CRS) -> Polygon:
    gdf = gpd.read_file(file_path).to_crs(crs)

    geom = gdf.geometry.iloc[0]

    return geom


def load_raster(file_path) -> xr.DataArray:
    raster = rxr.open_rasterio(file_path, masked=True)
    return raster


def save_raster(data_array: xr.DataArray, file_path: str):
    data_array.rio.to_raster(file_path)


def prepare_folder(
    project_id: str, scene_id: str, folder_type: Literal["raw", "result"]
) -> Path:
    raw_folder_path = Path(f"data/{folder_type}/{project_id}/{scene_id}")

    if raw_folder_path.is_dir():
        for file in raw_folder_path.iterdir():
            file.unlink()
        raw_folder_path.rmdir()

    raw_folder_path.mkdir(parents=True, exist_ok=True)

    return raw_folder_path
