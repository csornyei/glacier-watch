from datetime import datetime

import numpy as np
import xarray as xr
from shapely import MultiPolygon

from src.utils.logger import get_logger
from src.utils.models import GlacierSnowData

logger = get_logger("glacier_watch.process")


def clip_raster(raster: xr.DataArray, geometry, crs: str) -> xr.DataArray:
    clipped = raster.rio.clip(
        [geometry],
        crs,
        all_touched=True,
        drop=True,
    )
    return clipped


def stack_bands(bands: list[xr.DataArray]) -> xr.DataArray:
    stacked = xr.concat(bands, dim="band")
    return stacked


def compute_ndsi(band_green: xr.DataArray, band_swir: xr.DataArray) -> xr.DataArray:
    ndsi = (band_green - band_swir) / (band_green + band_swir)
    ndsi = ndsi.where(np.isfinite(ndsi))

    return ndsi


def create_mask(layer: xr.DataArray, threshold: float = 0.4) -> xr.DataArray:
    mask = layer >= threshold

    mask = mask.where(np.isfinite(layer))
    return mask


def analyze_glacier_snow_area(
    glacier_id: str,
    glacier_geom: MultiPolygon,
    dem: xr.DataArray,
    ndsi_mask: xr.DataArray,
    pixel_area: float,
    scene_id: str,
    analysis_id: str,
) -> GlacierSnowData:
    logger.info(f"Processing glacier {glacier_id}")

    glacier_mask = clip_raster(ndsi_mask, glacier_geom, ndsi_mask.rio.crs)
    clipped_dem = clip_raster(dem, glacier_geom, dem.rio.crs)

    glacier_snow_area = int(glacier_mask.sum().item()) * pixel_area

    snow_elevations = clipped_dem.where(glacier_mask).values.flatten()
    snow_elevations = snow_elevations[np.isfinite(snow_elevations)]

    snowline_elevation = np.percentile(snow_elevations, 20)

    result = GlacierSnowData(
        id=f"{scene_id}_{glacier_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        analysis_id=analysis_id,
        glacier_id=glacier_id,
        scene_id=scene_id,
        snow_area_m2=glacier_snow_area,
        snowline_elevation_m=snowline_elevation,
    )

    return result
