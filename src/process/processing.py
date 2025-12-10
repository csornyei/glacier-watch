import geopandas as gpd
import numpy as np
import xarray as xr

from src.utils.file import CRS, load_raster


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


def snow_area_by_glaciers(ndsi_mask: xr.DataArray, glaciers: gpd.GeoDataFrame) -> float:
    raster_crs = ndsi_mask.rio.crs
    transform = ndsi_mask.rio.transform()

    pixel_width = transform.a
    pixel_height = -transform.e
    pixel_area = pixel_width * pixel_height

    total_snow_area = int(ndsi_mask.sum().item()) * pixel_area
    glacier_snow = {}

    for _, glacier in glaciers.iterrows():
        glacier_id = glacier["glacier_id"]
        glacier_name = glacier["name"]
        glacier_og_area = glacier["area_m2"]

        glacier_geom = glacier["geometry"]
        glacier_mask = ndsi_mask.rio.clip(
            [glacier_geom],
            raster_crs,
            all_touched=True,
            drop=True,
        )

        glacier_snow_area = int(glacier_mask.sum().item()) * pixel_area
        glacier_snow[glacier_id] = {
            "id": glacier_id,
            "name": glacier_name,
            "original_area": glacier_og_area,
            "snow_area": glacier_snow_area,
        }

    return total_snow_area, glacier_snow


def snowline_calculation(
    glaciers: gpd.GeoDataFrame,
    ndsi_mask: xr.DataArray,
    dem: xr.DataArray,
    percentile: float = 20.0,
) -> dict[str, float]:
    raster_crs = ndsi_mask.rio.crs
    snowline_elevations = {}

    for _, glacier in glaciers.iterrows():
        glacier_id = glacier["glacier_id"]
        glacier_geom = glacier["geometry"]

        clipped_ndsi = ndsi_mask.rio.clip(
            [glacier_geom],
            raster_crs,
            all_touched=True,
            drop=True,
        )

        clipped_dem = dem.rio.clip(
            [glacier_geom],
            raster_crs,
            all_touched=True,
            drop=True,
        )

        snow_elevations = clipped_dem.where(clipped_ndsi).values.flatten()
        snow_elevations = snow_elevations[np.isfinite(snow_elevations)]

        if len(snow_elevations) == 0:
            snowline_elevations[glacier_id] = float("nan")
            continue

        snowline_elevation = np.percentile(snow_elevations, percentile)
        snowline_elevations[glacier_id] = snowline_elevation

    return snowline_elevations


def process_bands(
    bands: dict[str, str], aoi_geometry
) -> tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
    """
    Process the ingested bands (RGB and SWIR) to compute stacked data, NDSI, and NDSI mask.

    :param bands: Ingested bands with names and file paths
    :type bands: dict[str, str]
    :param aoi_geojson_path: Path to the GeoJSON file defining the area of interest
    :type aoi_geojson_path: Path
    :return: Stacked bands, NDSI, and NDSI mask
    :rtype: tuple[xr.DataArray, xr.DataArray, xr.DataArray]
    """

    bands_dataarrays = {
        band_name: clip_raster(load_raster(file_path), aoi_geometry, CRS)
        for band_name, file_path in bands.items()
    }

    stacked = stack_bands(list(bands_dataarrays.values())).assign_coords(
        band=list(bands_dataarrays.keys())
    )

    ndsi = compute_ndsi(bands_dataarrays["B03"], bands_dataarrays["B11"])

    ndsi_mask = create_mask(ndsi, threshold=0.4)

    return stacked, ndsi, ndsi_mask
