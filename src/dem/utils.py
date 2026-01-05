from pathlib import Path
from typing import List, Optional

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from rasterio.merge import merge
from rasterio.windows import from_bounds
from shapely.geometry import mapping

from src.utils.geo import reproject_geom


def clip_remote_geotiff_vsicurl(
    href: str,
    aoi_wgs84,
    out_path: str,
    *,
    all_touched: bool = True,
    pad_pixels: int = 2,
) -> Optional[str]:
    vsicurl_path = f"/vsicurl/{href}"
    out_path = str(out_path)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(vsicurl_path) as src:
        if src.crs is None:
            raise ValueError(f"Source has no CRS: {href}")

        aoi_src = reproject_geom(aoi_wgs84, "EPSG:4326", src.crs.to_string())

        raster_bounds = src.bounds
        aoi_bounds = aoi_src.bounds
        if (
            aoi_bounds[2] <= raster_bounds.left
            or aoi_bounds[0] >= raster_bounds.right
            or aoi_bounds[3] <= raster_bounds.bottom
            or aoi_bounds[1] >= raster_bounds.top
        ):
            return None

        minx = max(aoi_bounds[0], raster_bounds.left)
        miny = max(aoi_bounds[1], raster_bounds.bottom)
        maxx = min(aoi_bounds[2], raster_bounds.right)
        maxy = min(aoi_bounds[3], raster_bounds.top)

        win = from_bounds(minx, miny, maxx, maxy, transform=src.transform)
        win = win.round_offsets().round_lengths()

        col_off = max(0, int(win.col_off) - pad_pixels)
        row_off = max(0, int(win.row_off) - pad_pixels)
        width = int(win.width) + 2 * pad_pixels
        height = int(win.height) + 2 * pad_pixels

        width = min(width, src.width - col_off)
        height = min(height, src.height - row_off)

        win = rasterio.windows.Window(col_off, row_off, width, height)

        data = src.read(1, window=win, masked=True)

        win_transform = src.window_transform(win)

        aoi_geojson = [mapping(aoi_src)]
        outside = geometry_mask(
            aoi_geojson,
            out_shape=(data.shape[0], data.shape[1]),
            transform=win_transform,
            invert=False,
            all_touched=all_touched,
        )

        nodata = src.nodata
        if nodata is None:
            nodata = -9999.0 if np.issubdtype(data.dtype, np.floating) else -9999

        masked = np.ma.array(data, mask=np.ma.getmaskarray(data) | outside)
        out_arr = masked.filled(nodata)

        if np.all(out_arr == nodata):
            return None

        profile = src.profile.copy()
        profile.update(
            driver="GTiff",
            height=out_arr.shape[0],
            width=out_arr.shape[1],
            transform=win_transform,
            nodata=nodata,
            count=1,
            compress="deflate",
            tiled=True,
            predictor=2,
            BIGTIFF="IF_SAFER",
        )

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(out_arr, 1)

    return out_path


def mosaic_clipped_tifs(
    clipped_paths: List[str],
    output_path: str,
) -> str:
    if not clipped_paths:
        raise ValueError("clipped_paths is empty")

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    srcs = [rasterio.open(p) for p in clipped_paths]

    try:
        mosaic_arr, mosaic_transform = merge(
            srcs,
            resampling=Resampling.nearest,
        )

        profile = srcs[0].profile.copy()
        profile.update(
            {
                "driver": "GTiff",
                "height": mosaic_arr.shape[1],
                "width": mosaic_arr.shape[2],
                "transform": mosaic_transform,
                "count": mosaic_arr.shape[0],
                "compress": "deflate",
                "tiled": True,
                "predictor": 3,
                "BIGTIFF": "IF_SAFER",
            }
        )

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(mosaic_arr)

        return str(out_path)

    finally:
        for s in srcs:
            s.close()
