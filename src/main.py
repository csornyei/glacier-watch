import argparse
import json
from datetime import date
from pathlib import Path

import geopandas as gpd

from src.ingest import ingest_data
from src.process import process_bands, snow_area_by_glaciers, snowline_calculation
from src.utils import CRS, load_feature_from_geojson, load_raster, save_raster
from src.utils.config import config

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Glacier Data Ingestor")
    parser.add_argument(
        "--date_from",
        type=lambda s: date.fromisoformat(s),
        help="Start date for data ingestion (YYYY-MM-DD)",
        required=True,
    )
    parser.add_argument(
        "--date_to",
        type=lambda s: date.fromisoformat(s),
        help="End date for data ingestion (YYYY-MM-DD)",
        required=True,
    )
    parser.add_argument(
        "--aoi",
        type=Path,
        help="Path to the GeoJSON file defining the area of interest",
        required=True,
    )
    parser.add_argument(
        "--glaciers",
        type=Path,
        help="Path to the GeoJSON file defining the glaciers",
        required=True,
    )
    parser.add_argument(
        "--dem",
        type=Path,
        help="Path to the DEM file",
        required=True,
    )

    args = parser.parse_args()

    aoi_geometry = load_feature_from_geojson(args.aoi)
    glaciers = gpd.read_file(args.glaciers).to_crs(CRS)
    dem = load_raster(args.dem)

    files = ingest_data(
        date_from=args.date_from,
        date_to=args.date_to,
        aoi_geojson=args.aoi,
        raw_folder=Path(config.raw_data_folder),
    )

    print(files)

    print("Ingestion complete. Downloaded files:")

    results = {}

    for item_id, assets in files.items():
        try:
            print(f"Item ID: {item_id}")
            for asset_key, path in assets.items():
                print(f"\t{asset_key}: {path}")
            print("Processing started!")

            folder_path = assets["folder"]

            stacked, ndsi, ndsi_mask = process_bands(
                {
                    "B02": folder_path / "B02_20m.jp2",
                    "B03": folder_path / "B03_20m.jp2",
                    "B04": folder_path / "B04_20m.jp2",
                    "B11": folder_path / "B11_20m.jp2",
                },
                aoi_geometry=aoi_geometry,
            )

            save_raster(stacked, f"data/processed/{item_id}_stacked.tif")
            save_raster(ndsi, f"data/processed/{item_id}_ndsi.tif")
            save_raster(ndsi_mask, f"data/processed/{item_id}_ndsi_mask.tif")

            dem_reprojected = dem.rio.reproject_match(ndsi_mask)

            total_snow_area, glacier_snow_info = snow_area_by_glaciers(
                ndsi_mask, glaciers
            )
            snowline_elevations = snowline_calculation(
                glaciers, ndsi_mask, dem_reprojected
            )

            # merge glacier_snow_info and snowline_elevations
            result = {}
            for glacier_id, snow_info in glacier_snow_info.items():
                result[glacier_id] = {
                    "id": glacier_id,
                    "name": snow_info["name"],
                    "original_area": float(snow_info["original_area"]),
                    "snow_area": float(snow_info["snow_area"]),
                    "snowline_elevation": float(
                        snowline_elevations.get(glacier_id, None)
                    ),
                }
            results[item_id] = {
                "total_snow_area": float(total_snow_area),
                "glacier_snow_info": result,
            }
            print("Processing completed!")
        except Exception as e:
            print(f"Error processing item {item_id}: {e}")
        finally:
            if folder_path.is_dir():
                for file in folder_path.iterdir():
                    file.unlink()
                folder_path.rmdir()
    with open("data/processed/summary.json", "w") as f:
        json.dump(results, f, indent=4)
