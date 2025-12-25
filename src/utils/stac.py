from datetime import date
from logging import Logger
from typing import Tuple

import requests
from pystac_client import Client
from shapely.geometry import Polygon, mapping, box, shape
from tqdm import tqdm

from src.utils.config import config
from src.utils.geo import reproject_geom


class Stac:
    def __init__(self, logger: Logger):
        self.catalog = Client.open(config.stac_url)
        self.logger = logger

    def search_sentinel2_data(self, polygon: Polygon, date_from: date, date_to: date):
        search = self.catalog.search(
            collections=["sentinel-2-l2a"],
            intersects=mapping(polygon),
            datetime=f"{date_from.isoformat()}/{date_to.isoformat()}",
            query={"eo:cloud_cover": {"lt": 20}},
        )
        items = list(search.items())
        self.logger.info(
            f"Found {len(items)} Sentinel-2 items between {date_from} and {date_to}."
        )
        return items

    def __get_cdse_token(self) -> str:
        if not config.cdse_username or not config.cdse_password:
            self.logger.error(
                "CDSE_USERNAME and CDSE_PASSWORD must be set in environment variables."
            )
            raise SystemExit(
                "CDSE_USERNAME and CDSE_PASSWORD must be set in environment variables."
            )

        data = {
            "grant_type": "password",
            "client_id": "cdse-public",
            "username": config.cdse_username,
            "password": config.cdse_password,
        }

        resp = requests.post(
            config.stac_token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()

        data = resp.json()

        token = data.get("access_token")

        if not token:
            raise SystemExit("Could not obtain access_token from CDSE response.")

        self._cdse_token = token

        return token

    def get_cdse_token(self, refresh=False) -> str:
        if not refresh and hasattr(self, "_cdse_token"):
            return self._cdse_token
        return self.__get_cdse_token()

    @staticmethod
    def parse_asset_href(asset) -> str:
        href = asset.href

        extra = getattr(asset, "extra_fields", {})
        alternates = extra.get("alternate") or extra.get("alternates") or {}

        https_alt = alternates.get("https")
        if https_alt and "href" in https_alt:
            href = https_alt["href"]

        if href.startswith("https://") or href.startswith("http://"):
            return href
        elif href.startswith("s3://eodata/"):
            return href.replace(
                "s3://eodata/", "https://eodata.dataspace.copernicus.eu/"
            )

    def download_item_assets(self, asset_href, download_path: str):
        token = self.get_cdse_token()

        headers = {"Authorization": f"Bearer {token}"}

        try:
            with requests.get(asset_href, headers=headers, stream=True) as response:
                response.raise_for_status()
                with open(download_path, "wb") as f:
                    for chunk in tqdm(
                        response.iter_content(chunk_size=8192),
                        desc=f"Downloading {download_path.name}",
                    ):
                        if chunk:
                            f.write(chunk)
        except requests.HTTPError as e:
            self.logger.error(f"Failed to download asset from {asset_href}: {e}")
            raise

        self.logger.info(f"Downloaded asset to {download_path}")


class DemStac:
    def __init__(self, logger):
        self.catalog = Client.open(config.dem_stac_url)
        self.logger = logger

    def __get_item_proj_bbox_geom(self, item):
        props = item.properties or {}
        proj_code = props.get("proj:code")
        proj_bbox = props.get("proj:bbox")
        proj_geom = props.get("proj:geometry")

        if not proj_code or not proj_bbox:
            return None, None, None

        if isinstance(proj_code, str) and proj_code.upper().startswith("EPSG:"):
            epsg = int(proj_code.split(":")[1])
        else:
            epsg = int(proj_code)

        item_bbox = box(*proj_bbox)

        item_geom = None
        if proj_geom:
            try:
                item_geom = shape(proj_geom)
            except Exception:
                item_geom = None

        return epsg, item_bbox, item_geom

    def __score_item(self, item) -> Tuple[float, str]:
        props = item.properties or {}
        data_perc = float(props.get("pgc:data_perc") or 0.0)
        created = props.get("created") or ""
        return (data_perc, created)

    def search_dem_data(self, polygon):
        search = self.catalog.search(
            collections=["arcticdem-mosaics-v3.0-10m"],
            bbox=polygon.bounds,
        )
        items = list(search.items())
        self.logger.info(f"Found {len(items)} DEM items for the given polygon.")

        candidates = []
        for item in items:
            epsg, item_bbox, item_geom = self.__get_item_proj_bbox_geom(item)
            if not epsg or not item_bbox:
                continue

            poly_proj = reproject_geom(
                polygon, source_crs="EPSG:4326", target_crs=f"EPSG:{epsg}"
            )

            if not item_bbox.intersects(poly_proj):
                continue

            if item_geom is not None:
                if item_geom.covers(poly_proj):
                    candidates.append(item)
            else:
                if item_bbox.covers(poly_proj):
                    candidates.append(item)

        if not candidates:
            raise ValueError("No DEM items fully cover the given polygon area.")

        candidates.sort(key=self.__score_item, reverse=True)
        return candidates[0]

    def download_dem_asset(self, item, download_path: str, asset_key: str = "dem"):
        dem_asset = item.assets.get(asset_key)
        if not dem_asset:
            raise ValueError(f"DEM asset with key '{asset_key}' not found in item.")

        asset_href = dem_asset.href

        with requests.get(asset_href, stream=True) as response:
            response.raise_for_status()
            with open(download_path, "wb") as f:
                for chunk in tqdm(
                    response.iter_content(chunk_size=1024 * 1024),
                    desc=f"Downloading DEM to {download_path}",
                ):
                    if chunk:
                        f.write(chunk)

        self.logger.info(f"Downloaded DEM asset to {download_path}")
        return download_path
