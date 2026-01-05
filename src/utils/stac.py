from datetime import date
from logging import Logger

import requests
from pystac_client import Client
from shapely.geometry import Polygon, mapping
from tqdm import tqdm

from src.utils.config import config


class Stac:
    def __init__(self, logger: Logger):
        self.catalog = Client.open(config.stac_url)
        self.logger = logger

    def search_sentinel2_data(self, polygon: Polygon, date_from: date, date_to: date):
        search = self.catalog.search(
            collections=["sentinel-2-l2a"],
            intersects=mapping(polygon),
            datetime=f"{date_from.isoformat()}/{date_to.isoformat()}",
            query={"eo:cloud_cover": {"lt": float(config.cloud_cover_threshold) * 100}},
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
