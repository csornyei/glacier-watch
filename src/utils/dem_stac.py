from logging import Logger
from typing import Tuple
from itertools import combinations

import requests
from pystac_client import Client
from shapely.geometry import box, shape
from tqdm import tqdm
from shapely.ops import unary_union

from src.utils.config import config
from src.utils.geo import reproject_geom


class DemStac:
    def __init__(self, logger: Logger, target_coverage: float = 0.99):
        self.catalog = Client.open(config.dem_stac_url)
        self.logger = logger
        self.target_coverage = target_coverage

    def __pad_bounds(self, bounds, pad_deg: float):
        minx, miny, maxx, maxy = bounds
        return (
            minx - pad_deg,
            miny - pad_deg,
            maxx + pad_deg,
            maxy + pad_deg,
        )

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

    def __get_item_details(self, items):
        item_details = {}
        epsg_set = set()
        for item in items:
            epsg, item_bbox, item_geom = self.__get_item_proj_bbox_geom(item)
            item_details[item.id] = {
                "epsg": epsg,
                "bbox": item_bbox,
                "geom": item_geom,
            }
            if epsg:
                epsg_set.add(epsg)

        if len(epsg_set) > 1:
            raise ValueError("DEM items have different EPSG codes; cannot proceed.")

        return item_details

    def __get_item_candidates(self, items, item_details, polygon):
        candidates = []
        polygon_area = polygon.area

        for item in items:
            details = item_details.get(item.id, {})

            item_bbox = details.get("bbox")
            item_geom = details.get("geom")

            if not item_bbox.intersects(polygon):
                continue

            test_geom = item_geom if item_geom is not None else item_bbox

            if not test_geom.intersects(polygon):
                continue

            intersect_area = test_geom.intersection(polygon).area
            coverage = intersect_area / polygon_area

            self.logger.debug(
                f"Item {item.id} coverage: {coverage:.4f} (intersect area: {intersect_area}, poly area: {polygon_area})"
            )

            candidates.append((item, coverage))

        if not candidates:
            raise ValueError("No DEM items fully cover the given polygon area.")

        candidates.sort(
            key=lambda x: (x[1], *self.__score_item(x[0])),
            reverse=True,
        )

        return candidates

    def search_dem_data(self, polygon):
        search = self.catalog.search(
            collections=["arcticdem-mosaics-v3.0-10m"],
            bbox=self.__pad_bounds(polygon.bounds, pad_deg=0.1),
        )
        items = list(search.items())
        self.logger.info(f"Found {len(items)} DEM items for the given polygon.")

        item_details = self.__get_item_details(items)

        epsg = next(
            (d["epsg"] for d in item_details.values() if d.get("epsg") is not None),
            None,
        )

        if not epsg:
            raise ValueError("No valid EPSG code found in DEM items.")

        poly_proj = reproject_geom(
            polygon,
            source_crs="EPSG:4326",
            target_crs=f"EPSG:{epsg}",
        )

        polygon_area = poly_proj.area
        if polygon_area == 0:
            raise ValueError("AOI polygon has zero area after reprojection.")

        candidates = self.__get_item_candidates(items, item_details, poly_proj)

        if candidates[0][1] >= self.target_coverage:
            self.logger.info(
                f"Selected single DEM item {candidates[0][0].id} with coverage {candidates[0][1]:.4f}"
            )
            return [candidates[0][0]]

        tile_shapes = []
        for item, _ in candidates:
            details = item_details.get(item.id, {})
            item_geom = details.get("geom")
            item_bbox = details.get("bbox")
            tile_shapes.append(
                (item, item_geom if item_geom is not None else item_bbox)
            )

        best = None
        for r in range(1, len(tile_shapes) + 1):
            for combo in combinations(tile_shapes, r):
                union_geom = unary_union([shape for _, shape in combo])
                intersect_area = union_geom.intersection(poly_proj).area
                coverage = intersect_area / polygon_area

                if best is None or coverage > best[0]:
                    best = (coverage, [item for item, _ in combo])

                if coverage >= self.target_coverage:
                    chosen = [item for item, _ in combo]
                    self.logger.info(
                        f"Selected {len(chosen)} DEM items to cover AOI with coverage {coverage:.4f}"
                    )
                    return chosen

        best_coverage, best_items = best
        self.logger.info(
            f"Selected {len(best_items)} DEM items to cover AOI with coverage {best_coverage:.4f}"
        )
        return best_items

    def is_cog(self, href: str) -> bool:
        info = {"accept_ranges": None, "content_type": None, "tiff_magic": None}

        head_resp = requests.head(href, allow_redirects=True, timeout=15)
        head_resp.raise_for_status()

        info["accept_ranges"] = head_resp.headers.get("Accept-Ranges")
        info["content_type"] = head_resp.headers.get("Content-Type")

        range_resp = requests.get(
            href,
            headers={"Range": "bytes=0-3"},
            stream=True,
            timeout=15,
        )
        range_resp.raise_for_status()

        header = range_resp.content
        if (
            header.startswith(b"II*\x00")
            or header.startswith(b"MM\x00*")
            or header.startswith(b"II+\x00")
            or header.startswith(b"MM\x00+")
        ):
            info["tiff_magic"] = True
        else:
            info["tiff_magic"] = False
            info["header_bytes"] = str(header)

        is_cog = (info["accept_ranges"] == "bytes") and info["tiff_magic"] is True

        return is_cog, info

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
