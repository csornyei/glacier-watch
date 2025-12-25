import pyproj
from shapely.ops import transform


def reproject_geom(geometry, source_crs: str, target_crs: str):
    if source_crs == target_crs:
        return geometry
    transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)
    return transform(transformer.transform, geometry)
