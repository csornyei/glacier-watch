from dataclasses import dataclass
from typing import Tuple

import requests


@dataclass
class IsCogInfo:
    accept_ranges: str
    content_type: str
    tiff_magic: bool
    header_bytes: str = None


def is_cog(logger, href: str, headers: dict = {}) -> Tuple[bool, IsCogInfo]:
    info = IsCogInfo(accept_ranges=None, content_type=None, tiff_magic=None)
    try:
        head_resp = requests.head(
            href, headers=headers, allow_redirects=True, timeout=15
        )
        head_resp.raise_for_status()

        info.accept_ranges = head_resp.headers.get("Accept-Ranges")
        info.content_type = head_resp.headers.get("Content-Type")
    except Exception as e:
        info.accept_ranges = None
        info.content_type = None
        logger.error(f"Error fetching headers for {href}: {e}")

    try:
        range_resp = requests.get(
            href,
            headers={**headers, "Range": "bytes=0-3"},
            stream=True,
            timeout=15,
        )
        range_resp.raise_for_status()

        header = range_resp.content

        if len(header) < 4:
            info.tiff_magic = False
            info.header_bytes = str(header)
        elif len(header) > 10:
            info.tiff_magic = False
            info.header_bytes = str(header[:10])

        if (
            header.startswith(b"II*\x00")
            or header.startswith(b"MM\x00*")
            or header.startswith(b"II+\x00")
            or header.startswith(b"MM\x00+")
        ):
            info.tiff_magic = True
        else:
            info.tiff_magic = False
            # info.header_bytes = str(header)
    except Exception as e:
        info.tiff_magic = None
        logger.error(f"Error fetching range bytes for {href}: {e}")

    is_cog = (info.accept_ranges == "bytes") and info.tiff_magic is True

    return is_cog, info
