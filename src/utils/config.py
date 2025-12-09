import os
from dataclasses import dataclass

import yaml
from dotenv import load_dotenv

load_dotenv()

CRS = "EPSG:32633"


@dataclass
class Config:
    stac_url: str = os.getenv("STAC_URL", "https://stac.dataspace.copernicus.eu/v1/")
    stac_token_url: str = os.getenv(
        "STAC_TOKEN_URL",
        (
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
            "protocol/openid-connect/token"
        ),
    )
    cdse_username: str = os.getenv("CDSE_USERNAME", "")
    cdse_password: str = os.getenv("CDSE_PASSWORD", "")

    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./glacier_watch.db")

    raw_data_folder: str = os.getenv("RAW_DATA_FOLDER", "./data/raw")
    processed_data_folder: str = os.getenv("PROCESSED_DATA_FOLDER", "./data/processed")


config = Config()


def load_project_config(project_id: str) -> dict:
    config_path = f"data/{project_id}/config.yaml"
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Project config file not found: {config_path}")

    with open(config_path, "r") as f:
        project_config = yaml.safe_load(f)

    return project_config
