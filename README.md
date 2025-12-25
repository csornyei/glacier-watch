# Glacier Watch

Glacier Watch project's main data pipeline repository. The pipeline consists of 3 main parts:

- **Discover**: Query STAC catalogues for available Sentinel-2 scenes using project's area of interest.
- **Download**: Retrieve and cache raw imagery for local processing.
- **Process**: Compute NDSI, snowline elevation, and snow area per glacier.

There are also two extra scripts:
- Alembic migrations to set up the database for the project.
- Retrieve DEM tif files for each location's area of interest.

## Purpose

Glacier Watch is a reproducible data pipeline for monitoring seasonal snow cover and snowline elevation on glaciers using Sentinel-2 imagery and DEM data. It is designed to support long-term glacier and climate analysis by producing consistent, queryable time-series metrics per glacier.


## Limitations

- Cloud masking relies on Sentinel-2 metadata and may miss thin cloud cover.
- Snowline estimation accuracy depends on DEM resolution.
- Results are not intended as official glacier mass balance measurements.

## Typical Workflow

1. Set up PostGIS and run Alembic migrations
2. Using the API (see below) to add a project, or do it manually:
   1. create a new entry in the `project` table (requires a project_id, name and area_of_interest)
   2. create a folder in the `data` folder, named same as the `project_id`
   3. add a `config.yaml` which contains the bands to be retrieved (see example in `data/kebnekaise/config.yaml`)
3. Retrieve `dem.tif` file automatically by running `src/dem/main.py` with the project_id argument
4. Run `src/discover/main.py` to retrieve the available Sentinel-2 imagery between two dates
5. Run `src/download/main.py` to download the raw images.
6. Run `src/process/main.py` to compute glacier metrics.
7. Access results via the API or UI.

## Outputs

The pipeline produces:
- Time-series snow metrics per glacier
- Snowline elevation estimates per acquisition date per glacier
- Derived raster output (NDSI)
- Metadata linking results to Sentinel-2 scenes

Derived metrics are stored in PostGIS and can be queried via the API.

## Local Development

I used [`uv`](https://github.com/astral-sh/uv) for managing dependencies. To set them up run in the root directory:

```
uv sync
```

There is a `makefile` available to make running the different steps easier, which also assumes `uv` is available.

### Docker Compose

Docker Compose is the recommended way to run the pipeline. 

Start up the database with:

```
docker compose up -d postgres
```

Run the alembic migrations:

```
docker compose run alembic
```

Once it's finished to set up the database run the pipeline via:
```
docker compose run discover

docker compose run download

docker compose run process
```

#### Base Image

The dockerfile uses a custom base image. Building the image for ARM64 requires compiling rasterio which takes about 1 hour in Github Actions.

The base image contains gdal and the project's dependencies. Changing the dependencies requires rebuilding the base image, which can be done via pushing `base-v*.*.*` tag to Github.


## Deployment

The pipeline is deployed in a Kubernetes cluster. 
Other than the project it requires:
  - a `PersistentVolumeClaim` for both the raw and processed data.
  - a `postgis` database with it's own `PersistentVolumeClaim`
  - the `alembic`, `dem` and `discover_backfill` are handled via single jobs
  - `discover` is also runs on a weekly CRON to fetch the latest satellite images
  - `download` is deployed via normal `Deployment`, it supports multiple replicas and parallel downloads
  - `process` is also deployed via normal `Deployment`

## Related applications

There are multiple related projects to support Glacier Watch. You can find them here:
- API: retrieving data, manage files and projects (https://github.com/csornyei/glacier_watch_api)
- UI: both static and dynamic UI to visualise the results (https://github.com/csornyei/glacier_watch_ui)
- Exporter: a simple python script to move the results as static files to AWS S3 (https://github.com/csornyei/glacier_watch_exporter)

## Sources

Satellite images are retrieved from Copernicus Data Space Ecosystem's STAC Catalogue (https://dataspace.copernicus.eu/)

Glacier areas are retrieved from Randolph Glacier Inventory (https://nsidc.org/data/nsidc-0770/versions/6)

DEM files for snowline elevations are retrieved from Polar Geospatial Center's STAC Catalogue (https://www.pgc.umn.edu/)
