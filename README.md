# Field Observatory S3 Utilities (field_observatory_s3)
This package containes helper utilities to read the Field Observatory data in an S3 bucket at: https://field-observatory.data.lit.fmi.fi
Index page: https://field-observatory.data.lit.fmi.fi/index.html

## Installation
```console
pip install git+https://github.com/ollinevalainen/field_observatory_s3.git
```

## Usage
```python
from field_observatory_s3 import FOBucket
bucket = FOBucket()
harvest_dates_qvidja = bucket.get_harvest_dates("qvidja_ec")
```

## All available methods for FOBucket()
The "field_id" parameter is the "id" blocks in the blocks GeoJSON (https://data.lit.fmi.fi/field-observatory/fieldobs_blocks_translated.geojson).

```python
    def list_files(self, prefix: str, return_key: bool = False) -> list:

    def get_events(self, field_id: str) -> dict:

    def get_harvest_dates(self, field_id: str) -> list:

    def get_mowing_dates(self, field_id: str) -> list:

    def get_species_events(self, field_id: str) -> list:

    def get_harvest_amounts(self, field_id: str) -> list:

    def get_harvest_info(self, field_id: str) -> list:

    def get_mowings_as_harvest_info(self, field_id: str) -> list:

    def get_observation_events(self, field_id: str) -> list:

    def get_AGB_observations(self, field_id: str) -> list:

    def get_harvest_amount(self, field_id: str, date: str) -> float:

    def get_event_type(self, field_id: str, date: str) -> str:

    def get_field_timeseries_data(self, field_id: str, data_type: str) -> pd.DataFrame:

    def get_site_timeseries_data(self, site_id: str, data_type: str) -> pd.DataFrame:

    def get_timeseries_data(self, prefix: str) -> pd.DataFrame:

    def read_block_geojson(self) -> dict:

    def get_field_information(self, field_id: str) -> dict:

    def get_field_geometry(self, field_id: str) -> dict:
```

## TODO
- Add tests
- Improve documentation
