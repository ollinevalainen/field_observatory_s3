from field_observatory_s3.field_observatory_s3 import FOBucket


class TestFOBucket:
    def test_get_sites(self):
        fo_bucket = FOBucket()
        sites = fo_bucket.get_sites()
        assert isinstance(sites, list) == True
        assert len(sites) > 0

    def test_get_sites_with_site_type_filter(self):
        fo_bucket = FOBucket()
        sites = fo_bucket.get_sites(site_type_filter="Intensive Site")
        assert isinstance(sites, list) == True
        assert len(sites) > 0
        assert "qvidja" in sites

    def test_get_fields(self):
        fo_bucket = FOBucket()
        fields = fo_bucket.get_fields()
        assert isinstance(fields, list) == True
        assert len(fields) > 0

    def test_get_fields_with_site_filter(self):
        fo_bucket = FOBucket()
        fields = fo_bucket.get_fields(site_filter="qvidja")
        assert isinstance(fields, list) == True
        assert len(fields) > 0
        assert "qvidja_ec" in fields

    def test_get_fields_with_site_type_filter(self):
        fo_bucket = FOBucket()
        fields = fo_bucket.get_fields(site_type_filter="Intensive Site")
        assert isinstance(fields, list) == True
        assert len(fields) > 0
        assert "qvidja_ec" in fields

    def test_get_site_types(self):
        fo_bucket = FOBucket()
        site_types = fo_bucket.get_site_types()
        assert isinstance(site_types, list) == True
        assert len(site_types) > 0

    def test_get_field_datasense_devices(self):
        fo_bucket = FOBucket()
        field_datasense_devices = fo_bucket.get_field_datasense_devices(
            "ki", "0", "soil_sensors"
        )
        # check that list contains the correct devices
        expected_devices = ["T12-2", "T12-1"]
        assert set(field_datasense_devices) == set(expected_devices)

    def test_get_site_datasense_devices(self):
        fo_bucket = FOBucket()
        site_datasense_devices = fo_bucket.get_site_datasense_devices(
            "ki", "precipitation_sensors"
        )
        # check that list contains the correct devices
        expected_devices = ["RC-1"]
        assert set(site_datasense_devices) == set(expected_devices)

    def test_get_site_timeseries_data(self):
        import pandas as pd

        fo_bucket = FOBucket()
        df = fo_bucket.get_site_timeseries_data("ki", "fmimeteo")
        assert isinstance(df, pd.DataFrame) == True
        assert df.empty == False

    def test_get_site_fmi_weather_station_id(self):
        fo_bucket = FOBucket()
        station_id = fo_bucket.get_site_fmi_weather_station_id("ki")
        assert station_id == 100967

    def test_get_field_satellite_timeseries_data(self):
        import pandas as pd

        fo_bucket = FOBucket()
        df = fo_bucket.get_field_satellite_timeseries_data("ki_0", "ndvi")
        assert isinstance(df, pd.DataFrame) == True
        assert df.empty == False
        assert "ndvi" in df.columns
