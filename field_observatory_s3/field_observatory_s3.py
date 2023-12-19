"""
Helper functions for the Field Observatory data in S3 bucket

Classes:
-------------
.. autosummary::
    field_observatory_S3.FOBucket

Functions:
-------------
.. autosummary::
    field_observatory_S3.csv_files
    field_observatory_S3.read_files_to_dataframe

Author: Olli Nevalainen, Finnish Meteorological Institute
"""
import os
import json
import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.config import Config

FO_BLOCK_GEOJSON = "fieldobs_blocks_translated.geojson"


class FOBucket:
    """
    Class for handling the Field Observatory S3 bucket

    Attributes
    ----------
    s3 : boto3.resource
        S3 resource
    bucket : boto3.Bucket
        S3 bucket

    """

    FO_BUCKET_ENDPOINT = "https://data.lit.fmi.fi"
    FO_BUCKET_NAME = "field-observatory"

    def __init__(self) -> None:
        s3 = boto3.resource(
            service_name="s3",
            endpoint_url=FOBucket.FO_BUCKET_ENDPOINT,
            config=Config(signature_version=UNSIGNED),
        )
        bucket = s3.Bucket(FOBucket.FO_BUCKET_NAME)
        self.s3 = s3
        self.bucket = bucket

    def list_files(self, prefix: str, return_key: bool = False) -> list:
        """
        List files in a given prefix

        Parameters
        ----------
        prefix : str
            Prefix of the files
        return_key : bool, optional
            If True, returns only the key, otherwise returns the full URL

        Returns
        -------
        files_or_keys : list
            List of files or keys

        """
        files_or_keys = []
        for object in self.bucket.objects.filter(Prefix=prefix):
            if return_key:
                files_or_keys.append(object.key)
            else:
                end_point = FOBucket.FO_BUCKET_ENDPOINT.split("https://")[1]
                object_url = f"https://{self.bucket.name}.{end_point}/{object.key}"
                files_or_keys.append(object_url)
        return files_or_keys

    # Event file handling
    def get_events(self, field_id: str) -> dict:
        """
        Get events for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        events : dict
            Events for the field
        """
        prefix = field_id.replace("_", "/")
        field_files = self.list_files(prefix, return_key=True)
        events_file = os.path.join(prefix, "events.json")
        if events_file in field_files:
            object = self.s3.Object(FOBucket.FO_BUCKET_NAME, events_file)
            file_content = object.get()["Body"].read().decode("utf-8")
            events = json.loads(file_content)
        else:
            print(f"No events file for field {field_id}")
            events = None
        return events

    def get_harvest_dates(self, field_id: str) -> list:
        """
        Get harvest dates for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        harvest_dates : list
            Harvest dates for the field
        """
        events = self.get_events(field_id)
        if events is None:
            return []
        management_events = events["management"]["events"]
        harvest_dates = []
        for event in management_events:
            if event["mgmt_operations_event"] == "harvest":
                harvest_dates.append(event["date"])
        return harvest_dates

    def get_mowing_dates(self, field_id: str) -> list:
        """
        Get mowing dates for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        mowing_dates : list
            Mowing dates for the field
        """
        events = self.get_events(field_id)
        if events is None:
            return []
        management_events = events["management"]["events"]
        mowing_dates = []
        for event in management_events:
            if event["mgmt_operations_event"] == "mowing":
                mowing_dates.append(event["date"])
        return mowing_dates

    def get_species_events(self, field_id: str) -> list:
        """
        Get species events for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        species_events : list
            Species events for the field

        """
        events = self.get_events(field_id)
        if events is None:
            return []
        management_events = events["management"]["events"]
        species_events = []
        for event in management_events:
            if event["mgmt_operations_event"] == "harvest":
                species = event["harvest_crop"]
            elif event["mgmt_operations_event"] == "mowing":
                if "moved_crop" in event:
                    species = event["moved_crop"]
                else:
                    species = "-99.0"
            else:
                continue

            species_event = {
                "date": event["date"],
                "species": species,
                "event_type": event["mgmt_operations_event"],
            }
            species_events.append(species_event)

        return species_events

    def get_harvest_amounts(self, field_id: str) -> list:
        """
        Get harvest amounts for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        harvest_amounts : list
            Harvest amounts for the field

        """
        events = self.get_events(field_id)
        if events is None:
            return []
        management_events = events["management"]["events"]
        harvest_amounts = []

        for event in management_events:
            if event["mgmt_operations_event"] == "harvest":
                if "harvest_yield_harvest_dw_total" in event:
                    amount = event["harvest_yield_harvest_dw_total"]
                elif "harvest_yield_harvest_dw" in event:
                    amount = event["harvest_yield_harvest_dw"]
                else:
                    continue

                if (amount == -99.0) or (amount == "-99.0"):
                    amount = None

                harvest_amounts.append(amount)

        return harvest_amounts

    def get_harvest_info(self, field_id: str) -> list:
        """
        Get harvest information for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        harvest_events : list
            List of harvest events for the field

        """
        events = self.get_events(field_id)
        if events is None:
            return []
        management_events = events["management"]["events"]
        harvest_events = []
        for event in management_events:
            event_info = {}
            if event["mgmt_operations_event"] == "harvest":
                event_info["date"] = event["date"]
                if "harvest_yield_harvest_dw_total" in event:
                    event_info["amount"] = event["harvest_yield_harvest_dw_total"]
                elif "harvest_yield_harvest_dw" in event:
                    event_info["amount"] = event["harvest_yield_harvest_dw"]
                else:
                    event_info["amount"] = None

                if event_info["amount"] == "-99.0":
                    event_info["amount"] = None

                event_info["event_type"] = "harvest"

                harvest_events.append(event_info)

        return harvest_events

    def get_mowings_as_harvest_info(self, field_id: str) -> list:
        """
        Get mowing information for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        harvest_events : list
            List of mowing events for the field

        """
        events = self.get_events(field_id)
        if events is None:
            return []
        management_events = events["management"]["events"]
        harvest_events = []
        for event in management_events:
            event_info = {}
            if event["mgmt_operations_event"] == "mowing":
                event_info["date"] = event["date"]
                event_info["amount"] = None
                event_info["event_type"] = "mowing"
                harvest_events.append(event_info)

        return harvest_events

    def get_observation_events(self, field_id: str) -> list:
        """
        Get observation events for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        observation_events : list
            List of observation events for the field
        """
        events = self.get_events(field_id)
        if events is None:
            return []
        management_events = events["management"]["events"]
        observation_events = []
        for event in management_events:
            if event["mgmt_operations_event"] == "observation":
                observation_events.append(event)
        return observation_events

    def get_AGB_observations(self, field_id: str) -> list:
        """
        Get AGB observations for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        AGB_observations : list
            List of AGB observations for the field
        """
        observation_events = self.get_observation_events(field_id)
        AGB_observations = []
        if observation_events is not None:
            for event in observation_events:
                if ("observation_type" in event) and (
                    event["observation_type"] == "observation_type_vegetation"
                    and "tops_C" in event
                    and event["tops_C"] != "-99.0"
                ):
                    biomass_observation = {
                        "date": event["date"],
                        "AGB (gC/m2)": event["tops_C"],
                    }
                    AGB_observations.append(biomass_observation)
        return AGB_observations

    # Get harvest amount of specific date and field
    def get_harvest_amount(self, field_id: str, date: str) -> float:
        """
        Get harvest amount for a given field and date

        Parameters
        ----------
        field_id : str
            Field ID
        date : str
            Date

        Returns
        -------
        amount : float
            Harvest amount for the field and date
        """
        harvest_events = self.get_harvest_info(field_id)
        amount = None
        for event in harvest_events:
            if pd.to_datetime(event["date"]) == pd.to_datetime(date):
                amount = event["amount"]
                if amount == "-99.0":
                    amount = None
                else:
                    amount = event["amount"]
            else:
                amount = None
        return amount

    # Get event type of specific date and field
    def get_event_type(self, field_id: str, date: str) -> str:
        """
        Get event type for a given field and date

        Parameters
        ----------
        field_id : str
            Field ID
        date : str
            Date

        Returns
        -------
        event_type : str
            Event type for the field and date
        """
        events = self.get_events(field_id)
        if events is None:
            return []
        management_events = events["management"]["events"]
        event_type = None
        for event in management_events:
            # If no date, skip
            if "date" not in event:
                continue
            if pd.to_datetime(event["date"]) == pd.to_datetime(date):
                event_type = event["mgmt_operations_event"]
            else:
                continue
        return event_type

    # CSV Data handling
    def get_field_timeseries_data(self, field_id: str, data_type: str) -> pd.DataFrame:
        """
        Get timeseries data for a given field and data type

        Parameters
        ----------
        field_id : str
            Field ID
        data_type : str
            Data type

        Returns
        -------
        df : pd.DataFrame
            Dataframe of the timeseries data
        """
        prefix = field_id.replace("_", "/") + "/" + data_type
        df = self.get_timeseries_data(prefix, data_type)
        return df

    def get_site_timeseries_data(self, site_id: str, data_type: str) -> pd.DataFrame:
        """
        Get timeseries data for a given site and data type

        Parameters
        ----------
        site_id : str
            Site ID
        data_type : str
            Data type

        Returns
        -------
        df : pd.DataFrame
            Dataframe of the timeseries data
        """
        prefix = site_id + "/" + data_type
        df = self.get_timeseries_data(prefix, data_type)
        return df

    def get_timeseries_data(self, prefix: str) -> pd.DataFrame:
        """
        Get timeseries data for a given prefix

        Parameters
        ----------
        prefix : str
            Prefix

        Returns
        -------
        df : pd.DataFrame
            Dataframe of the timeseries data
        """
        files = csv_files(self.list_files(prefix))
        df = read_files_to_dataframe(files)
        return df

    def read_block_geojson(self) -> dict:
        """
        Read block geojson file from S3 bucket

        Returns
        -------
        block_json : dict
            Block geojson as dict
        """
        block_geojson = FO_BLOCK_GEOJSON
        object = self.s3.Object(FOBucket.FO_BUCKET_NAME, block_geojson)
        file_content = object.get()["Body"].read().decode("utf-8")
        block_json = json.loads(file_content)
        return block_json

    def get_field_information(self, field_id: str) -> dict:
        """
        Get field information for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        field_information : dict
            Field information for the field

        Raises
        ------
        ValueError
            If the field is not found

        """
        block_json = self.read_block_geojson()
        field_information = None
        for feature in block_json["features"]:
            if feature["properties"]["id"] == field_id:
                field_information = feature
                break
            else:
                continue
        if field_information is None:
            raise ValueError(f"Field {field_id} not found.")
        else:
            return field_information

    def get_field_geometry(self, field_id: str) -> dict:
        """
        Get field geometry for a given field

        Parameters
        ----------
        field_id : str
            Field ID

        Returns
        -------
        field_geometry : dict
            Field geometry for the field
        """
        field_information = self.get_field_information(field_id)
        field_geometry = field_information["geometry"]
        return field_geometry


def csv_files(files):
    return [f for f in files if f.endswith(".csv")]


def read_files_to_dataframe(files) -> pd.DataFrame:
    """
    Read files to dataframe

    Parameters
    ----------
    files : list
        List of files

    Returns
    -------
    df : pd.DataFrame
        Dataframe of the files
    """
    for i, f in enumerate(files):
        tmp_df = pd.read_csv(f)

        if i == 0:
            df = tmp_df
        else:
            df = pd.concat([df, tmp_df])
    time_col = df.columns[0]
    df[time_col] = pd.to_datetime(df[time_col])
    df.set_index(time_col, drop=True, inplace=True)
    return df
