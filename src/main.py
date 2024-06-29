"""
ELT script to extract SF crime data and load into GCS.
"""

from typing import Tuple

import pandas as pd
import pyarrow as pa
import requests
from google.cloud import bigquery, storage


def fetchDataToGCS(url: str, filename: str, bucket_name: str) -> None:
    """
    Use the Python requests library to download the data in CSV format and
    saved in the raw bucket in GCS.
    """
    # Fetch the request
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(f"raw/{filename}.csv")
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with blob.open(mode="wb") as file:
            for batch in response.iter_content(chunk_size=100 * 1024 * 1024):
                file.write(batch)
            # file.flush()
    csvToParquet(filename, bucket_name)
    blob.delete()


def csvToParquet(filename: str, bucket_name: str) -> None:
    """
    Use the Python pandas library to read the CSV formatted data and save as a
    parquet file in the parquet bucket in GCS.
    """
    df = pd.read_csv(
        f"gcs://{bucket_name}/raw/{filename}.csv",
        sep="|",
        parse_dates=[
            "Incident Datetime",
            "Incident Date",
            "Incident Time",
            "Report Datetime",
        ],
        date_format={
            "Incident Datetime": "%Y/%m/%d %I:%M:%S %p",
            "Incident Date": "%Y/%m/%d",
            "Incident Time": "%H:%M",
            "Report Datetime": "%Y/%m/%d %I:%M:%S %p",
        },
        # blocksize="10MB",
        # dtype={'Incident Datetime': 'object',
        #         'Incident Date': 'object',
        #         'Incident Time': 'object',
        #         'Incident Year': 'int8',
        #         'Incident Day of Week': 'object',
        #         'Report Datetime': 'object',
        #         'Row ID': 'int64',
        #         'Incident ID': 'int32',
        #         'Incident Number': 'int64',
        #         'CAD Number': 'float64',
        #         'Report Type Code': 'object',
        #         'Report Type Description': 'object',
        #         'Filed Online': 'object',
        #         'Incident Code': 'int32',
        #         'Incident Category': 'object',
        #         'Incident Subcategory': 'object',
        #         'Incident Description': 'object',
        #         'Resolution': 'object',
        #         'Intersection': 'object',
        #         'CNN': 'float64',
        #         'Police District': 'object',
        #         'Analysis Neighborhood': 'object',
        #         'Supervisor District': 'Int64',
        #         'Latitude': 'float32',
        #         'Longitude': 'float32',
        #         'Point': 'object',
        #         'Neighborhoods': 'Int64',
        #         'ESNCAG - Boundary File': 'Int64',
        #    'Central Market/Tenderloin Boundary Polygon - Updated': 'Int64',
        #         'Civic Center Harm Reduction Project Boundary': 'Int64',
        #         'HSOC Zones as of 2018-06-05': 'Int64',
        #         'Invest In Neighborhoods (IIN) Areas': 'Int64',
        #         'Current Supervisor Districts': 'Int64',
        #         'Current Police Districts': 'Int64'}
    )

    df.rename(
        columns={
            "Incident Datetime": "Incident_Datetime",
            "Incident Date": "Incident_Date",
            "Incident Time": "Incident_Time",
            "Incident Year": "Incident_Year",
            "Incident Day of Week": "Incident_Day_of_Week",
            "Report Datetime": "Report_Datetime",
            "Row ID": "Row_ID",
            "Incident ID": "Incident_ID",
            "Incident Number": "Incident_Number",
            "CAD Number": "CAD_Number",
            "Report Type Code": "Report_Type_Code",
            "Report Type Description": "Report_Type_Description",
            "Filed Online": "Filed_Online",
            "Incident Code": "Incident_Code",
            "Incident Category": "Incident_Category",
            "Incident Subcategory": "Incident_Subcategory",
            "Incident Description": "Incident_Description",
            # "Resolution": "Resolution",
            # "Intersection": "Intersection",
            # "CNN": "CNN",
            "Police District": "Police_District",
            "Analysis Neighborhood": "Analysis_Neighborhood",
            "Supervisor District": "Supervisor_District",
            "Supervisor District 2012": "Supervisor_District_2012",
            # "Latitude": "Latitude",
            # "Longitude": "Longitude",
            # "Point": "oint",
            # "Neighborhoods": "Neighborhoods",
            "ESNCAG - Boundary File": "ESNCAG_Boundary_File",
            "Central Market/Tenderloin Boundary Polygon - Updated": "Central_Market_Tenderloin_Boundary_Polygon_Updated",
            "Civic Center Harm Reduction Project Boundary": "Civic_Center_Harm_Reduction_Project_Boundary",
            "HSOC Zones as of 2018-06-05": "HSOC_Zones_as_of_2018_06_05",
            "Invest In Neighborhoods (IIN) Areas": "Invest_In_Neighborhoods_Areas",
            "Current Supervisor Districts": "Current_Supervisor_Districts",
            "Current Police Districts": "Current_Police_Districts",
        },
        inplace=True,
    )

    schema = pa.schema(
        [
            pa.field("Incident_Datetime", pa.timestamp("s", tz="America/Los_Angeles")),
            pa.field("Incident_Date", pa.date32()),
            pa.field("Incident_Time", pa.time32("s")),
            pa.field("Incident_Year", pa.int32()),
            pa.field("Incident_Day_of_Week", pa.string()),
            pa.field("Report_Datetime", pa.timestamp("s", tz="America/Los_Angeles")),
            pa.field("Row_ID", pa.int64()),
            pa.field("Incident_ID", pa.int32()),
            pa.field("Incident_Number", pa.int64()),
            pa.field("CAD_Number", pa.float64()),
            pa.field("Report_Type_Code", pa.string()),
            pa.field("Report_Type_Description", pa.string()),
            pa.field("Filed_Online", pa.bool_()),
            pa.field("Incident_Code", pa.int32()),
            pa.field("Incident_Category", pa.string()),
            pa.field("Incident_Subcategory", pa.string()),
            pa.field("Incident_Description", pa.string()),
            pa.field("Resolution", pa.string()),
            pa.field("Intersection", pa.string()),
            pa.field("CNN", pa.float64()),
            pa.field("Police_District", pa.string()),
            pa.field("Analysis_Neighborhood", pa.string()),
            pa.field("Supervisor_District", pa.int64()),
            pa.field("Supervisor_District_2012", pa.int64()),
            pa.field("Latitude", pa.float32()),
            pa.field("Longitude", pa.float32()),
            pa.field("Point", pa.string()),
            pa.field("Neighborhoods", pa.int64()),
            pa.field("ESNCAG_Boundary_File", pa.int64()),
            pa.field("Central_Market_Tenderloin_Boundary_Polygon_Updated", pa.int64()),
            pa.field("Civic_Center_Harm_Reduction_Project_Boundary", pa.int64()),
            pa.field("HSOC_Zones_as_of_2018_06_05", pa.int64()),
            pa.field("Invest_In_Neighborhoods_Areas", pa.int64()),
            pa.field("Current_Supervisor_Districts", pa.int64()),
            pa.field("Current_Police_Districts", pa.int64()),
        ]
    )

    df.to_parquet(
        f"gcs://{bucket_name}/{filename}.parquet", engine="pyarrow", schema=schema
    )


def createExternalTable(
    bigquery_client: bigquery.Client, filename: str, bucket_name: str
) -> None:

    table_id = f"sf-crime-data-pipeline.sf_crime_data_dataset.{filename}"
    job_config = bigquery.LoadJobConfig(
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    uri = f"gs://{bucket_name}/{filename}.parquet"

    # Make an API request.
    load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)

    load_job.result()  # Waits for the job to complete.


def createDashboardView(bigquery_client: bigquery.Client, table_name) -> None:
    view_id = "sf-crime-data-pipeline.sf_crime_data_dataset.Dashboard_View"
    base_table_id = f"sf-crime-data-pipeline.sf_crime_data_dataset.{table_name}"

    # Make an API request to create the materialized view.
    try:
        bigquery_client.get_table(view_id)
        print(f"Table {view_id} already exists.")
    except:
        view = bigquery.Table(view_id)
        view.mview_query = f"""
            SELECT fact_crime.`Incident_Description` AS IncidentDescription,
                    fact_crime.Latitude,
                    fact_crime.Longitude,
                    fact_crime.`Incident_Date` AS IncidentFullDate,
                    dim_date.year AS IncidentYear,
                    dim_date.month_name AS IncidentMonth,
                    dim_date.day_name AS IncidentDayOfWeek,
                    dim_time.hour24 AS IncidentHour,
                    dim_date.holiday_name AS IncidentHolidayName,
                    dim_date.is_weekday AS IncidentisWeekend,
                    fact_crime.`Incident_Time` AS IncidentFullTime12,
                    dim_time.timeOfDay AS IncidentTimeOfDay,
                    fact_crime.Intersection,
                    fact_crime.`Police_District` AS PoliceDistrict,
                    fact_crime.`Analysis_Neighborhood` AS AnalysisNeighborhood,
                    fact_crime.`Incident_Category` AS IncidentCategory,
                    fact_crime.`Incident_Subcategory` AS IncidentSubcategory,
                    fact_crime.`Report_Type_Description` AS ReportType
            FROM {base_table_id} fact_crime
            JOIN `sf-crime-data-pipeline.sf_crime_data_dataset.dim_date` dim_date
                ON fact_crime.`Incident_Date` = dim_date.date
            JOIN `sf-crime-data-pipeline.sf_crime_data_dataset.dim_time` dim_time
                ON fact_crime.`Incident_Time` = dim_time.fullTime24
        """
        view = bigquery_client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")


def main(request: str) -> Tuple[str, int]:
    url = "https://data.sfgov.org/api/views/wg3w-h783/rows.csv?accessType=DOWNLOAD&bom=false&format=false&delimiter=%7C"  # noqa: E501
    filename = "SFCrimeData2018toPresent"
    bucket_name = "sf_crime_data_lake_sf-crime-data-pipeline"
    bigquery_client = bigquery.Client()
    fetchDataToGCS(url, filename, bucket_name)
    for file in [filename, "dim_date", "dim_time"]:
        createExternalTable(bigquery_client, file, bucket_name)
    createDashboardView(bigquery_client, filename)
    return "Done!", 200


# if __name__ == '__main__':
#     main()
