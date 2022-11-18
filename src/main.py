import pandas as pd
import pyarrow as pa
import requests
from google.cloud import bigquery, storage


def fetchDataToGCS(url, filename, bucket_name):
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
            for batch in response.iter_content(chunk_size=10 * 1024 * 1024):
                file.write(batch)
            # file.flush()
    csvToParquet(filename, bucket_name)
    blob.delete()


def csvToParquet(filename, bucket_name):
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
        infer_datetime_format=True,  # blocksize="10MB",
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

    schema = pa.schema(
        [
            pa.field("Incident Datetime", pa.timestamp("s", tz="America/Los_Angeles")),
            pa.field("Incident Date", pa.date32()),
            pa.field("Incident Time", pa.time32("s")),
            pa.field("Incident Year", pa.int32()),
            pa.field("Incident Day of Week", pa.string()),
            pa.field("Report Datetime", pa.timestamp("s", tz="America/Los_Angeles")),
            pa.field("Row ID", pa.int64()),
            pa.field("Incident ID", pa.int32()),
            pa.field("Incident Number", pa.int64()),
            pa.field("CAD Number", pa.float64()),
            pa.field("Report Type Code", pa.string()),
            pa.field("Report Type Description", pa.string()),
            pa.field("Filed Online", pa.bool_()),
            pa.field("Incident Code", pa.int32()),
            pa.field("Incident Category", pa.string()),
            pa.field("Incident Subcategory", pa.string()),
            pa.field("Incident Description", pa.string()),
            pa.field("Resolution", pa.string()),
            pa.field("Intersection", pa.string()),
            pa.field("CNN", pa.float64()),
            pa.field("Police District", pa.string()),
            pa.field("Analysis Neighborhood", pa.string()),
            pa.field("Supervisor District", pa.int64()),
            pa.field("Latitude", pa.float32()),
            pa.field("Longitude", pa.float32()),
            pa.field("Point", pa.string()),
            pa.field("Neighborhoods", pa.int64()),
            pa.field("ESNCAG - Boundary File", pa.int64()),
            pa.field(
                "Central Market/Tenderloin Boundary Polygon - Updated", pa.int64()
            ),
            pa.field("Civic Center Harm Reduction Project Boundary", pa.int64()),
            pa.field("HSOC Zones as of 2018-06-05", pa.int64()),
            pa.field("Invest In Neighborhoods (IIN) Areas", pa.int64()),
            pa.field("Current Supervisor Districts", pa.int64()),
            pa.field("Current Police Districts", pa.int64()),
        ]
    )

    df.to_parquet(
        f"gcs://{bucket_name}/{filename}.parquet", engine="pyarrow", schema=schema
    )


def createExternalTable(filename, bucket_name):
    client = bigquery.Client()
    table_id = f"sf-crime-data-pipeline.sf_crime_data_dataset.{filename}"
    job_config = bigquery.LoadJobConfig(
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    uri = f"gs://{bucket_name}/{filename}.parquet"

    # Make an API request.
    client.load_table_from_uri(uri, table_id, job_config=job_config)

    # load_job.result()  # Waits for the job to complete.


def main(request):
    url = "https://data.sfgov.org/api/views/wg3w-h783/rows.csv?accessType=DOWNLOAD&bom=false&format=false&delimiter=%7C"  # noqa: E501
    filename = "SFCrimeData2018toPresent"
    bucket_name = "sf_crime_data_lake_sf-crime-data-pipeline"
    fetchDataToGCS(url, filename, bucket_name)
    for filename in [filename, "dim_date", "dim_time"]:
        createExternalTable(filename, bucket_name)
    return "Done!", 200


# if __name__ == '__main__':
#     main()
