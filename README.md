# SFCrimeDataPipeline
The City of San Francisco provides a daily updated report with incidents filed with SFPD's reporting system. This data pipeline automates the ELT process into a dimensional model ready for data analysis querying.

## Table of Contents
  * [Extract Load Tansform](#extract-load-transform)
    * [Data Pipeline Diagram](#data-pipeline-diagram)
    * [Dimensional Model](#dimensional-model)
  * [Dashboard](#dashboard)
  * [Development](#development)
    * [Built With](#built-with)
    * [Setup](#setup)
      * [terraform.tfvars File](#terraform.tfvars-file)
      * [Run Terraform](#run-terraform)
  * [Authors](#authors)

## Extract Load Transform
* Extract - Download the data from [Police Department Incident Reports: 2018 to Present](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783) dataset as CSV format.
* Load - Copy data from CSV file into a staging table in Postgres database.
* Transform - The raw data is transformed into a [dimensional model](#dimensional-model) with a star schema design.

### Data Pipeline Diagram
![SFCrimePipeline](https://user-images.githubusercontent.com/60832092/198851862-ff51a26a-1849-4f08-aee5-f833f8535fcb.png)

### Dimensional Model
![SFCrimeDimensionalModel](https://user-images.githubusercontent.com/60832092/198851851-318ad2c6-3342-435b-aadf-f15abb5b53fc.png)

## Dashboard
Tableau Public [dashboard](https://public.tableau.com/app/profile/pat3330/viz/SFCrimeData_16407224575150/Story?publish=yes).

## Development
These instructions will get you a copy of the project up and running for development and deployment.

### Built With
* [Python 3.10](https://docs.python.org/3/) - The scripting language used.
  * [gcsfs](https://gcsfs.readthedocs.io/en/latest/) - Python module used to read from Google Cloud Storage.
  * [Pandas](https://pandas.pydata.org/) - Python data manipulation library used to read CSV file.
  * [Pyarrow](https://arrow.apache.org/docs/python/index.html) - Python API for Apache Arrow used to write parquet format files.
  * [Requests](https://docs.python-requests.org/en/latest/) - Python HTTP library used to fetch the data.
* [Terraform](https://www.terraform.io/) - Infrastructure as code (IAC) software tool used to automate building and changing Google Cloud platform configurations.
* [Google Cloud Platform](https://cloud.google.com/) - Google based suite of cloud computing services used.
  * [Google BigQuery](https://cloud.google.com/bigquery) - Fully managed, serverless data warehouse used for analytical queries.
  * [Google Cloud Storage](https://cloud.google.com/storage) - Cloud based object storage used to store ingested data.
  * [Google Cloud Functions](https://cloud.google.com/functions) - Serverless execution environment used to run the data pipeline.
  * [Google Cloud Scheduler](https://cloud.google.com/scheduler) - Cron job scheduler used to schedule and trigger the data pipeline to run.

### Setup
#### GCP Setup
1. Create a [Google Cloud project](https://console.cloud.google.com/projectcreate) in Google Cloud console.
2. Turn on APIs for the service account:
  * BigQuery API
  * BigQuery Migration API
  * BigQuery Storage API
  * Cloud Build API
  * Cloud Datastore API
  * Cloud Debugger API
  * Cloud Functions API
  * Cloud Logging API
  * Cloud Monitoring API
  * Cloud Pub/Sub API
  * Cloud Scheduler API
  * Cloud SQL
  * Cloud Storage
  * Cloud Storage API
  * Cloud Trace API
  * Container Registry API
  * Google Cloud APIs
  * Google Cloud Storage JSON API
  * Legacy Cloud Source Repositories API
  * Service Management API
  * Service Usage API
3. In Identity and Access Management (IAM), create a service account
  * BigQuery Admin
  * Cloud Functions Admin
  * Cloud Scheduler Admin
  * Service Account User
  * Storage Admin
  * Storage Object Admin
  * Viewer
4. Download the service account keys JSON file to the parent directory of the project on the local development machine.

#### terraform.tfvars File
Create a `terraform.tfvars` file under the `Terraform` directory of the project (in the same directory as the `main.tf` file):
```
project = "sf-crime-data-pipeline"
region = "us-west1"
bucket_name = "sf-crime-data"
storage_class = "STANDARD"
BQ_DATASET = "sf_crime_data_dataset"
TABLE_NAME = "sf_crime_data"
credentials = "sf-crime-data-pipeline.json"
```
* Notes: Change the region and unique names for your own project. For the credentials, replace with full path to the GCP service account credentials used to build the project.

#### Run Terraform
`terraform.tfvars` file configurations can be changed to suit your needs.

Run the following command to preview modifications or plan that would be applied later:
```
terraform plan
```
Run the following command to create, build, or apply changes to the project in GCP:
```
terraform apply
```
Run the following command to delete the project from GCP:
```
terraform destroy
```

## Authors
* **Patrick Yu** - *Initial work* - [patrickgods1](https://github.com/patrickgods1)