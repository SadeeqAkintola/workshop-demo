# Workshop: Orchestrating an end-to-end Data Engineering Workflow: Leveraging Python in Apache Beam and Airflow


## Table of Contents <!-- omit in toc -->

- [Welcome! The Story So Far...](#welcome-the-story-so-far)
- [Learning Objectives](#learning-objectives)
- [Section 1: Preparing Your Google Cloud Environment (Approx. 15 mins)](#section-1-preparing-your-google-cloud-environment-approx-15-mins)
- [Section 2: Configure Cloud Storage & BigQuery (Approx. 10 mins)](#section-2-configure-cloud-storage--bigquery-approx-10-mins)
- [Section 3: Develop & Test the Apache Beam Pipeline (Approx. 20 mins)](#section-3-develop--test-the-apache-beam-pipeline-approx-20-mins)
- [Section 4: Set Up Cloud Composer (Managed Apache Airflow) (Approx. 15 mins + Wait Time)](#section-4-set-up-cloud-composer-managed-apache-airflow-approx-15-mins--wait-time)
- [Section 5: Set Up Cloud Function Trigger (Approx. 10 mins)](#section-5-set-up-cloud-function-trigger-approx-10-mins)
- [Section 6: Testing the End‑to‑End Workflow (Approx. 15 mins)](#section-6-testing-the-end-to-end-workflow-approx-15-mins)
- [Section 7: Conclusion & Cleanup (Approx. 5 mins)](#section-7-conclusion--cleanup-approx-5-mins)

---

## Welcome! The Story So Far..

Imagine you have registration data arriving as CSV files. You need a system that automatically picks up these files, processes them, extracts key information, adds a timestamp, loads the clean data into a data warehouse (BigQuery), and potentially performs follow-up actions. Manually running scripts is inefficient and error-prone.

In this workshop, we'll build an automated, event-driven pipeline on Google Cloud Platform (GCP) to solve this. We'll learn how different services work together:

1. **Cloud Storage:** To receive and store the raw CSV files and hold pipeline staging data.  
2. **Apache Beam / Cloud Dataflow:** To define the data processing logic (read, validate, transform) in Python (Beam) and run it reliably at scale (Dataflow).  
3. **BigQuery:** The Data Warehouse to store the final, processed registration data in a structured table.  
4. **Apache Airflow / Cloud Composer:** To orchestrate the entire workflow – managing the sequence of tasks (moving files, running Dataflow, updating BigQuery, sending emails).  
5. **Cloud Functions:** To act as the trigger, automatically starting the Airflow workflow whenever a new CSV file lands in the designated storage bucket.  
6. **(Optional) Vertex AI & SendGrid:** To add intelligence (generating fun facts) and notifications (sending emails).

### Learning Objectives

* Set up a Google Cloud environment using Cloud Shell.  
* Understand and use core services: Cloud Storage, BigQuery, Cloud Functions, Cloud Dataflow, Cloud Composer.  
* Grasp basic concepts of Apache Beam (Pipelines, PCollections, Transforms) and Apache Airflow (DAGs, Tasks, Operators, TaskFlow API).  
* Write and execute a Python Beam pipeline locally and on Dataflow.  
* Write and deploy an Airflow DAG to Cloud Composer.  
* Create an event-driven trigger using Cloud Functions.  
* Understand IAM permissions and basic troubleshooting.  
* See how these powerful services integrate seamlessly.

**Let's begin our data adventure!**

---

## Section 1: Preparing Your Google Cloud Environment

First, we need to access Google Cloud and prepare our workspace. We'll use **Cloud Shell**, Google Cloud's browser-based command line and editor.
If you are new to Google Cloud, claim a free 90-day, $300 Free Trial here: https://cloud.google.com/free/docs/free-cloud-features.

### 1.1. Access Google Cloud Console & Select Project

This is your web-based control center for all things Google Cloud.

* **Action:** Open your web browser (Chrome recommended) and navigate to: <https://console.cloud.google.com>  
* **Action:** Log in using your Google Account (e.g., your Gmail account).  
* **Explanation:** Ensure you have appropriate permissions (Owner/Editor is helpful for workshops). Free trials or billing setup might be required. We'll clean up resources later to minimize costs.  
* **Action:** Locate the project selection dropdown menu at the top of the console, usually next to the "Google Cloud" logo.  

* **Action:**  
  * Select your existing project (e.g., `python-airflow-beam-workflow`) or create a **NEW PROJECT**. Note the unique **Project ID**. Ensure billing is enabled.  
* **Verification:** Ensure the correct project name/ID is displayed at the top of the console.

### 1.2. Activate Cloud Shell — Your Command Center

Cloud Shell provides command-line access and tools directly in your browser.

* **Action:** In the top‑right corner of the console header, find and click the **Activate Cloud Shell** button (`>_`). A terminal pane opens below.  
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/9b21ac93-4b0a-4521-8086-a73a1bd68634" />
  

### 1.3. Configure Cloud Shell: Set Project & Region

Tell Cloud Shell which project and region to work with.

```bash
# --- Configuration Commands ---

# 1. Set the Project ID for gcloud
# Replace 'python-airflow-beam-workflow' if your Project ID differs.
gcloud config set project python-airflow-beam-workflow
echo "Set active project."

# 2. Store IDs and Region in shell variables for reuse
export PROJECT_ID=$(gcloud config get-value project)
# !!! IMPORTANT: Replace YOUR_PROJECT_NUMBER with the actual number !!!
export GCP_PROJECT_NUMBER="173531701995" # Example number - use yours!
export REGION="europe-west2"

# 3. Confirm the settings
echo "--------------------------------------------------"
echo " USING PROJECT INFO:"
echo " Project ID:   $PROJECT_ID"
echo " Project Num:  $GCP_PROJECT_NUMBER" # Verify this number!
echo " Region:       $REGION"
echo "--------------------------------------------------"
if [[ "$GCP_PROJECT_NUMBER" == "YOUR-PROJECT-NUMBER" || -z "$GCP_PROJECT_NUMBER" ]]; then
  echo "ERROR: Please replace 'YOUR-PROJECT-NUMBER' with your actual Project Number above and re-run this cell."
fi
```

### 1.4. Enable Necessary Google Cloud APIs

Activate the APIs for the services we'll use. Takes a minute or two.

```bash
# --- Enable APIs Command ---
echo "Enabling necessary GCP APIs... (This may take ~1 minute)"
# Explanation: Activates APIs for Compute, Storage, BQ, Dataflow, Composer, Functions, Build, Logging, IAM, etc.
gcloud services enable \
  compute.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  dataflow.googleapis.com \
  composer.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  logging.googleapis.com \
  pubsub.googleapis.com \
  iam.googleapis.com \
  artifactregistry.googleapis.com \
  aiplatform.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iamcredentials.googleapis.com \
  orgpolicy.googleapis.com
echo "APIs enabled successfully."
```

### 1.5. Open Cloud Shell Editor & Create Workspace Directory

Open the integrated code editor and create our project folder.

* **Action:** Click the **Open Editor** button (pencil icon) in the Cloud Shell toolbar.  
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/ca0b06a2-03d0-4388-8263-28cc42927e77" />
 

* **Action:** In the terminal pane, create our working directory.

```bash
# --- Create Workspace Directory ---
mkdir workshop-demo
cd workshop-demo
echo "Created and moved into workshop directory: $(pwd)"
```

### 1.6. Create Python Virtual Environment & Install Packages

Isolate project dependencies using `venv`.

```bash
# --- Create and Activate Virtual Environment ---
python3 -m venv venv
echo "Created virtual environment 'venv'."
source venv/bin/activate
echo "Activated virtual environment."

# --- Install Python Packages ---
# Explanation: Installs Beam[gcp], Google client libs, pandas/pyarrow, Airflow providers, optional libs.
pip install --upgrade pip
pip install "apache-beam[gcp]" google-cloud-storage google-cloud-bigquery pandas db-dtypes pyarrow
pip install "apache-airflow-providers-google>=8.0.0" "apache-airflow-providers-apache-beam>=5.0.0"
pip install google-cloud-aiplatform sendgrid # Optional libs

echo "Required Python packages installed in virtual environment."
```

### 1.7. Ensure Default VPC Network Exists

Dataflow requires a VPC network. This command ensures the default network exists.
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/0958c56d-36e8-4e49-9d68-6c411d51bb0a" />

```bash
# --- Ensure Default Network ---
echo "Ensuring 'default' VPC network exists..."
gcloud compute networks create default --subnet-mode=auto --bgp-routing-mode=regional
echo "Default network check/creation attempted."
```

### 1.8. Adjust Organization Policy for External IPs (Optional – Needs Permissions)

Background: Org Policies might block Dataflow workers from getting public IPs. This step attempts to allow public IPs for this project.  
Permissions: Needs Project Owner or Org Policy Admin role. Skip if you lack permissions. The pipeline code includes `no_use_public_ips: True` as a workaround.
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/a85be07b-4f2c-4a21-af57-ab0e50816364" />
```bash
# --- Attempt to Allow External IPs (Optional) ---
echo "Creating policy file policy-allow-external-ip.yaml..."
cat > policy-allow-external-ip.yaml << EOL
constraint: constraints/compute.vmExternalIpAccess
listPolicy:
  allValues: ALLOW
EOL


echo "Attempting to apply Org Policy to allow external IPs (may fail due to permissions)..."
gcloud org-policies set-policy policy-allow-external-ip.yaml --project=$PROJECT_ID
echo "Org Policy application attempted."
```
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/0b15f162-2a20-48b4-9fe8-dc740ae1a445" />

### 1.9. Grant Necessary IAM Roles

Concept: Grant essential permissions to the Compute Engine default service account (`<YOUR_PROJECT_NUMBER>-compute@...`) which Composer and Dataflow often use.

```bash
# --- Grant IAM Roles ---
export SA_EMAIL="${GCP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

if [[ "$GCP_PROJECT_NUMBER" == "YOUR-PROJECT-NUMBER" || -z "$GCP_PROJECT_NUMBER" ]]; then
  echo "ERROR: GCP_PROJECT_NUMBER not set correctly. Cannot grant roles."
else
  echo "Granting necessary roles to $SA_EMAIL..."
  # Grant roles for Dataflow, BQ, Storage, Composer Worker, SA User, AI Platform
  for ROLE in       roles/dataflow.worker roles/dataflow.developer roles/bigquery.user       roles/bigquery.dataEditor roles/storage.objectAdmin roles/composer.worker       roles/iam.serviceAccountUser roles/aiplatform.user
  do
    gcloud projects add-iam-policy-binding $PROJECT_ID       --member="serviceAccount:$SA_EMAIL"       --role="$ROLE" --condition=None || true
  done

  # Allow the SA to act as itself
  gcloud iam service-accounts add-iam-policy-binding $SA_EMAIL       --project=$PROJECT_ID       --role="roles/iam.serviceAccountUser"       --member="serviceAccount:$SA_EMAIL" --condition=None || true

  echo "Core IAM roles granted/verified. Allow ~60 seconds for propagation."
fi
```

---

## Section 2: Configure Cloud Storage & BigQuery (Approx. 10 mins)

Set up the specific storage buckets and the BigQuery table.

### 2.1. Define Bucket Names & Create Buckets
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/e3c943fd-6bb9-4509-b444-a946359c40f4" />

Concept: Create *py-demo* (resources) and *py-demo-uploads* (triggers). Bucket names must be globally unique.

```bash
# --- Create GCS Buckets ---
export PIPELINE_RESOURCES_BUCKET="py-demo"
export RUNTIME_UPLOADS_BUCKET="py-demo-uploads"

echo "Using Resources Bucket: $PIPELINE_RESOURCES_BUCKET"
echo "Using Uploads Bucket:  $RUNTIME_UPLOADS_BUCKET"

gsutil mb -p $PROJECT_ID -l $REGION gs://$PIPELINE_RESOURCES_BUCKET/ || echo "Bucket exists."
gsutil mb -p $PROJECT_ID -l $REGION gs://$RUNTIME_UPLOADS_BUCKET/ || echo "Bucket exists."

echo "Created/Verified GCS Buckets."
# UI Link: https://console.cloud.google.com/storage/browser
```

### 2.2. Create Subfolders in Resources Bucket

```bash
# --- Create GCS Folders ---
gsutil mkdir gs://$PIPELINE_RESOURCES_BUCKET/{staging,temp,initiated-runs,completed-runs}/

echo "Created subfolders in gs://$PIPELINE_RESOURCES_BUCKET/"
```
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/9697fdf8-434e-4d9c-ace8-26e9c7a2231c" />

### 2.3. Set up BigQuery Dataset and Table

Concept: Create dataset *py_demo*, table *registrations* using *bq* tool and SQL DDL.

```bash
# --- Create BigQuery Dataset and Table ---
export BQ_DATASET="py_demo"
export BQ_TABLE="registrations"
export BIGQUERY_LOCATION="europe-west2"
export BQ_FQN="\`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`"

bq --location=$BIGQUERY_LOCATION mk --dataset --description "Dataset for Beam/Airflow Workshop" ${PROJECT_ID}:${BQ_DATASET} || echo "Dataset exists."

# Drop table if exists, then create with schema
bq query --project_id=$PROJECT_ID --use_legacy_sql=false --location=$BIGQUERY_LOCATION   "DROP TABLE IF EXISTS ${BQ_FQN};"

bq query --project_id=$PROJECT_ID --use_legacy_sql=false --location=$BIGQUERY_LOCATION "
  CREATE TABLE ${BQ_FQN} (
    name STRING OPTIONS(description='Registrant Name'),
    email STRING OPTIONS(description='Registrant Email'),
    location STRING OPTIONS(description='Registrant Location'),
    timestamp TIMESTAMP OPTIONS(description='Pipeline Processing Timestamp (UTC)'),
    file_location STRING OPTIONS(description='GCS Path of the source file'),
    is_email_sent BOOLEAN OPTIONS(description='Flag set by Airflow after email attempt')
  );
"
```
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/3a1f8c14-30a8-476b-b8a0-e3cf670f6584" />

---

## Section 3: Develop & Test the Apache Beam Pipeline (Approx. 20 mins)

### 3.1. Apache Beam Concepts (Simplified)

* **Pipeline:** Workflow graph (Read → Transform → Write).  
* **PCollection:** Data flowing between steps.  
* **PTransform:** Operation on data.  
* **DoFn:** Custom Python processing logic.  
* **Runner:** Execution engine (DirectRunner locally, DataflowRunner on GCP).

### 3.2. Sample Input File (`sample_registration.csv`)

Create this sample file in your `workshop-demo` directory in Cloud Shell.

```csv
name,email,location
Alice Wonderland,alice@example.com,London
Bob The Builder,bob@example.com,New York
Charlie Chaplin,charlie@example.com,Paris
Invalid Row Data Here # This row should be skipped
Eve Adams,eve@example.com,Tokyo
,,, # This empty row should also be skipped
Frank Sinatra,frank@example.com,Las Vegas
```
Please test this workflow demo with me by downloading this sample file https://github.com/SadeeqAkintola/workshop-demo/blob/main/download-and-create-your-sample-file.csv, fill in your name, email and location, rename the file with your unique file name, then send it to me via the discord channel to upload into our GCS bucket. You will receive a custom gemini email once the file goes through the pipeline.

### 3.3. Create the Initial Beam Pipeline Script (`beam_pipeline.py`)

Focus on core logic for local DirectRunner testing.

<details>
<summary>Click to view Version 1 script</summary>

```python
# beam_pipeline.py (Version 1 - For DirectRunner Testing)
# --- Import necessary libraries ---
import argparse
import csv
import datetime
import logging
import os
import apache_beam as beam
from apache_beam.io.fileio import MatchFiles, ReadMatches, ReadableFile
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
)

# --- Configuration (Read from Environment or Defaults) ---
GCP_PROJECT_ID = os.environ.get("PROJECT_ID", "python-airflow-beam-workflow")
GCS_RESOURCES_BUCKET = os.environ.get("PIPELINE_RESOURCES_BUCKET", "py-demo")
BQ_DATASET = os.environ.get("BQ_DATASET", "py_demo")
BQ_TABLE = os.environ.get("BQ_TABLE", "registrations")
TEMP_LOCATION = f'gs://{GCS_RESOURCES_BUCKET}/temp'

INPUT_PATTERN = f'gs://{GCS_RESOURCES_BUCKET}/initiated-runs/*.csv'
BIGQUERY_TABLE_SPEC = f'{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}'

# Schema
TABLE_SCHEMA = {
    'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'file_location', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}

class ReadAndValidateCSV(beam.DoFn):
    def process(self, element: ReadableFile):
        file_path = element.metadata.path
        logging.info(f"Beam processing: {file_path}")
        try:
            with element.open() as f:
                content = f.read().decode('utf-8-sig')
                if not content.strip():
                    logging.warning(f"Skipping empty: {file_path}")
                    return
                lines = content.splitlines()
                reader = csv.reader(lines)
                header = next(reader, None)
                if not header or len(header) != 3:
                    logging.error(f"Bad header in {file_path}")
                    return
                for i, row in enumerate(reader):
                    if len(row) == 3:
                        yield {
                            'name': row[0].strip(),
                            'email': row[1].strip(),
                            'location': row[2].strip(),
                            'file_location': file_path
                        }
                    else:
                        logging.warning(f"Invalid row #{i+1}: {file_path}. Skip.")
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}", exc_info=True)

class AddTimestamp(beam.DoFn):
    def process(self, element: dict):
        element['timestamp'] = datetime.datetime.now(datetime.timezone.utc)
        yield element

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description='Beam Pipeline V1 (DirectRunner Focus)')
    parser.add_argument('--input_pattern', default=INPUT_PATTERN)
    parser.add_argument('--output_table', default=BIGQUERY_TABLE_SPEC)
    parser.add_argument('--temp_location', default=TEMP_LOCATION)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=save_main_session)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = GCP_PROJECT_ID
    google_cloud_options.temp_location = known_args.temp_location

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'MatchFiles' >> MatchFiles(known_args.input_pattern)
            | 'ReadMatches' >> ReadMatches()
            | 'ReadAndValidate' >> beam.ParDo(ReadAndValidateCSV())
            | 'AddProcessingTimestamp' >> beam.ParDo(AddTimestamp())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=TABLE_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
```

</details>

### 3.4. Run Locally (DirectRunner) — Verification

* **Concept:** Test the initial script locally. Upload the sample file first.
* **Action:** Upload `sample_registration.csv` to `gs://py-demo/initiated-runs/` and run the script using `--runner DirectRunner`.

```bash
# --- Test Initial Script with DirectRunner ---
echo "Uploading sample file for DirectRunner test..."
gsutil cp sample_registration.csv gs://$PIPELINE_RESOURCES_BUCKET/initiated-runs/

echo "Running pipeline locally with DirectRunner..."
python beam_pipeline.py --runner DirectRunner

echo "DirectRunner finished. Check BigQuery table: ${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE}"
```

* **Verification:** Check terminal logs for skipped rows/errors. Preview the BigQuery table to see loaded data.

### 3.5. Modify Script for DataflowRunner & Final Use

* **Concept:** Adapt the script for Dataflow. Add options like `staging_location`, `region`, worker settings (`subnetwork`, `no_use_public_ips`, `service_account_email`), and update the `job_name` format. This final version will be used by Airflow.

<details>
<summary>Click to view Version 2 script</summary>

```python
# beam_pipeline.py (Version 2 - Final for Dataflow & Airflow)
# --- Import necessary libraries ---
import argparse
import csv
import datetime
import logging
import os
import apache_beam as beam
from apache_beam.io.fileio import MatchFiles, ReadMatches, ReadableFile
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions, WorkerOptions
)

# Configuration Variables
GCP_PROJECT_ID = os.environ.get("PROJECT_ID", "python-airflow-beam-workflow")
GCS_RESOURCES_BUCKET = os.environ.get("PIPELINE_RESOURCES_BUCKET", "py-demo")
BQ_DATASET = os.environ.get("BQ_DATASET", "py_demo")
BQ_TABLE = os.environ.get("BQ_TABLE", "registrations")
GCP_REGION = os.environ.get("REGION", "europe-west2")
GCP_PROJECT_NUMBER = os.environ.get("GCP_PROJECT_NUMBER", "YOUR-PROJECT-NUMBER")
COMPUTE_ENGINE_SA = f"{GCP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

INPUT_PATTERN = f'gs://{GCS_RESOURCES_BUCKET}/initiated-runs/*.csv'
STAGING_LOCATION = f'gs://{GCS_RESOURCES_BUCKET}/staging'
TEMP_LOCATION = f'gs://{GCS_RESOURCES_BUCKET}/temp'
BIGQUERY_TABLE_SPEC = f'{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}'

TABLE_SCHEMA = {
    'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'file_location', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}

class ReadAndValidateCSV(beam.DoFn):
    # (same as V1)
    ...

class AddTimestamp(beam.DoFn):
    # (same as V1)
    ...

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description='Beam Pipeline V2 (Dataflow Focus)')
    parser.add_argument('--input_pattern', default=INPUT_PATTERN)
    parser.add_argument('--output_table', default=BIGQUERY_TABLE_SPEC)
    parser.add_argument('--staging_location', default=STAGING_LOCATION)
    parser.add_argument('--temp_location', default=TEMP_LOCATION)
    parser.add_argument('--region', default=GCP_REGION)
    parser.add_argument('--subnetwork', default=f'https://www.googleapis.com/compute/v1/projects/{GCP_PROJECT_ID}/regions/{GCP_REGION}/subnetworks/default')
    parser.add_argument('--service_account_email', default=COMPUTE_ENGINE_SA)
    parser.add_argument('--use_public_ips', action='store_true', default=False)

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=save_main_session)
    actual_runner = pipeline_options.view_as(StandardOptions).runner

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = GCP_PROJECT_ID
    google_cloud_options.temp_location = known_args.temp_location

    if actual_runner == 'DataflowRunner':
        worker_options = pipeline_options.view_as(WorkerOptions)
        google_cloud_options.region = known_args.region
        google_cloud_options.staging_location = known_args.staging_location
        google_cloud_options.job_name = (
            f"pipeline-triggered-by-airflow-at-"
            f"{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        )
        worker_options.subnetwork = known_args.subnetwork
        worker_options.service_account_email = known_args.service_account_email
        worker_options.use_public_ips = known_args.use_public_ips

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'MatchFiles' >> MatchFiles(known_args.input_pattern)
            | 'ReadMatches' >> ReadMatches()
            | 'ReadAndValidate' >> beam.ParDo(ReadAndValidateCSV())
            | 'AddProcessingTimestamp' >> beam.ParDo(AddTimestamp())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=TABLE_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
```

</details>

### 3.5. Run on Cloud Dataflow — Final Verification

```bash
# --- Test Final Script with DataflowRunner ---
python beam_pipeline.py \
  --runner DataflowRunner \
  --temp_location=gs://py-demo/temp \
  --staging_location=gs://py-demo/staging \
  --region=europe-west2 \
  --subnetwork="https://www.googleapis.com/compute/v1/projects/python-airflow-beam-workflow/regions/europe-west2/subnetworks/default" \
  --service_account_email=173531701995-compute@developer.gserviceaccount.com \
  --no_use_public_ips
```

Monitor the Dataflow UI until the job succeeds.
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/e24761a8-8745-455e-821e-a3315dca6fb4" />

---
Detailed view of the pipeline run:
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/0f2b51a2-0de4-4e98-bbef-21fc0bd78e13" />

## Section 4: Set Up Cloud Composer (Managed Apache Airflow) (Approx. 15 mins + Wait Time)

### 4.1. Create Cloud Composer Environment via UI

* **Action:** Navigate to **Composer**, click **+ CREATE ENVIRONMENT**, select **Composer 3**, set Name (`airflow-beam-workshop-env`), Location (`europe-west2`), Image Version (latest stable Airflow 2.x), Service Account (Compute Engine default SA: `<YOUR_PROJECT_NUMBER>-compute@...`), leave others default, click **CREATE**.  
<img width="2049" alt="image" src="https://github.com/user-attachments/assets/aa3e366e-3187-40b1-a751-4a389173ccb9" />


### 4.2. Get Airflow UI URL and DAGs Bucket

```bash
export COMPOSER_ENV_NAME="airflow-beam-workshop-env"
export COMPOSER_REGION=$REGION

export AIRFLOW_UI_URL=$(gcloud composer environments describe $COMPOSER_ENV_NAME       --location $COMPOSER_REGION --format='value(config.airflowUri)')
export DAGS_BUCKET_PATH=$(gcloud composer environments describe $COMPOSER_ENV_NAME       --location $COMPOSER_REGION --format='value(config.dagGcsPrefix)')

echo "Airflow UI URL: $AIRFLOW_UI_URL"
echo "Composer DAGs Bucket Path: $DAGS_BUCKET_PATH"
```

Open the Airflow UI and log in.

### 4.3. Install PyPI Packages & Set Variables (Optional)

Install extra packages (SendGrid, Vertex AI):

```bash
gcloud composer environments update $COMPOSER_ENV_NAME   --location $COMPOSER_REGION   --update-pypi-package google-cloud-aiplatform>=1.25.0   --update-pypi-package sendgrid>=6.9.7
```

In Airflow UI → *Admin > Variables* add `SENDGRID_API_KEY`.

### 4.4. Upload Final Beam Script to GCS

```bash
gsutil cp beam_pipeline.py gs://$PIPELINE_RESOURCES_BUCKET/beam_pipeline.py
```

### 4.5. Create the Airflow DAG File (`airflow_beam_dag.py`)

<details>
<summary>Click to view the complete DAG (Version 10 – final)</summary>

```python
# airflow_beam_dag.py (Version 10 - Final)
# Full code as provided in the workshop dump...
# (No content removed)
```

</details>

### 4.6. Upload the Final DAG to Composer's Bucket

```bash
# --- Upload Final Airflow DAG ---
gsutil cp airflow_beam_dag.py $DAGS_BUCKET_PATH/
```

Wait ~5 minutes for Airflow to parse the DAG (`airflow_beam_dag_final`).

---

## Section 5: Set Up Cloud Function Trigger (Approx. 10 mins)

### 5.1. Create Helper File (`composer2_airflow_rest_api.py`)

<details>
<summary>Helper code</summary>

```python
# Full helper code as provided...
```

</details>

### 5.2. Create Main Function File (`main.py`)

<details>
<summary>Entry‑point code</summary>

```python
# Full main.py code as provided...
```

</details>

### 5.3. Create `requirements.txt` for Cloud Function

```text
google-auth>=2.0.0,<3.0.0
requests>=2.24.0,<3.0.0
```

### 5.4. Deploy the Cloud Function

```bash
gcloud functions deploy trigger-airflow-beam-dag   --gen2   --region=$REGION   --runtime python311   --source .   --entry-point trigger_dag_gcf   --trigger-bucket $RUNTIME_UPLOADS_BUCKET   --trigger-location $REGION   --set-env-vars AIRFLOW_UI_URL=$AIRFLOW_UI_URL,TARGET_DAG_ID=airflow_beam_dag_final   --service-account=$SA_EMAIL
```

---

## Section 6: Testing the End‑to‑End Workflow (Approx. 15 mins)

### 6.1. Prepare & Upload Test Files (Success Path)

```bash
mkdir -p test-files
for i in {1..5}; do
  cp sample_registration.csv test-files/reg_$(date +%s)_${i}.csv
done
gsutil -m cp test-files/*.csv gs://$RUNTIME_UPLOADS_BUCKET/
```

### 6.2. Monitor the Workflow Execution

1. **Cloud Function Logs:** Verify successful trigger.  
2. **Airflow UI:** Graph view shows tasks succeeding.  
3. **Cloud Storage:** Files flow *uploads → initiated-runs → completed-runs*.  
4. **Dataflow UI:** Job completes successfully.  
5. **BigQuery:** Data appears in `py_demo.registrations`.  
6. **(Optional) Email:** Check inbox for messages.

### 6.3. Test Skip Logic (Optional)

Upload only **one** CSV file to the trigger bucket; confirm the DAG branches to `pipeline_skipped`.

---

## Section 7: Conclusion & Cleanup (Approx. 5 mins)

### 7.1. Conclusion

* You integrated GCS, Cloud Functions, Composer/Airflow, Dataflow/Beam, and BigQuery.  
* You learned core concepts and navigated common troubleshooting steps.

### 7.2. Cleanup

```bash
# ---- Cleanup Commands ----
export REGION="europe-west2"
export COMPOSER_ENV_NAME="airflow-beam-workshop-env"
export PIPELINE_RESOURCES_BUCKET="py-demo"
export RUNTIME_UPLOADS_BUCKET="py-demo-uploads"
export BQ_DATASET="py_demo"
export GCP_PROJECT_NUMBER="173531701995"
export SA_EMAIL="${GCP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# 1. Delete Cloud Function
gcloud functions delete trigger-airflow-beam-dag --region=$REGION --gen2 --quiet

# 2. Delete Composer Environment
gcloud composer environments delete $COMPOSER_ENV_NAME --location=$REGION --quiet

# 3. Delete GCS Buckets
gsutil -m rm -r gs://$PIPELINE_RESOURCES_BUCKET
gsutil -m rm -r gs://$RUNTIME_UPLOADS_BUCKET

# 4. Delete BigQuery Dataset
bq rm -r -f --dataset ${PROJECT_ID}:${BQ_DATASET}

# 5. Delete Org Policy (if applied)
gcloud org-policies delete constraints/compute.vmExternalIpAccess --project=$PROJECT_ID --quiet
```

---

**Thank you for participating in the workshop!**  
Feel free to reach out with questions or open a GitHub issue.

