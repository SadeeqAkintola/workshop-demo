# beam_pipeline.py
# --- Import necessary libraries ---
import argparse
import csv
import datetime
import logging
import os
import apache_beam as beam
from apache_beam.io.fileio import MatchFiles, ReadMatches, ReadableFile
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions

# --- Configuration Variables ---
GCP_PROJECT_ID = 'python-airflow-beam-workflow'
GCS_RESOURCES_BUCKET = 'py-demo'
BQ_DATASET = 'py_demo'
BQ_TABLE = 'registrations'
GCP_REGION = 'europe-west2' # Used primarily for Dataflow job region

# --- Construct URIs and Table Specs ---
INPUT_PATTERN = f'gs://{GCS_RESOURCES_BUCKET}/initiated-runs/*.csv'
STAGING_LOCATION = f'gs://{GCS_RESOURCES_BUCKET}/staging' # Needed for Dataflow
TEMP_LOCATION = f'gs://{GCS_RESOURCES_BUCKET}/temp' # Needed for Dataflow AND BQ Loads
BIGQUERY_TABLE_SPEC = f'{GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}'

# Define BigQuery Table Schema
TABLE_SCHEMA = {
    'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'file_location', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}
# --- End Configuration Variables ---

# --- Define Processing Logic ---
class ReadAndValidateCSV(beam.DoFn):
    """Reads ReadableFile, validates CSV, yields valid rows."""
    def process(self, element: ReadableFile):
        file_path = element.metadata.path
        logging.info(f"Beam pipeline processing file: {file_path}")
        try:
            with element.open(compression_type=beam.io.filesystem.CompressionTypes.AUTO) as f:
                content = f.read().decode('utf-8-sig')
                if not content.strip():
                    logging.warning(f"Skipping empty file: {file_path}")
                    return
                lines = content.splitlines()
                reader = csv.reader(lines)
                header = next(reader, None)
                if not header or len(header) != 3:
                    logging.error(f"Skipping invalid file: Bad header in {file_path}")
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
                        logging.warning(f"Invalid row #{i+1} in file {file_path}. Skipping.")
        except UnicodeDecodeError as e:
             logging.error(f"Error decoding file {file_path}: {e}. Skipping.")
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}", exc_info=True)

class AddTimestamp(beam.DoFn):
    """Adds a processing timestamp."""
    def process(self, element: dict):
        element['timestamp'] = datetime.datetime.now(datetime.timezone.utc)
        yield element

# --- Define and Run the Pipeline ---
def run(argv=None, save_main_session=True):
    """Defines and runs the Beam pipeline, correctly handling runner selection and options."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_pattern', default=INPUT_PATTERN)
    parser.add_argument('--output_table', default=BIGQUERY_TABLE_SPEC)
    parser.add_argument('--staging_location', default=STAGING_LOCATION)
    parser.add_argument('--temp_location', default=TEMP_LOCATION) # Keep temp_location arg
    parser.add_argument('--region', default=GCP_REGION)
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Initialize PipelineOptions with all parsed arguments (including --runner)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=save_main_session)
    actual_runner = pipeline_options.view_as(StandardOptions).runner
    logging.info(f"Pipeline will use runner: {actual_runner or 'Default (likely DirectRunner)'}")

    # --- Set options based on parsed args and runner ---
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)

    # Project and Temp Location are needed by BQ Sink (FILE_LOADS) even for DirectRunner
    google_cloud_options.project = GCP_PROJECT_ID
    google_cloud_options.temp_location = known_args.temp_location
    if not google_cloud_options.temp_location:
         # Should not happen with defaults, but as a safeguard
         raise ValueError("Pipeline option --temp_location is required.")

    # Set Dataflow-specific options ONLY if DataflowRunner is explicitly chosen or defaulted
    # (Beam default might be DirectRunner if DataflowRunner is not installed/specified)
    if actual_runner == 'DataflowRunner':
        logging.info("Configuring additional options for DataflowRunner...")
        google_cloud_options.region = known_args.region
        google_cloud_options.staging_location = known_args.staging_location
        google_cloud_options.job_name = (
            f"pipeline-triggered-in-beam-at-"
            f"{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        )
        #pipeline_options.view_as(WorkerOptions).use_public_ips = False
        # Ensure required locations are set for Dataflow
        if not google_cloud_options.staging_location:
             raise ValueError("Pipeline option --staging_location is required for DataflowRunner.")
        if not google_cloud_options.region:
             google_cloud_options.region = GCP_REGION # Fallback to default if not provided
             logging.warning(f"Dataflow runner selected but no region provided, defaulting to {GCP_REGION}")
    else:
        logging.info("Skipping Dataflow-specific options (staging_location, region, job_name, use_public_ips).")
        google_cloud_options.region = None
        google_cloud_options.staging_location = None
        google_cloud_options.job_name = None
        # Ensure use_public_ips is explicitly None if not Dataflow
        #pipeline_options.view_as(WorkerOptions).use_public_ips = None


    # --- Log the Effective Configuration ---
    logging.info(f"Effective Pipeline Options:")
    all_options = pipeline_options.get_all_options()
    for opt, val in sorted(all_options.items()): # Sort for consistent logging
        if opt not in ['pickle_library', 'pipeline_type_check', 'save_main_session', 'job_name']: # Reduce verbosity
             # Don't log job_name again if None
             if val is not None:
                 logging.info(f"  {opt}: {val}")

    # --- Define the Pipeline Structure ---
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'MatchFiles' >> MatchFiles(known_args.input_pattern)
            | 'ReadMatches' >> ReadMatches()
            | 'ReadAndValidate' >> beam.ParDo(ReadAndValidateCSV())
            | 'AddProcessingTimestamp' >> beam.ParDo(AddTimestamp())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=TABLE_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND
                # Beam will use temp_location set in pipeline_options here
            )
        )
    logging.info("Pipeline definition complete. Runner will now execute.")

# --- Main Execution Block ---
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()