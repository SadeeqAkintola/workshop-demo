# airflow_beam_dag.py

import os
import logging
from datetime import datetime, timedelta
import datetime as dt

from airflow.decorators import dag, task, task_group
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException

# Optional libs
try:
    import sendgrid
    from sendgrid.helpers.mail import Mail
    import vertexai
    from vertexai.generative_models import GenerativeModel, SafetySetting
    from google.api_core import exceptions as gexc
    SENDGRID_VERTEX_INSTALLED = True
except ImportError:
    SENDGRID_VERTEX_INSTALLED = False
    logging.warning("SendGrid / VertexAI unavailable – email task will be skipped.")

# ------------ CONFIG ------------
GCP_PROJECT_ID     = "python-airflow-beam-workflow"
GCP_PROJECT_NUMBER = "173531701995"
GCP_REGION         = "europe-west2"

BQ_TABLE_FQN       = "`python-airflow-beam-workflow.py_demo.registrations`"
BIGQUERY_LOCATION  = "EU"

PIPELINE_RESOURCES_BUCKET = "py-demo"
RUNTIME_UPLOADS_BUCKET    = "py-demo-uploads"
BEAM_PYTHON_SCRIPT_PATH   = f"gs://{PIPELINE_RESOURCES_BUCKET}/beam_pipeline.py"

MIN_FILES_TO_TRIGGER = 5
FILE_MATCH_PATTERN   = "*.csv"

SENDGRID_API_KEY_VAR_NAME = "SENDGRID_API_KEY"
SENDER_EMAIL              = "datatalkswithsadeeq@gmail.com"
VERTEX_MODEL_ID           = "gemini-1.0-pro"

COMPUTE_SA = f"{GCP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# ------------ DEFAULT ARGS ------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

# ------------ TASKS ------------
@task.branch(task_id="check_files_branch")
def check_files_branch(**kw) -> str:
    files = GCSHook().list(RUNTIME_UPLOADS_BUCKET, match_glob=FILE_MATCH_PATTERN) or []
    if len(files) >= MIN_FILES_TO_TRIGGER:
        kw["ti"].xcom_push("files_to_process", files)
        return "move_files_to_initiated_runs"
    return "pipeline_skipped"

@task(task_id="move_files_to_initiated_runs")
def move_files(src_bucket: str, dst_bucket: str, dst_prefix: str, **kw) -> list:
    files = kw["ti"].xcom_pull(task_ids="check_files_branch", key="files_to_process")
    if not files:
        raise AirflowSkipException("No files.")
    gcs, moved = GCSHook(), []
    for obj in files:
        dst_obj = os.path.join(dst_prefix, os.path.basename(obj))
        gcs.rewrite(src_bucket, obj, dst_bucket, dst_obj)
        gcs.delete(src_bucket, obj)
        moved.append(f"gs://{dst_bucket}/{dst_obj}")
    return moved

# Gemini helper
def describe_location(location: str) -> str:
    try:
        vertexai.init(project=GCP_PROJECT_ID, location=GCP_REGION)
        model = GenerativeModel(VERTEX_MODEL_ID)
        resp = model.generate_content(
            f"Tell me some fun facts about {location} in 150 words",
            generation_config={"max_output_tokens": 8192, "temperature": 1.0},
            safety_settings=[SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE)],
            stream=False,
        )
        return resp.text.strip()
    except (gexc.GoogleAPIError, Exception) as e:
        logging.warning(f"Gemini unavailable for {location}: {e}")
        return f"{location} is an amazing place with a rich history!"

# ---------- TG: query + email ----------
@task_group(group_id="process_registrations_and_notify")
def process_registrations_and_notify():

    @task(task_id="query_new")
    def query_new():
        sql = f"""
            SELECT name, email, location
            FROM {BQ_TABLE_FQN}
            WHERE (is_email_sent IS NULL OR is_email_sent = FALSE)
              AND email IS NOT NULL AND email <> ''
            LIMIT 1000
        """
        df = BigQueryHook(
            gcp_conn_id="google_cloud_default",
            project_id=GCP_PROJECT_ID,
            use_legacy_sql=False,
            location=BIGQUERY_LOCATION,
        ).get_pandas_df(sql=sql, dialect="standard", location=BIGQUERY_LOCATION)
        recs = df.to_dict("records")
        if not recs:
            raise AirflowSkipException("No new registrations.")
        return recs

    @task(task_id="send_comprehensive_emails")
    def send_emails(records: list):
        if not SENDGRID_VERTEX_INSTALLED:
            raise AirflowSkipException("SendGrid/VertexAI libs missing.")

        sg = sendgrid.SendGridAPIClient(Variable.get(SENDGRID_API_KEY_VAR_NAME))
        successes = []

        for r in records:
            name, email, location = r["name"], r["email"], r["location"]
            funfacts = describe_location(location)
            bullets = "".join(
                f"<li style='margin-bottom:4px'>{s.strip()}</li>"
                for s in funfacts.split(".") if s.strip()
            ) or "<li>Gemini was a bit shy today— but your city is awesome!</li>"

            html_body = f"""
            <html>
              <body style="font-family:Arial,Helvetica,sans-serif; color:#222; line-height:1.6">
                <h2 style="color:#0a84ff">👋 Hey {name}!</h2>

                <p>Thanks for joining my <strong>PyCon Lithuania 2025</strong> workshop!
                   Missed a part? Watch the replay
                   <a href="https://pycon.lt/talks/MZ8DBC" target="_blank">here</a>.
                   Full demo source lives on
                   <a href="https://github.com/SadeeqAkintola/beam-summit-2024-airflow" target="_blank">
                   GitHub</a> – feel free to ⭐ and share.</p>

                <p style="background:#f6f8fa; padding:12px; border-left:4px solid #0a84ff">
                  ✅ Your CSV containing <strong>{email}</strong> has been processed and loaded.
                </p>

                <h3 style="margin-top:28px">✨ Fun facts about {location}</h3>
                <ul style="padding-left:20px">{bullets}</ul>

                <hr style="border:none; border-top:1px solid #eee; margin:32px 0">

                <p><strong>Stay in the loop</strong> →
                   <a href="https://x.com/SadeeqAkintola" target="_blank">@SadeeqAkintola</a></p>

                <p style="font-size:90%; color:#666">
                  see you in the data streams,<br>
                  <strong>Sadeeq</strong> 🚀
                </p>
              </body>
            </html>
            """

            plain_body = (
                f"Hey {name}!\n\n"
                "Thanks for attending my PyCon Lithuania 2025 workshop.\n"
                "Replay: https://pycon.lt/talks/MZ8DBC\n\n"
                f"Your CSV with {email} was processed.\n\n"
                "Fun facts about {location}:\n  - " +
                "\n  - ".join(s.strip() for s in funfacts.split('.') if s.strip()) +
                "\n\nStar the repo: https://github.com/SadeeqAkintola/beam-summit-2024-airflow\n"
                "Follow me on X: https://x.com/SadeeqAkintola\n\n"
                "see you in the data streams,\nSadeeq 🚀"
            )

            try:
                resp = sg.send(
                    Mail(
                        from_email=SENDER_EMAIL,
                        to_emails=email,
                        subject="🚀 PyCon Lithuania 2025 Workshop Follow-Up!",
                        plain_text_content=plain_body,
                        html_content=html_body,
                    )
                )
                if 200 <= resp.status_code < 300:
                    successes.append(email)
            except Exception as e:
                logging.warning(f"SendGrid fail {email}: {e}")

        if not successes:
            raise AirflowSkipException("No successful emails.")

    send_emails(query_new())

# -------- BigQuery flag update (replaced) --------
UPDATE_SQL = f"""
UPDATE {BQ_TABLE_FQN}
SET is_email_sent = TRUE
WHERE (is_email_sent IS NULL OR is_email_sent = FALSE)
  AND email <> ""
"""

# Will be instantiated inside the DAG definition below:

# Cleanup task
@task(task_id="move_files_to_completed", trigger_rule=TriggerRule.ALL_SUCCESS)
def move_to_completed(src: str, dst: str, src_prefix: str, dst_prefix: str):
    gcs = GCSHook()
    for obj in gcs.list(src, prefix=src_prefix, match_glob="*.csv") or []:
        dst_obj = os.path.join(dst_prefix, os.path.basename(obj))
        gcs.rewrite(src, obj, dst, dst_obj)
        gcs.delete(src, obj)

# -------------------- DAG ---------------------
@dag(
    dag_id="airflow_beam_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
def airflow_beam_dag():

    start   = DummyOperator(task_id="start")
    decide  = check_files_branch()
    skip    = DummyOperator(task_id="pipeline_skipped")

    moved = move_files(
        src_bucket=RUNTIME_UPLOADS_BUCKET,
        dst_bucket=PIPELINE_RESOURCES_BUCKET,
        dst_prefix="initiated-runs",
    )

    run_df = BeamRunPythonPipelineOperator(
        task_id="run_beam_pipeline",
        py_file=BEAM_PYTHON_SCRIPT_PATH,
        runner="DataflowRunner",
        gcp_conn_id="google_cloud_default",
        deferrable=True,
        dataflow_config=DataflowConfiguration(
            project_id=GCP_PROJECT_ID,
            location=GCP_REGION
        ),
        pipeline_options={
            "job_name": f"pipeline-{dt.datetime.utcnow():%Y%m%d-%H%M%S}",
            "temp_location": f"gs://{PIPELINE_RESOURCES_BUCKET}/temp",
            "staging_location": f"gs://{PIPELINE_RESOURCES_BUCKET}/staging",
            "no_use_public_ips": True,
            "service_account_email": COMPUTE_SA,
        },
    )

    notify = process_registrations_and_notify()

    # 🔄 Replace old Python task with BigQueryInsertJobOperator
    update_bigquery_email_flag = BigQueryInsertJobOperator(
        task_id="update_bigquery_email_flag",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": UPDATE_SQL,
                "useLegacySql": False,
            }
        },
        location=BIGQUERY_LOCATION,
    )

    cleanup = move_to_completed(
        src=PIPELINE_RESOURCES_BUCKET,
        dst=PIPELINE_RESOURCES_BUCKET,
        src_prefix="initiated-runs", dst_prefix="completed-runs"
    )

    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ----- define dependencies -----
    start >> decide
    decide >> skip >> end
    decide >> moved >> run_df >> notify >> update_bigquery_email_flag >> cleanup >> end

airflow_beam_dag()