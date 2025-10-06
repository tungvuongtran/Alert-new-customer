# dags/weekly_new_customer_account_check.py
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.email import EmailOperator

# ---- Your shared failure callback (import your real one if available) ----
# from common.alerts import send_failure_email
def send_failure_email(context):
    try:
        error_response = {
            'batch_unique_id': 'None',
            'job_name': 'None',
            'error': f"""<b>DAG</b>: {context['dag'].dag_id}<br/>
                         <b>Task</b>: {context['task_instance'].task_id}<br/>
                         <b>Execution Date</b>: {context['execution_date']}<br/>
                         <b>Log URL</b>: {context['task_instance'].log_url}<br/>
                      """
        }
        project_id = context.get('project', '')
        email_sender = context.get('email_sender', '')
        email_sender_password = ''
        receiver_email_list = context.get('email_receiver_list', [])
        job_params = {
            'dag_id': context['dag'].dag_id,
            'task_id': context['task_instance'].task_id,
            'email_sender': email_sender,
            'email_app_pwd': email_sender_password,
            'email_receiver_list': receiver_email_list,
            'response': [error_response]
        }
        response = trigger_email_dag_error(job_params)  # noqa: F821  (your internal function)
        print(f"Email trigger response from send_failure_email - {response}")
    except BaseException as be:
        print(f"DAG errored! Exception triggering failure email - {str(be)}")


SQL_DETECTION = """
SELECT DISTINCT
  UPPER(customer_account_no) AS customer_account_no_cap,
  customer_account_no,
  AccountNo
FROM `sp-prd-gdt-udp-datalake.parcel_tracking_curated.parcel_mail_journey_curated` cc
LEFT JOIN `sp-prd-gdt-udp-datalake.parcel_tracking_curated.dim_customermapping` cd
  ON cc.customer_account_no = cd.AccountNo
WHERE
  partition_yearmonth >= 202401
  AND (UPPER(customer_account_no) = customer_account_no)
  AND cd.AccountNo IS NULL
ORDER BY customer_account_no
"""

def _df_to_html_table(df: pd.DataFrame, limit: int = 50) -> str:
    if df.empty:
        return "<p>No rows to display.</p>"
    trimmed = df.head(limit)
    # Safe HTML table for email (no external CSS)
    headers = "".join(f"<th>{h}</th>" for h in trimmed.columns)
    body_rows = []
    for _, row in trimmed.iterrows():
        tds = "".join(f"<td>{'' if pd.isna(v) else v}</td>" for v in row.tolist())
        body_rows.append(f"<tr>{tds}</tr>")
    tbody = "".join(body_rows)
    return f"""
    <table border="1" cellpadding="6" cellspacing="0">
      <thead><tr>{headers}</tr></thead>
      <tbody>{tbody}</tbody>
    </table>
    """

def detect_new_accounts(**context):
    """
    Run the detection SQL and push results to XCom:
      - row_count (int)
      - html_preview (str)
    """
    bq_conn_id = context["params"]["bq_conn_id"]
    project_id = context["params"]["project_id"]
    preview_limit = int(context["params"].get("preview_limit", 50))

    hook = BigQueryHook(gcp_conn_id=bq_conn_id, use_legacy_sql=False, location=context["params"].get("bq_location"))
    df = hook.get_pandas_df(SQL_DETECTION)  # runs in the project configured by the connection

    row_count = len(df.index)
    print(f"[detect_new_accounts] Found {row_count} rows.")
    html_preview = _df_to_html_table(df, limit=preview_limit)

    # Push to XCom for downstream tasks
    ti = context["ti"]
    ti.xcom_push(key="row_count", value=row_count)
    ti.xcom_push(key="html_preview", value=html_preview)

    # Also keep a CSV snapshot in logs (optional)
    if row_count > 0:
        csv_sample = df.head(preview_limit).to_csv(index=False)
        print("CSV Preview (first rows):\n" + csv_sample)

    return row_count

def has_new_rows(**context) -> bool:
    """Short-circuit downstream when there is nothing to notify."""
    ti = context["ti"]
    row_count = int(ti.xcom_pull(task_ids="detect_new_accounts", key="row_count") or 0)
    print(f"[has_new_rows] row_count={row_count}")
    return row_count > 0


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": send_failure_email,
}

with DAG(
    dag_id="weekly_new_customer_account_check",
    description="Weekly BigQuery check for new customer_account_no not in dim_customermapping; email Data Analytics Team if found.",
    start_date=datetime(2025, 10, 5, 9, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")),  # first Sunday ref (no pendulum)
    schedule_interval="0 9 * * SUN",  # Every Sunday 09:00 Asia/Ho_Chi_Minh
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["monitoring", "parcel-tracking", "curated"],
    params={
        # ---- adjust to your environment ----
        "project_id": "sp-prd-gdt-udp-datalake",
        "bq_conn_id": "google_cloud_default",  # Airflow GCP connection ID
        "bq_location": "asia-southeast1",      # optional
        "preview_limit": 50,

        # For your failure-callback, if it reads these from context:
        "email_sender": "no-reply@dataplatform.yourdomain",
        "email_receiver_list": ["data-analytics-team@yourdomain"],
        "project": "sp-prd-gdt-udp-datalake",
    },
) as dag:

    detect_new = PythonOperator(
        task_id="detect_new_accounts",
        python_callable=detect_new_accounts,
        provide_context=True,
    )

    new_rows_gate = ShortCircuitOperator(
        task_id="has_new_rows",
        python_callable=has_new_rows,
        provide_context=True,
    )

    notify_team = EmailOperator(
        task_id="email_data_analytics_team",
        to="{{ params.email_receiver_list }}",  # list is OK; Airflow will join
        subject="[Monitoring] New customer accounts not mapped to dim_customermapping",
        html_content="""
        <p><b>Environment</b>: {{ params.project_id }}</p>
        <p><b>DAG</b>: {{ dag.dag_id }}</p>
        <p><b>Task</b>: {{ task.task_id }}</p>
        <p><b>Execution Date</b>: {{ ts }}</p>
        <p><b>Row count</b>: {{ ti.xcom_pull(task_ids='detect_new_accounts', key='row_count') }}</p>
        <p><b>Preview</b>:</p>
        {{ ti.xcom_pull(task_ids='detect_new_accounts', key='html_preview') | safe }}
        """,
    )

    detect_new >> new_rows_gate >> notify_team
