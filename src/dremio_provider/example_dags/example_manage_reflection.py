import datetime

from airflow import DAG
from dremio_provider.operators.dremio import DremioCreateReflectionOperator

with DAG(
    dag_id="dremio_reflection_manage",
    description="Example DAG to create or update reflections in dremio",
    catchup=False,
    start_date=datetime.datetime(2024, 1, 1),
    tags=["DREMIO"],
) as dag:
    reflection_task = DremioCreateReflectionOperator(
        task_id="reflection_manage",
        dremio_conn_id="dremio_default",
        auto_inference=True,
        source="source.schema.table",
        reflection_spec={"type": "RAW", "name": "raw_reflection", "enabled": True},
        wait_for_completion=True,
    )
