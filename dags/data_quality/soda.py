import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"

def yt_elt_dq(schema):
    try:
        task = BashOperator(
            task_id = f"soda_test{schema}",
            bash_command = f"soda scan -d{DATASOURCE} -c {SODA_PATH}/config.yml -v SCHEMA={schema} {SODA_PATH}/checks.yml"
        )
        return task
    except Exception as e:
        logger.error(f"Error running dq checks for SCHEMA = {schema}")
        raise e
