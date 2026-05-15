from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from loguru import logger


class SaaSDatabricksRunNowOperator(DatabricksRunNowOperator):
    """
    SaaS-specific Databricks operator that extends DatabricksRunNowOperator.
    Provides custom logging and explicit XCom push of run_id.
    """

    def __init__(self, *args, **kwargs):
        self._saas_job_id = kwargs.get('job_id')  # store separately, don't override parent
        super().__init__(*args, **kwargs)

    def execute(self, context):
        logger.info(f"Triggering Databricks Job ID: {self._saas_job_id}")

        result = super().execute(context)

        run_id = context['ti'].xcom_pull(task_ids=context['ti'].task_id, key='run_id')
        if not run_id:
            run_id = result

        if not run_id:
            raise ValueError(f"Failed to get run_id for job {self._saas_job_id}")

        logger.success(f"Job {self._saas_job_id} triggered. Run ID: {run_id}")

        context['ti'].xcom_push(key='return_value', value=run_id)
        return run_id