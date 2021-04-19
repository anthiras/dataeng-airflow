from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Perform data quality checks on Redshift tables using SQL queries.

    Args:
        redshift_conn_id: Redshift connection ID
        quality_checks: List of quality checks. Each item in the list is a tuple of (SQL query string, expected result)
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for query, expected_result in self.quality_checks:
            records = redshift.get_records(query)[0]
            if records[0] != expected_result:
                raise ValueError(f"The following data quality check failed with result {expected_result}, expected result {expected_result}: {query}")
            self.log.info(f"The following data quality check passed with expected result {expected_result}: {query}")