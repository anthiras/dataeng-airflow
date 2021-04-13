from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Load data from Redshift staging table into a fact table.

    Args:
        redshift_conn_id: Redshift connection ID
        fact_table: Fact table name
        sql_query: A SELECT statement to fetch the data to insert
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 fact_table='',
                 sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        sql = f"INSERT INTO {self.fact_table} {self.sql_query}"

        self.log.info(f"Loading fact table {self.fact_table}")
        redshift.run(sql)
