from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load data from Redshift staging table into a dimensional table.

    Args:
        redshift_conn_id: Redshift connection ID
        dim_table: Dimension table name
        sql_query: A SELECT statement to fetch the data to insert
        append: Data will be appended to the table if True, or table be truncated before load if set to False
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dim_table='',
                 sql_query='',
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.dim_table = dim_table
        self.sql_query = sql_query
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append:
            self.log.info(f"Clearing dimension table {self.dim_table}")
            redshift.run(f"DELETE FROM {self.dim_table}")

        sql = f"INSERT INTO {self.dim_table} {self.sql_query}"
        self.log.info(f"Loading dimension table {self.dim_table}")
        redshift.run(sql)
