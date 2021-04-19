from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Stage data from S3 into Redshift staging tables.

    Args:
        redshift_conn_id: Redshift connection ID
        aws_credentials_id: AWS credentials ID
        table: Staging table name
        s3_bucket: S3 bucket to fetch data from
        s3_key: S3 key for the object/object prefix to fetch, which may contain templated context parameters, e.g. year/month
        json: Optional json_option to use for the Redshift COPY command, default is 'auto'
        region: The AWS region
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json="auto",
                 region='us-west-2',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.region = region

    def execute(self, context):
        """Copy data from S3 into the destination Redshift table"""
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json,
            self.region
        )
        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
        redshift.run(sql)