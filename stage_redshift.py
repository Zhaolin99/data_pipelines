from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'


    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                aws_credentials_id='',
                table='',
                s3_bucket='',
                json_path='',
                region='',
                truncate=False,
                *args,**kwargs):
        super(StageToRedshiftOperator, self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.json_path = json_path
        self.region = region
        self.truncate = truncate

    def execute(self, context):
        self.log.info('StageToRedshiftOperator is executing')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f'Truncate the Redshift table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
        

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket,
            credentials.access_key,
            credentials.secret_key,
            self.json_path,
            self.region
        )
        
        redshift.run(formatted_sql)

