from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class StageToRedshiftOperator(BaseOperator):
   ui_color = '#358140'


   @apply_defaults
   def __init__(self,
               redshift_conn_id='',
               aws_credentials_id='',
               table='',
               s3_bucket='',
               json_path='',
               *args, **kwargs):


       super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
       self.redshift_conn_id = redshift_conn_id
       self.aws_credentials_id = aws_credentials_id
       self.table = table          
       self.s3_bucket = s3_bucket
       self.json_path = json_path


   def execute(self, context):
       self.log.info(f"Staging {self.table} to Redshift")


       # Establish connections to S3 and Redshift
       s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
       redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)


       # Clear existing data from the staging table
       self.log.info(f"Clearing data from destination Redshift table {self.table}")
       redshift_hook.run(f"DELETE FROM {self.table}")


       # Copy data from S3 to Redshift
       self.log.info(f"Copying data from S3 to Redshift table {self.table}")
       s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
       copy_sql = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{s3_hook.get_credentials().access_key}' SECRET_ACCESS_KEY '{s3_hook.get_credentials().secret_key}' JSON '{self.json_path}'"
       redshift_hook.run(copy_sql)


