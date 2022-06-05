from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id='',
                 aws_credentials_id='',
                 destination_table='',
                 s3_bucket='',
                 s3_key='',
                 file_extension = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.destination_table = destination_table
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_extension = file_extension

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id =self.conn_id)
        
        self.log.info("Truncate destination table in Redshift.")
        
        redshift.run(f'TRUNCATE {self.destination_table};')

        self.log.info('Copy data from S3 to Redshift.')

        generated_key = self.s3_key.format(**context)

        #Process 2 files: log-data & song-data. And load these files to tables.
        if self.s3_key == "song-data":
            s3_path =f's3://{self.s3_bucket}/{generated_key}'
        else:
            s3_key = f'{self.s3_key}/{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json'
            s3_path =f's3://{self.s3_bucket}/{self.s3_key}'
        sql_statement = ''' 
                                COPY {}
                                FROM '{}'
                                ACCESS_KEY_ID '{}'
                                SECRET_ACCESS_KEY '{}'
                                FORMAT AS JSON 'auto';
                             '''.format(
                                    self.destination_table, 
                                    s3_path,
                                    credentials.access_key,
                                    credentials.secret_key
                                 )
        redshift.run(sql_statement)
        self.log.info('Complete loading data from S3 into Redshift tables.')






