from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, conn_id = '', table_name = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('Start data quality.')
        redshift_conn = PostgresHook(postgres_conn_id = self.conn_id)
        records = redshift_conn.get_records(f'SELECT COUNT(*) FROM {self.table_name}')
        

        self.log.info('Checking the # of records.')
        if len(records) < 1 or len(records[0]) < 1:
            self.log.info(f'DataQuality Check Failed : Table {self.table_name} returned no results.')
            raise ValueError(f'DataQuality Check Failed : Table {self.table_name} returned no results.')

        num_of_records = records[0][0]
        if num_of_records < 1:
            self.log.info(f'DataQuality Check Failed : Table {self.table_name} is empty.')
            raise ValueError(f'DataQuality Check Failed : Table {self.table_name} is empty.')

        self.log.info(f'DataQuality Check Passed : Table {self.table_name} contains {num_of_records} records.')

        self.log.info('Completed checking data quality.')
        