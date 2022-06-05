from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, 
                 conn_id = '', 
                 destination_table = '',
                 load_query = '',
                 truncate_before_load = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = conn_id
        self.destination_table = destination_table
        self.load_query = load_query
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id = self.conn_id)

            
        if self.truncate_before_load:
            self.log.info(f'If truncate_before_load has been set to TRUE. Starting to delete table: {self.destination_table}.')
            redshift.run(f'DELETE FROM {self.destination_table}; COMMIT;')
        
        self.log.info(f'Load data into Dim table: {self.destination_table}.')
        
        sql_statement = f'''INSERT INTO                                          {self.destination_table}
                       {self.load_query};                                    COMMIT;
                    '''
        redshift.run(sql_statement)
        
        self.log.info(f'Update Dim table successfully: {self.destination_table}.')
      
