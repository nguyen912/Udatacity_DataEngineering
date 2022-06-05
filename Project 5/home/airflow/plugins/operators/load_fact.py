from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 destination_table = '',
                 load_query = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.load_query = load_query
        self.destination_table = destination_table

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        self.log.info(f'Load data into Fact table: {self.destination_table}')        
        sql_statement = f''' INSERT INTO                                         {self.destination_table}
                      {self.load_query}; 
                      COMMIT;
                    '''
        redshift.run(sql_statement)
        self.log.info('Fact table is loaded completely.')
