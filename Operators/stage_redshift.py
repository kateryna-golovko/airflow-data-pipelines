from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 sql_query,  # Add sql_query as a parameter
                 conn_id="redshift", 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.sql_query = sql_query  # Store the SQL query to be used
        self.conn_id = conn_id  # Store the Redshift connection ID

    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')

        # Create a PostgresHook to connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        # Log the query for visibility
        self.log.info(f"Executing SQL query: {self.sql_query}")
        
        # Execute the query using Redshift hook
        redshift.run(self.sql_query)





