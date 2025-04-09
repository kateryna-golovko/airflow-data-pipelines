from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                conn_id="",
                 sql_queries=[
                     SELECT count(*) from ....
                 ],
                 expected_results=[],
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = conn_id
        self.sql_queries = sql_queries
        self.expected_results = expected_results

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')