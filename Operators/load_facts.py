from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # The SQL query for data transformation (to be executed on the Redshift cluster)  
                 redshift_conn_id="",  # Redshift connection ID
                 target_table="",     # Target table to load data into (fact table)
                 append_only=False,    # Flag to determine append-only behavior for fact tables
                 sql_query="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        # Store parameters
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.append_only = append_only
        self.sql_query = sql_query

    def execute(self, context):
        # Create a PostgresHook to connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # If not append-only, clear the table first (delete all data)
        if not self.append_only:
            self.log.info(f"Deleting data from {self.target_table} fact table...")
            redshift.run(f"DELETE FROM {self.target_table}")
        
        if self.target_table == "songplay":
            create_sql = final_project_sql_statements.SqlQueries.songplay_table_create
        else:
            raise ValueError(f"Unknown fact table: {self.target_table}")

        self.log.info(f"Ensuring fact table {self.target_table} exists")
        redshift.run(create_sql)

        # Log and run the INSERT statement for the fact table
        self.log.info(f"Inserting data into {self.target_table} fact table...")
        insert_statement = f"INSERT INTO {self.target_table} \n{self.sql_query}"  # Combine table name with SQL
        self.log.info(f"Running SQL: \n{insert_statement}")
        redshift.run(insert_statement)  # Execute the query to load data
        
        # Success log
        self.log.info(f"Successfully completed insert into {self.target_table} fact table")
