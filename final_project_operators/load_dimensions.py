from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements

class LoadDimensionOperator(BaseOperator):
    """
        Purpose of the Operator:
            - Load data into a Redshift dimension table using a provided SQL query in the final_project_sql_statements.script
            - Truncate the table before loading to avoid duplicate records if truncate=True
        Inputs: 
            - sql_query: SQL query that selects data for the dimension table
            - redshift_conn_id: Airflow connection ID to Redshift
            - target_table: Name of the dimension table to load data into
            - truncate: Boolean flag to determine whether to truncate the table before loading data in
        Outputs: 
            - Populates the specified dimension table in Redshift with data
        execute function does:
            - Creates the table if it doesn't exist
            - Truncates the table if `truncate` is set to True
            - Executes the SQL insert query to load data into the dimension table
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 sql_query="",  
                 redshift_conn_id="",  
                 target_table="",    
                 truncate=True,      
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.truncate = truncate

    def execute(self, context):
        """
            Purpose of the function:
                - Orchestrates the loading of data into a dimension table in Redshift
            Input:
                - context: The Airflow execution context 
            Output:
                - Inserts specified data into the Redshift dimension table
            Functionality:
                - Connects to Redshift using PostgresHook
                - Runs a CREATE TABLE statement for a specified table name
                - If `truncate` is set to True, clears all existing records from the table
                - Runs the final INSERT query using the provided SQL logic
        """

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading data into {self.target_table}")

        # Check whether table exists
        check_table_exists_sql = f"""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '{self.target_table}';
        """
        table_exists = redshift.get_records(check_table_exists_sql)

        if self.target_table == "user_info":
            create_sql = final_project_sql_statements.SqlQueries.user_table_create
        elif self.target_table == "song":
            create_sql = final_project_sql_statements.SqlQueries.song_table_create
        elif self.target_table == "artist":
            create_sql = final_project_sql_statements.SqlQueries.artist_table_create
        elif self.target_table == "time":
            create_sql = final_project_sql_statements.SqlQueries.time_table_create
        else:
            raise ValueError(f"Unknown dimension table: {self.target_table}")

        # Create table if it doesn't exist
        if not table_exists:
            self.log.info(f"Table '{self.target_table}' does not exist.")
            redshift.run(create_sql)
            self.log.info(f"Table '{self.target_table}' created successfully.")
        else:
            self.log.info(f"Table '{self.target_table}' already exists.")

        # If truncate is enabled, first truncate the table
        if self.truncate:
            self.log.info(f"Truncating dimension table {self.target_table}")
            # Run a TRUNCATE query to empty the table
            truncate_sql = f"TRUNCATE TABLE {self.target_table};"
            redshift.run(truncate_sql)

        # Load the new data
        self.log.info(f"Inserting new data into {self.target_table}")
        
        insert_statement = f"INSERT INTO {self.target_table} \n{self.sql_query}"
        redshift.run(insert_statement)

        self.log.info(f"Data successfully loaded into {self.target_table}")

