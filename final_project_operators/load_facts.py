from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements

class LoadFactOperator(BaseOperator):
    """
        Purpose of the Operator:
            - Load data into a fact table in Redshift
        Inputs: 
            - redshift_conn_id: Airflow connection to Redshift
            - target_table: Name of the fact table to load data into
            - append_only: Boolean flag to determine whether existing data should be deleted before loading
            - sql_query: SQL query used to retrieve data to insert into the fact table
        Outputs: 
            - Data inserted into the specified fact table in Redshift
        execute() function does:
            - Checks if the fact table exists
            - Deletes existing data if append_only set up to False (optional)
            - Drops the table if it already exists by using sql queries from final_project_sql_statements.py file
            - Executes the INSERT INTO query to load data into the fact table
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",    
                 append_only=False,   
                 sql_query="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.append_only = append_only
        self.sql_query = sql_query

    def execute(self, context):
        """
            Purpose of the function:
                - Executes the process of loading data into a Redshift fact table

            Input:
                - context: Airflow context dictionary

            Output:
                - The fact table in Redshift is populated with new data (inserted)

            Functionality:
                - Connects to Redshift
                - Checks if the target fact table exists in Redshift
                - Deletes all existing rows if append_only is set up to False
                - Creates a table using sql statements from final_project_sql_statements
                - Executes an SQL query to insert data into the fact table
        """

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Check whether the table exists in the provided Redshift location
        check_table_exists_sql = f"""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '{self.target_table}';
        """
        table_exists = redshift.get_records(check_table_exists_sql)

        # Create the table if it does not exist
        if not table_exists:
            self.log.info(f"Table '{self.target_table}' does not exist.")
            if self.target_table == "songplay":
                create_sql = final_project_sql_statements.SqlQueries.songplay_table_create
                redshift.run(create_sql)
                self.log.info(f"Table '{self.target_table}' created successfully.")
            else:
                raise ValueError(f"Unknown fact table: {self.target_table}")
        else:
            self.log.info(f"Table '{self.target_table}' already exists.")

        # Delete data if append-only=False
        if not self.append_only:
            self.log.info(f"Append mode is set to False. Deleting data from '{self.target_table}'.")
            redshift.run(f"DELETE FROM {self.target_table}")
        else:
            self.log.info(f"Append mode is set to True. No records deletion for '{self.target_table}'.")

        # Insert new data
        self.log.info(f"Inserting data into '{self.target_table}' fact table.")
        insert_statement = f"INSERT INTO {self.target_table} \n{self.sql_query}"
        self.log.info(f"Running SQL:\n{insert_statement}")
        redshift.run(insert_statement)

        self.log.info(f"Successfully completed loading data into '{self.target_table}' fact table.")
