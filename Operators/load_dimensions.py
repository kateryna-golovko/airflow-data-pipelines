from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 sql_query="",  # SQL query for transforming data (e.g., loading the dimension table)
                 redshift_conn_id="",  # Redshift connection ID
                 target_table="",     # Target table to load data into (dimension table)
                 truncate=True,       # Flag for truncating the table before inserting data
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        # Store parameters
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.truncate = truncate

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')

        self.log.info(f"Loading data into {self.target_table}...")

        # Create a PostgresHook to connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

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

        self.log.info(f"Ensuring dimension table {self.target_table} exists")
        redshift.run(create_sql)


        # If truncate is enabled, first truncate the table
        if self.truncate:
            self.log.info(f"Truncating dimension table {self.target_table}")
            # Run a TRUNCATE query to empty the table
            truncate_sql = f"TRUNCATE TABLE {self.target_table};"
            redshift.run(truncate_sql)

        # After truncation (or if truncate is disabled), load the new data
        self.log.info(f"Inserting new data into {self.target_table}")
        # The SQL query to load data into the dimension table
        rendered_sql = self.sql_query.format(target_table=self.target_table)
        redshift.run(rendered_sql)

        self.log.info(f"Data successfully loaded into {self.target_table}")
