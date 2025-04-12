from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Redshift connection ID
                 target_table="",      # Target table to load data into (fact table)
                 append_only=False,    # Flag to determine append-only behavior for fact tables
                 sql_query="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Store parameters
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.append_only = append_only
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Redshift-compatible way to check if the table exists
        check_table_exists_sql = f"""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '{self.target_table}';
        """
        table_exists = redshift.get_records(check_table_exists_sql)

        if table_exists:
            if not self.append_only:
                self.log.info(f"Deleting data from {self.target_table} fact table...")
                redshift.run(f"DELETE FROM {self.target_table}")
        else:
            self.log.info(f"Table {self.target_table} does not exist. Skipping DELETE.")

        if self.target_table == "songplay":
            create_sql = final_project_sql_statements.SqlQueries.songplay_table_create
        else:
            raise ValueError(f"Unknown fact table: {self.target_table}")

        self.log.info(f"Ensuring fact table {self.target_table} exists...")
        redshift.run(create_sql)

        self.log.info(f"Inserting data into {self.target_table} fact table...")
        insert_statement = f"INSERT INTO {self.target_table} \n{self.sql_query}"
        self.log.info(f"Running SQL: \n{insert_statement}")
        redshift.run(insert_statement)

        self.log.info(f"Successfully completed insert into {self.target_table} fact table.")
