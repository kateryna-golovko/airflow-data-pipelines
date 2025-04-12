from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_queries=None,
                 expected_results=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_queries = sql_queries or []
        self.expected_results = expected_results or []

        if len(self.sql_queries) != len(self.expected_results):
            raise ValueError("The number of SQL queries must match the number of expected results.")

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for idx, query in enumerate(self.sql_queries):
            self.log.info(f"Running data quality check #{idx + 1}")
            self.log.info(f"Executing query: {query}")

            result = redshift.get_records(query)

            if not result or not result[0]:
                raise ValueError(f"Data quality check failed. Query returned no results: {query}")

            actual = result[0][0]
            expected = self.expected_results[idx]

            if actual != expected:
                raise ValueError(f"Data quality check failed. Query: {query} | "
                                 f"Expected: {expected}, Got: {actual}")
            self.log.info(f"Data quality check passed. Query: {query} | Result: {actual}")