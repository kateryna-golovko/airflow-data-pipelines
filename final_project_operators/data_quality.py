from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
        Purpose of the Operator:
            - Run data quality checks on a Redshift database to validate the data loads
        Inputs: 
            - redshift_conn_id: Airflow connection to Redshift
            - sql_queries: List of SQL queries for data quality checks
            - expected_results: List of expected results that each SQL query should return
        Outputs: 
            - Logs success if all checks pass
            - Raises an error if any check fails/number of queries and expected result not match 
        execute() function does:
            - Runs each query against Redshift using PostgresHook
            - Compares the result of each query with its expected result
            - Raises a ValueError if a mismatch/empty result is found
    """

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
        """
            Purpose of the function:
                - Execute all provided data quality checks for the tables
            Input:
                - context: Airflow execution context (not in use)
            Output:
                - Logs results of checks
                - Raises errors for failed checks
            Functionality:
                - Connects to Redshift
                - Iterates through each query in the list
                - Runs the query and retrieves the result
                - Compares the actual result to the expected result (provided in final_project.py)
                - Logs pass/fail status
        """

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for i, query in enumerate(self.sql_queries):
            self.log.info(f"Executing query number {i+1}: {query}")

            result = redshift.get_records(query)

            if not result or not result[0]:
                raise ValueError(f"Query returned no results: {query}")

            actual_result = result[0][0]
            expected_result = self.expected_results[i]

            if actual_result != expected_result:
                raise ValueError(f"Data quality check has failed. Query: {query}"
                                 f"Expected result: {expected_result} vs Actual result: {actual_result}")

            self.log.info(f"Data quality check has succeeded. Query: {query} vs Result: {actual_result}")