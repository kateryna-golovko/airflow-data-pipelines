from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from udacity.common import final_project_sql_statements

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)  # This tells Airflow to render 's3_key' dynamically with Jinja templating

    # SQL template for the COPY statement
    copy_sql = """
        COPY {table}
        FROM 's3://{s3_bucket}/{s3_key}'
        CREDENTIALS 'aws_iam_role={iam_role}'
        FORMAT AS JSON '{json_path}'
        REGION '{region}'
        ACCEPTINVCHARS AS '?'
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 iam_role="",
                 region="us-west-2",  # Default AWS region
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        # Assign the parameters to instance variables
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.iam_role = iam_role
        self.region = region

    def execute(self, context):
        # Get AWS credentials (in case you need them elsewhere)
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_hook.get_credentials()  # Not used directly, but retained for possible token use

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Drop and recreate staging table
        if self.table == "staging_events":
            create_sql = final_project_sql_statements.SqlQueries.staging_events_table_create
        elif self.table == "staging_songs":
            create_sql = final_project_sql_statements.SqlQueries.staging_songs_table_create
        else:
            raise ValueError(f"Unknown staging table: {self.table}")

        self.log.info(f"Dropping and recreating staging table: {self.table}")
        redshift.run(f"DROP TABLE IF EXISTS {self.table}")
        redshift.run(create_sql)

        self.log.info(f"Clearing data from destination Redshift table: {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Render the S3 key with Airflow templating (already marked as a template field)
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info(f"Rendered S3 path: {s3_path}")

        # Determine JSON path format
        # Only override if json_path is not provided
        if self.json_path:
            json_paths = f"s3://{self.s3_bucket}/{self.json_path}"
        else:
            json_paths = "auto"

        self.log.info(f"Using JSON format: {json_paths}")

        # Format COPY command with placeholders
        formatted_sql = self.copy_sql.format(
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=rendered_key,
            iam_role=self.iam_role,
            json_path=json_paths,
            region=self.region
        )

        self.log.info("Executing COPY command on Redshift...")
        self.log.debug(f"COPY command: {formatted_sql}")

        try:
            redshift.run(formatted_sql)
            self.log.info("COPY command completed successfully.")
        except Exception as e:
            self.log.error(f"Error executing COPY command: {e}")
            raise














