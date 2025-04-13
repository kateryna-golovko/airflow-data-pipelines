from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from udacity.common import final_project_sql_statements

class StageToRedshiftOperator(BaseOperator):
    """
        Purpose of the Operator: 
            - Load data from S3 into a staging table inside Redshift using COPY command
        Inputs: 
            - redshift_conn_id: Airflow connection ID for Redshift
            - aws_credentials_id: Airflow connection ID for AWS credentials
            - table: Name of the staging table in Redshift
            - s3_bucket: S3 bucket name containing the source data
            - s3_key: S3 path to the data files
            - json_path: JSON path for data mapping
            - iam_role: AWS IAM role ARN for Redshift COPY command access
            - region: AWS region where S3 data is located
        Outputs: 
            - Redshift staging table, specified inside the final_project.py  
        execute() function does:
            - Connects to AWS and Redshift
            - Creates the staging table (DROP + CREATE Statements from final_project_sql_statements.py)
            - Goes through the S3 path dynamically using the execution context
            - Constructs the COPY SQL command - uses reference from Project 2, which is included in final_project_sql_statements.py
            - Executes the COPY command to load data into Redshift
    """

    ui_color = '#358140'
    template_fields = ("s3_key",) 

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
                 region="", 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.iam_role = iam_role
        self.region = region

    def execute(self, context):
        """
            Purpose of the function:
                - Executes the logic to copy data from S3 into a Redshift staging table
            Input:
                - context: Airflow context dictionary (provides runtime info - e.g. execution date)
            Output:
                - Data is loaded into the target staging table in Redshift (the info for which is mentioned in the final_project.py)
            Functionality:
                - #Retrieves AWS credentials 
                - Uses final_project_sql_statements.py script to get guidance on CREATE and DROP statements
                - Formats the S3 key using the context
                - Constructs the COPY command dynamically
                - Executes the COPY command using Redshift connection
                - Logs success or failure
        """

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Drop and re-create staging table
        if self.table == "staging_events":
            create_sql = final_project_sql_statements.SqlQueries.staging_events_table_create
        elif self.table == "staging_songs":
            create_sql = final_project_sql_statements.SqlQueries.staging_songs_table_create
        else:
            raise ValueError(f"Unknown staging table: {self.table}")

        self.log.info(f"Dropping and re-creating staging table: {self.table}")
        redshift.run(create_sql)

        # Use Airflow templating for the S3 key 
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info(f"Rendered S3 path: {s3_path}")

        if self.json_path.lower() == "auto":
            json_paths = "auto"
        elif self.json_path.startswith("s3://"):
            json_paths = self.json_path
        else:
            json_paths = f"s3://{self.s3_bucket}/{self.json_path}"

        self.log.info(f"Using JSON format: {json_paths}")

        # Format COPY command with placeholders
        copy_sql = self.copy_sql.format(
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=rendered_key,
            iam_role=self.iam_role,
            json_path=json_paths,
            region=self.region
        )

        self.log.info("Executing COPY command on Redshift: {copy_sql}")

        try:
            redshift.run(copy_sql)
            self.log.info("COPY command completed successfully.")
        except Exception as e:
            self.log.error(f"Error executing COPY command: {e}")
            raise















