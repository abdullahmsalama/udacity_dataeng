from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table_name,
                 s3_bucket_name,
                 s3_data_path,
                 aws_conn_id = "aws_credentials",
                 redshift_conn_id = "redshift",
                 copy_option = '',
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.s3_bucket_name = s3_bucket_name
        self.s3_data_path = s3_data_path
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.copy_option = copy_option
        
    def execute(self, context):
        # Just log the beginning of staging the data
        self.log.info(f'Staging data from {self.s3_bucket_name}/{self.s3_data_path} to table {self.table_name}')
        
        aws_hook = AwsHook("aws_credentials")
        aws_credentials = aws_hook.get_credentials()
        
        # Defining the copy query (will be used with the operator)
        copy_command = """
                    COPY {table}
                    FROM 's3://{s3_bucket_name}/{s3_data_path}'
                    with aws_credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {copy_option};
                     """.format(table = self.table_name,
                           s3_bucket_name = self.s3_bucket_name,
                           s3_data_path = self.s3_data_path,
                           access_key = aws_credentials.access_key,
                           secret_key = aws_credentials.secret_key,
                           copy_option = self.copy_option
                               )        
        # Connecting to redshift
       # redshift_hook = PostgresOperator(
       #     task_id = self.task_id,
        #    postgres_conn_id = self.redshift_conn_id,
       #     sql = copy_command,
       #     )"""
    
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run(copy_command)

       
        self.log.info("""The COPY command finished sucessfully \n The command is
                      {copy_command}""".format(copy_command = copy_command))
       