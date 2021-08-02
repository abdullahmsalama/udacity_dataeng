from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table_name,
                 insert_sql,
                 redshift_conn_id ="redshift",
                 *args,
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Mapping params
        self.table_name = table_name
        self.insert_sql = insert_sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # Just log the beginning of execution
        self.log.info(f'beginning loading of the fact operator')
        
        # The sql command to be used in the redshift hook
        sql_command = """
            INSERT INTO {table_name} {sql_query};""".\
        format(table_name=self.table_name, sql_query=self.insert_sql)
        
        # Connecting to redshift
        redshift_hook = PostgresOperator(
            task_id = "redshift_connection " + self.redshift_conn_id,
            postgres_conn_id = self.redshift_conn_id,
            sql_command = self.insert_sql,
            )
        self.log.info("""connected with redshift and inserted to the table {table} with the
        following SQL command: {command}""".format(table = self.table_name, command = sql_command))