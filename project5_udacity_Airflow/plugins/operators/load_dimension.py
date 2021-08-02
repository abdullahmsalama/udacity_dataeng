from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table_name,
                 insert_sql,
                 truncate_table,
                 redshift_conn_id ="redshift",
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Mapping params
        self.table_name = table_name
        self.insert_sql = insert_sql
        self.truncate_table = truncate_table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # Just log the beginning of execution
        self.log.info(f'beginning loading of the dimension operator')
        redshift_hook = PostgresHook(
            task_id = "redshift_connection " + self.redshift_conn_id,
            postgres_conn_id = self.redshift_conn_id)

        # Check whether truncation is needed first
        if self.truncate_table:
            self.log.info('Truncating table {} before inserting'.format(self.table_name))
            redshift_hook.run('TRUNCATE TABLE {}'.format(self.table_name))
            self.log.info('truncated table {}, now inserting'.format(self.table_name))

        
        # The sql command to be used in the redshift hook
        sql_command = """
            INSERT INTO {table_name} {sql_query};""".\
        format(table_name=self.table_name, sql_query=self.insert_sql)
        
        # Run the insert sql with the hook
        redshift_hook.run(sql_command)
        
        self.log.info("""connected with redshift and inserted to the table {table} with the
        following SQL command: {command}""".format(table = self.table_name, command = sql_command))
