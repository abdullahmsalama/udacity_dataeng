from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 quality_checks,
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks

    def execute(self, context):
        self.log.info('Starting the checks')
        
        redshift_hook = PostgresHook("redshift")

        for check_index, check in enumerate(self.quality_checks):
            result = int(redshift_hook.get_first(sql=check['sql_check'])[0])

            if result == check['expected_result']:
                self.log.info("""Check number {check_number} has passed, the sql '{sql}' has resulted 
                to {check_result}, and indeed {expected} was expected""".format(check_number = check_index + 1,
                                                                         sql = check['sql_check'],
                                                                         check_result = result,
                                                                         expected = check['expected_result']))
            else:
                raise AssertionError("""The current check (check {check_number}) has failed, the sql '{sql}' has resulted 
                to {check_result}, but {expected} was expected""".format(check_number = check_index + 1,
                                                                         sql = check['sql_check'],
                                                                         check_result = result,
                                                                         expected = check['expected_result']))

        self.log.info("All checks passed successfully!")