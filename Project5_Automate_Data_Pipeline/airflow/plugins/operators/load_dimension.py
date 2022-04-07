from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 mode="append-only",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table 
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        # create a connection to aws 
        self.log.info("Connect to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # mode setting 
        if self.mode == "delete-load":
            self.log.info("Deleting existing data table on Redshift")
            redshift.run("TRUNCATE {}".format(self.table))

        # Load dimension table 
        self.log.info("Run INSERT query to load data from s3 to Redshift")
        # run insert query
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))
