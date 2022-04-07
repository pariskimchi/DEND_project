from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'




    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table = table 
        self.sql_query = sql_query

    def execute(self, context):
        # create connection to aws redshift 
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # On redshift from staging table to fact table 
        self.log.info("Run INSERT query to load data from S3 to Redshift")
        
        # insert_query will be setting on dag file with operator
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))

