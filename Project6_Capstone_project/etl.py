
import configparser
import os 
import logging 

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import pyspark 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf,isnan, col, upper
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import to_date,year,month
from pyspark.sql.types import DateType


# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"





# AWS configuration 
config = configparser.ConfigParser()
config.read('capstone.cfg',encoding='utf-8-sig')


os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

AWS_ACCESS_KEY_ID     = config.get('AWS', 'AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')



# data processing functions
def create_spark_session():
    """
        Create spark session object
    
    """
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
    os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2") \
        .config("fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .getOrCreate()


    return spark



# functions to utiliy 

# rename columns function 
def rename_columns(table, new_columns):
    """
        rename column name on spark dataframe
        
        input:
            - table: df to rename
            - new_colmns: new name of columns list
        output:
            - renamed df
    """
    
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table

# convert to datetime Type
def SAS_to_date(date):
    """
        Convert i94 immigration dataset's date value to datetime
    
    """
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')

def convert_to_target_type(df, cols,target_type=""):
    """
    Convert the column to datatype you want 
    Args:
        df : Spark dataframe to be processed.
        cols : List of column names that should be converted to datatype
        tartget_type : target type to convert to 
    Returns:
        df : Processed Spark dataframe
    """

    for col in [col for col in cols if col in df.columns]:
        df = df.withColumn(col, df[col].cast(target_type))
    return df

def code_mapper(f_content, idx):
    """
        function to load SAS data 
    """
    
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'","") for i in f_content2]
    
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    
    return dic   

# get_visa_type2
def get_visa_group(visa):
    """
        change i94visa to visa group
    """
    visatype2 = ''
    if visa == 1.0:
        visatype2= 'Business'
    elif visa == 2.0:
        visatype2 = 'Pleasure'
    elif visa == 3.0:
        visatype2 = "Student"
    return visatype2


# processing table function
def process_immigration_data(spark, input_data, output_data):
    """
        Process immigration dataset to get fact_immigration and 
        dim_immi_airline tables 

        Args:
            spark {object}: SparkSession object 
            input_data {object}: Source S3 endpoint 
            output_data {object}: Target S3 endpoint 

        Returns:
            None
    """

    logging.info("Start processing immigrationd dataset")
    
    # Read immigration data file 
    immi_path = os.path.join(input_data+"sas_data")
    logging.info(immi_path)
    
    immigration_df = spark.read.load(immi_path)

    logging.info("Start processing fact_immigration table")

    # exgtract columns to create fact_immigration table 
    target_cols = ['cicid','i94yr','i94mon','i94res','i94port','arrdate','depdate','i94addr','gender','visatype','fltno']

    # create fact_immigration table 
    fact_immigration_df = immigration_df.select(target_cols).distinct()\
        .withColumn('immigration_id', monotonically_increasing_id())
        
    # Data Wrangling to match data model
    new_cols = ['cic_id','year','month','country_code','city_code','arrive_date','depart_date','state_code','gender','visa_type','flight_num']

    fact_immigration_df = rename_columns(fact_immigration_df,new_cols)



    # set udf 
    SAS_to_date_udf = udf(SAS_to_date, DateType())
    # apply convert date 
    fact_immigration_df = fact_immigration_df.withColumn('arrive_date',\
                                                        SAS_to_date_udf(col('arrive_date')))
    fact_immigration_df = fact_immigration_df.withColumn('depart_date',\
                                                        SAS_to_date_udf(col('depart_date')))

    # convert country code , year, month to integer
    fact_immigration_df = fact_immigration_df.withColumn('country_code',F.col('country_code').cast('integer'))
    fact_immigration_df = fact_immigration_df.withColumn('year',F.col('year').cast('integer'))
    fact_immigration_df = fact_immigration_df.withColumn('month',F.col('month').cast('integer'))

    # write fact_immigration table to parquet files 
    fact_immigration_df.write.parquet(path=output_data+"fact_immigration",mode="overwrite")

    logging.info("Start processing dim_airline tables")

    # extract columns to create dim_immi_airline table
    target_cols = ['cicid','airline','admnum','fltno','visatype']

    dim_airline_df = immigration_df.select(target_cols)\
        .withColumn('immi_airline_id',monotonically_increasing_id())

    # rename columns 
    new_cols = ['cic_id','airline','admission_num','flight_num','visa_type']

    dim_immi_airline = rename_columns(dim_airline_df, new_cols)

    # write dim_immi_airline table as parquet 
    dim_immi_airline.write.parquet(path=output_data+"dim_immi_airline",mode='overwrite')

    logging.info("Start processin dim_visa table")

    # extract columns to create dim_visa table
    
    get_visa_group_udf = udf(lambda x:get_visa_group(x))

    # create visatype df from visatype column 
    df_visatype = immigration_df.select(['visatype','i94visa']).distinct()\
            .withColumn('visa_group',get_visa_group_udf('i94visa'))\
            .withColumn("visa_type_key",monotonically_increasing_id())

    # rename on visatype to visa_type 
    df_visatype = df_visatype.withColumnRenamed("visatype","visa_type")\
        .withColumnRenamed("i94visa","visa_code")\
    
    # wrtie dim_visa table as parquet
    df_visatype.write.parquet(path=output_data+"dim_visa",mode='overwrite')






def process_demography_data(spark, input_data, output_data):
    """ Process demograpy data to get dim_demog_population 
     and dim_demog_statistics table

        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint

        Returns:
            None
    """

    logging.info("Start processing dim_demog table")

    #read demograph dataset 
    demog_path = os.path.join(input_data+"us-cities-demographics.csv")
    demog_df = spark.read.format('csv').options(header=True, delimiter=';').load(demog_path)

    # rename columns
    new_cols = ['city','state','median_age','male','female','total','veteran','foreign_born','avg_house_size','state_code','race','count']

    dim_demog_df= rename_columns(demog_df,new_cols)

    # add id pk column 
    dim_demog_df = dim_demog_df.withColumn('demog_id',monotonically_increasing_id())

    # convert column type using function
    int_cols = ['male', 'female', 'total',
                'veteran', 'foreign_born', 'count']
    float_cols = ['median_age','avg_house_size']

    dim_demog_df = convert_to_target_type(dim_demog_df, int_cols, "integer")
    dim_demog_df = convert_to_target_type(dim_demog_df, float_cols, "float")

    # convert city, state columns to upper
    dim_demog_df = dim_demog_df.withColumn('city',upper(F.col('city')))\
        .withColumn('state',upper(F.col('state')))

    # write dim_demog table as parquet 
    dim_demog_df.write.parquet(path=output_data+"dim_demog",mode='overwrite')



def process_temperature_data(spark, input_data, output_data):
    """ Process temperature data to get dim_temperature table

        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint

        Returns:
            None
    """

    logging.info("Start processing dim_temperature")

    # read temperature dataset 
    temp_path = os.path.join(input_data+"GlobalLandTemperaturesByCity.csv") 
    temp_df = spark.read.option("delimiter",",").option("header","True").csv(temp_path)

    # filter by only United States 
    temp_df_usa = temp_df.filter(temp_df.Country == 'United States')
    temp_df_usa = temp_df_usa.select(['dt', 'AverageTemperature', 'AverageTemperatureUncertainty',\
                         'City', 'Country']).distinct()

    # rename columns 
    new_cols = ['dt','avg_temp','avg_temp_uncer','city','country','lat','long']
    temp_df_usa= rename_columns(temp_df_usa,new_cols)
    # convert dt to date_time
    temp_df_usa = temp_df_usa.withColumn("dt",to_date(F.col("dt"),'yyyy-MM-dd'))
    # convert it to float
    float_cols = ["avg_temp","avg_temp_uncer"]
    temp_df_usa = convert_to_target_type(temp_df_usa, float_cols, "float")

    # get year, month columns and add id column
    temp_df_usa = temp_df_usa.withColumn("year",year(F.col("dt")))\
        .withColumn("month",month(F.col("dt")))\
        .withColumn('temp_id',monotonically_increasing_id())
    # drop dt 
    dim_temp = temp_df_usa.drop('dt')

    # write dim_temp table as parquet 
    dim_temp.write.parquet(path=output_data+"dim_temp",mode='overwrite')


def main():
    
    spark = create_spark_session()
    
    input_data = SOURCE_S3_BUCKET
    output_data = DEST_S3_BUCKET
    
    process_immigration_data(spark, input_data, output_data)
    process_demography_data(spark, input_data, output_data)
    process_temperature_data(spark, input_data, output_data)
    logging.info("Data processing completed")


if __name__ == "__main__":
    main()