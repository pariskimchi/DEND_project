# Data Engineering Capstone Project 


## Project Summary 


The objective of this project was to create ETL pipeline for I94 immigration data, Global land temperature data and US demography dataset to form an analytics database on imigration events as Data Warehouse. 


The project follows the follow steps:

* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


## Project Structure 


- `/sample_data`: sample dataset
- `capstone.cfg`: AWS configuration file 
- `etl.py` : ETL pipeline builder file from s3 bucket to s3 bucket
- `Capstone project template.ipynb`: jupyter notebook that was used for building the ETL pipeline.

## Prerequisites 
- AWS S3 
- Apache Spark
- Python 3 for data processingL
    - Pandas - explaratory data analysis on small dataset 
    - PySpark - data processing on large dataset



### Step 1: Scope the Project and Gather Data

#### Scope
This project will integrate I94 immigration data, world temperature data and US demographic data to setup a data warehouse with fact and dimension tables.

* Data Sets 
    1. [I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html)
    2. [World Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)
    3. [U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

#### Describe and Gather Data 

| Data Set | Format | Description |
| ---      | ---    | ---         |
|[I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html)| SAS | Data contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).|
|[World Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)| CSV | This dataset is from Kaggle and contains monthly average temperature data at different country in the world wide.|
|[U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)| CSV | This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.|
---

## Step 2: Explore and Assess the Data

#### Explore the data 
1. Use pandas and pyspark for exploratory data analysis to understand on datasets
2. split datasets to Fact-dimension tables and checking missing values

Please refer to [Capstone_Project_Template.ipynb](https://github.com/pariskimchi/DEND_project/blob/main/Project6_Capstone_project/Capstone_Project_Template.ipynb).


### Step 3: Define the Data Model

#### Conceptual Data Model
Since the purpose of this data warehouse is for OLAP and BI app usage, we will model these data sets with star schema data modeling.

* Star Schema

    ![alt text](https://github.com/pariskimchi/DEND_project/blob/main/Project6_Capstone_project/capstone_star_schema.png
)

#### Data Pipeline Build Up steps
1. Load datasets 
2. clean the i94 immigration dataset to create spark dataframe for each month
    and create Fact-immigration table.
3. create airline dataframe from i94 immigration dataset and clean, then create
    airline dimensional table, save as parquet.
4. clean us-demograph dataset and create demograhpy dimensional table, save as parquet.
5. clean global temperature dataset and create temperature dimensional table, save as parquet



### Step 4: Run Pipelines to Model the Data 

#### 4.1 Create the data model
Please refer to [Capstone_Project_Template.ipynb](https://github.com/pariskimchi/DEND_project/blob/main/Project6_Capstone_project/Capstone_Project_Template.ipynb).

#### 4.2 Running the ETL pipeline 

The ETL pipeline is defined in the `etl.py` script  
and this script is to create ETL pipeline to create Fact-Dimensional table 
in Amazon S3. 

`spark-submit --packages saurfang:spark-sas7bdat:2.0.0-s_2.11 --packages org.apache.hadoop:hadoop-aws:2.7.2 etl.py`


