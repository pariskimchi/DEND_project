# Project: Data Modeling with Apache Cassandra 

This project is the data modeling with Apache Cassandra to create 
a database which can create queries on Sparkify user activity data
for analysis. 

1. Build up ETL pipeline to iterate/process events raw dataset and 
generate new dataset 
2. Creating appropriate Apache Cassandra tables to answer 3 specific questions
3. Inserting data from new dataset to Apache Cassandra tables
4. Testing the results by select statements



## Project Structure

```
Data Modeling with Cassandra
|____event_data                # Raw dataset
| |____...events.csv
|
|____event_datafile_new.csv    # new dataset by iterating event_data
|
|____Project_1B.ipynb          # notebook for Apache Cassandra queries
|
|____images                    # Referenced image for new dataset
| |____image_event_datafile_new

```
