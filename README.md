
# Data Warehousing on Redshift database for sparkify startup.

   This Project is mainly focused on data warehousing, Sparkify startup wants to move their data into cloud, they chose AWS cloud services. We are analysing the data that is in S3 storage. Data is in 2 buckets, one is Song_data that is information of songs and artists, another buckets holds users logs data which has events about user activity. Both these source files are in JSON format.
   
   Our goal of this project is to extract data from S3 storage into staging tables and then load into Redshift DataWarehouse.
   
**ETL Pipeline:**  We write ETL pipelines in SQL and will call them using Python. Since source data is in S3 Bucket we need to load them into our staging cluster.
    
**SQL_Queries.Py** has SQL statements to CREATE,INSERT and DROP tables.

**Create_tables.Py** will drop and recreate the tables.

**etl.py** is used to extract data from source to staging tables and load then to Redshift.

In order to execute the ETL Pipeline, Open the terminal to the root folder.
				Then execute *`python create_tables.py`* to drop/create tables. and to execute total etl process please run *`python etl.py`* script.

    