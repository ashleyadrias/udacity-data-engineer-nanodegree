Purpose:
The purpose of this project is to create an ETL pipeline(etl.py) for a data lake hosted on Udacity's S3 bucket. We are to load the data from S3, process the data into their respective fact and dimension tables using Spark, and then load the parquet files back into S3.

Steps:
1. I created an etl jupyter notebook to build a prototype pipeline before adding the code to etl.py. This definitely helped debug and learn from my mistakes.
2. The star schema created in the the previous projects were used as a guideline
3. A new user was created on AWS IAM with S3 full access. These credentials were then added to dl.cfg
4. In order to convert the 'ts' column in the song_table to datetime, 2 User-Defined Functions were created:
>get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
>get_datetime = udf(lambda x: F.to_date(x), TimestampType())
5. To create the songs, time, and songplays table, I decided to use pyspark sql, so this required creating temporary tables:
>log_df.createOrReplaceTempView("log_df_table")
>song_df.createOrReplaceTempView("song_df_table")

How to run:
1. Replace AWS IAM Credentials in dl.cfg
2. If needed, modify input and output data paths in etl.py main function
3. In the terminal, run python etl.py