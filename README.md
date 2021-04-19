# Data pipelines with Apache Airflow

This repo contains my solution for the Data pipelines with Airflow project of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Purpose

The project involves an imaginary music streaming startup called Sparkify. Sparkify is using song play data to analyze the streaming habits of the users, and needs an automated ETL pipeline to process the data. In this project an Apache Airflow data pipeline is setup to execute and monitor a scheduled ETL pipeline, while validating data quality and testing results. The data consists of song metadata derived from the [Million Song Dataset](http://millionsongdataset.com/) [1] as well as song play logs. The data needs to be extracted from JSON files in S3, transformed into a dimensional model, and loaded into Sparkify's Redshift data warehouse.

## DAG configuration

`dags/sparkify_etl.py` contains the configuration of the data pipeline as a directed acyclic graph (DAG). The DAG is scheduled to run hourly, from a specific starting date, retrying 3 times with 5 minutes intervals on failure.

The operators of the DAG, defined in the `plugins/operators` folder, performs the following sequence of tasks:

1. Stage the data into Redshift staging tables.
2. Load the songplays fact table.
3. Load the dimension tables.
4. Run data quality checks.

## Stage operator

The stage operator loads data into Redshift using a parameterised SQL COPY statement. This allows specifying the source and destination from the outside, and in this way reusing the same operator.

In the DAG the stage operator is used twice, to load song data and log data in parallel.

## Fact and dimension operators

The fact and dimension operators fill data into a given target table name based on a given SQL query.

The fact operator always appends data to the table, whereas the dimension operator can either run in append mode, or truncate-insert mode.

In the DAG, a single fact operator is used to load songplay data, and four dimension operators are used to load songs, artist, users and time data.

## Data quality operator

The data quality operator performs a sequence of data quality checks, configured as a list of SQL statements along with expected results. If a check does not match the expected result, the task will fail.

## How to run

Use docker-compose to run Airflow:

```
docker-compose up
```

Setup a Redshift cluster, and initialize it with database table using the `create_tables.sql` script.

## References

[1] Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere. The Million Song Dataset. In Proceedings of the 12th International Society for Music Information Retrieval Conference (ISMIR 2011), 2011.