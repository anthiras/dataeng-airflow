# Data pipelines with Apache Airflow

This repo contains my solution for the Data pipelines with Airflow project of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Purpose

The project involves an imaginary music streaming startup called Sparkify. Sparkify is using song play data to analyze the streaming habits of the users, and needs an automated ETL pipeline to process the data. In this project an Apache Airflow data pipeline is setup to execute and monitor a scheduled ETL pipeline, while validating data quality and testing results. The data consists of song metadata derived from the [Million Song Dataset](http://millionsongdataset.com/) [1] as well as song play logs. The data needs to be extracted from JSON files in S3, transformed into a dimensional model, and loaded into Sparkify's Redshift data warehouse.

## How to run

Use docker-compose to run Airflow.

First initialize the database:

```
docker-compose up airflow-init
```

Then start Airflow:

```
docker-compose up
```

## References

[1] Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere. The Million Song Dataset. In Proceedings of the 12th International Society for Music Information Retrieval Conference (ISMIR 2011), 2011.