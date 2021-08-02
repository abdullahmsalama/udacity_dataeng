## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## File structure

dl.cfg: a file containing the necessary AWS access and secretkey needed to access S3 (load and save).

etl.py: as the name suggests, responsible for extracting the data from S3, processing and transforing the data using Spark and loading them to another bucket in S3.

- data

	This folder contains the data represented in song-data.zip and log-data.zip. The data on this repo is empty as the original data is huge!


### Run steps

just run "python etl.py" from the command line, everything is explained and commented in the code.