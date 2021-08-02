# A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# The JSON logs are included in the data folder. The processing of the data is done through an ETL approach (extract -load-transform). The main script is etl.py, nevertheless, create_tables.py needs to be run before to drop and create the tables again. A star schema was designed for better quering, the components for the schema are 5 tables as follows:

![Alt text](schema.png)

## etl.ipynb is for simulation to make sure everything works by reading only one file to create each table, then the same steps to process all files in etl.py.

## sql_queries.py include all drop, create and insert commands that are being used in etl.py, and is where the tables and their schemas are defined.

## create_tables.py is responsible for dropping and creating all tables, clearing the way for another run.

## test.ipynb as the name suggests is only a test file for checking that the tables have been fed with data and for any other simulation purposes.

- data

	The data folder only includes one file in each end folder as the original data is big

# To run the project, from the command line insert "python create_tables.py", this will delete all tables and create the tables with the desired schema, then insert to the command line "python etl.py", this will run the etl.py which will process all files, transform them and load/insert to the tables, then you can run test.ipynb to check that all tables were run correctly. P.S: you can run any script in a notebook by adding ! in the cell, ex. (!python create_tables.py), so add what you want to run to test.ipynb and have fun!