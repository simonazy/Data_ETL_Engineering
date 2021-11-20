# API data to Database
## Overview 
This project is to build a ETL pipeline to fetch real-time data from an open source API and store that data into a database. We have used Yelp FUSION API as the open source API available and for database we used Postgres. 

## Config File
```
[KEYS]
API_KEY=<YOUR API KEY>


[DATABASE]
host=<HOST NAME>
database=<DB NAME>
username=<USER NAME>
password=<PASSWORD>
port=<PORT>

```


## Files
```
auth.py - Contains configuration variable for making HTTP Request

businesssearch.py - Contains class to handle results returned from the search request

databasedriver.py - Contains Connection detials to Postgres database and executing queries

queries.py - Contains queries to create schema and tables in postgres and insert statement format

request.py - Contains class to handle making request to the API

driver.py - Entry point for the application, contains parsing command line arguments and control the program flow.
```

## How to Run
`python driver.py --term food --location Boston --price 4` 


## Results
