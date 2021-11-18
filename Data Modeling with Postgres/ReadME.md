Files used on the project:

1. **data** folder nested at the home of the project, where all needed jsons reside.
2. **sql_queries.py** contains all your sql queries, and is imported into the files bellow.
3. **create_tables.py** drops and creates tables. You run this file to reset your tables before each time you run your ETL scripts.
4. **test.ipynb** displays the first few rows of each table to let you check your database.
5. **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into your tables. 
6. **etl.py** reads and processes files from song_data and log_data and loads them into your tables. 

### Break down of steps followed

1º Wrote DROP, CREATE and INSERT query statements in sql_queries.py

2º Run in console
 ```
python create_tables.py
```

3º Run etl in console, and verify results:
 ```
python etl.py
```
