# Export data from PostgresDB to .parquet
### Configuration
1. Enter your database connection parameters in `settings.yaml`.
2. Change the default `destination_folder` as needed. This is where the .parquet files will be saved.
3. Change the default `chunk_size` as needed. This indicates the following:
	- The number of rows to ask from the database server in one network call. 
	- The number of rows to load into your local machine memory in each iteration.
	- The number of rows in one .parquet file. 
 4. In the `export` section, add schemas to the list, and their corresponding tables to be exported.

### Getting started
1. Install the dependencies using `pip install -r requirements.txt`
2. Run the main.py using `python main.py`