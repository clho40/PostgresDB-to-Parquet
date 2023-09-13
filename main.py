import pandas as pd
import dask.dataframe as da
import psycopg2
import yaml


def export_table_to_parquet(schema, table, cursor, destination_folder, pg_chunk_size):
    cursor.itersize = pg_chunk_size
    print(f"Exporting {schema}.{table}...")
    sql_query = f"SELECT * FROM {schema}.{table}"
    cursor.execute(sql_query)

    c = 0
    while True:
        data = cursor.fetchmany(pg_chunk_size)
        column_names = [desc[0] for desc in cursor.description]
        if len(data) > 0:
            df = pd.DataFrame(data, columns=column_names)
            ddf = da.from_pandas(df, chunksize=pg_chunk_size)
            name_function = lambda x: f"{schema}_{table}_{c}.parquet"
            ddf.to_parquet(destination_folder, name_function=name_function)
            c += 1
        else:
            break

    print(f"{schema}.{table} exported into {c} part(s)")


def main():
    try:
        # Load settings from YAML file
        with open("settings.yaml") as file:
            settings = yaml.safe_load(file)

        db_params = settings["database"]
        export = settings["export"]
        pg_chunk_size = settings["chunk_size"]
        destination_folder = settings["destination_folder"]

        # Establish a connection to the database
        with psycopg2.connect(**db_params) as conn:
            for data in export:
                schema = data["schema"]
                for table in data["tables"]:
                    with conn.cursor(name="server_side_cursor") as cursor:
                        export_table_to_parquet(
                            schema, table, cursor, destination_folder, pg_chunk_size
                        )
                        cursor.close()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        print("Done")


if __name__ == "__main__":
    main()
