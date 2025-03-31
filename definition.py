from dagster import asset, Definitions

@asset(io_manager_def=ibis_duckdb_io_manager)
def raw_data():
    # Return a pandas DataFrame
    return pd.DataFrame({"id": [1, 2], "value": [10, 20]})

@asset(io_manager_def=ibis_duckdb_io_manager)
def processed_data(raw_data):
    # raw_data is an Ibis table; perform transformations
    return raw_data.mutate(new_value=raw_data.value * 2)

defs = Definitions(
    assets=[raw_data, processed_data],
    resources={
        "io_manager": ibis_duckdb_io_manager.configured({"database_path": "my_data.db"})
    },
)
