import ibis
import pandas as pd
import uuid
from contextlib import contextmanager
from dagster import IOManager, Field, io_manager, InitResourceContext

class IbisDuckDBIOManager(IOManager):
    def __init__(self, connection: ibis.BaseBackend):
        self.con = connection

    def handle_output(self, context, obj):
        if obj is None:
            return

        table_name = self._get_table_name(context.asset_key)

        if isinstance(obj, pd.DataFrame):
            temp_table = f"temp_{uuid.uuid4().hex}"
            self.con.register(temp_table, obj)
            self.con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {temp_table}")
            self.con.unregister(temp_table)
        elif isinstance(obj, ibis.expr.types.TableExpr):
            sql = obj.compile()
            self.con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {sql}")
        else:
            raise NotImplementedError(
                f"Unsupported type {type(obj)}. Supported types are pandas.DataFrame and ibis.TableExpr."
            )

    def load_input(self, context):
        table_name = self._get_table_name(context.upstream_output.asset_key)
        return self.con.table(table_name)

    def _get_table_name(self, asset_key) -> str:
        return ".".join(asset_key.path)

@io_manager(
    config_schema={
        "database_path": Field(
            str,
            default_value="database.db",
            description="Path to the DuckDB database file.",
        )
    }
)
def ibis_duckdb_io_manager(init_context: InitResourceContext) -> IbisDuckDBIOManager:
    database_path = init_context.resource_config["database_path"]
    connection = ibis.duckdb.connect(database_path)
    try:
        yield IbisDuckDBIOManager(connection)
    finally:
        connection.close()
