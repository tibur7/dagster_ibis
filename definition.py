from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    asset,
    resource,
    IOManager,
    io_manager,
    ConfigurableIOManager
)
from typing import Optional, Dict, Any
import ibis
from ibis.expr.types import Table

# 1. Define custom IOManager to handle Ibis tables
class IbisIOManager(ConfigurableIOManager):
    """Handles Ibis tables by storing their SQL representations as metadata"""
    
    def handle_output(self, context, obj: Optional[Table]):
        if obj is None:  # Handle non-table outputs
            return
        
        # Store table metadata instead of the actual data
        return MaterializeResult(
            metadata={
                "sql": obj.compile(),
                "schema": str(obj.schema()),
                "columns": list(obj.columns)
            }
        )

    def load_input(self, context) -> Table:
        """Reconstruct Ibis table from context"""
        # Get connection from resources
        con = context.resources.ibis_duckdb
        
        # Rebuild table reference using asset name as table name
        return con.table(context.asset_key.path[-1])

# 2. Define resource with type hints
@resource
def ibis_duckdb_resource(_) -> ibis.backends.duckdb.Backend:
    return ibis.duckdb.connect()

# 3. Create assets using Ibis tables
@asset(required_resource_keys={"ibis_duckdb"})
def raw_data(context: AssetExecutionContext) -> Table:
    con = context.resources.ibis_duckdb
    
    # Create and return table without executing
    return con.create_table(
        "raw_data",
        schema={"id": "int", "value": "str"}
    )

@asset(required_resource_keys={"ibis_duckdb"})
def processed_data(context: AssetExecutionContext, raw_data: Table) -> Table:
    """Filter raw data using lazy execution"""
    return raw_data.filter(raw_data.value == "test")

# 4. Configure Definitions with resources and IOManager
defs = Definitions(
    assets=[raw_data, processed_data],
    resources={
        "ibis_duckdb": ibis_duckdb_resource,
        "io_manager": IbisIOManager()
    }
)

@asset(required_resource_keys={"ibis_duckdb"})
def materialized_results(context: AssetExecutionContext, processed_data: Table) -> MaterializeResult:
    """Execute and materialize the final results"""
    df = processed_data.execute()
    return MaterializeResult(
        metadata={
            "row_count": len(df),
            "columns": list(df.columns)
        },
        data=df  # Actual data storage
    )
