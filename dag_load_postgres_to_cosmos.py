"""
Airflow DAG: load multiple Postgres tables into Azure Cosmos DB (SQL API)
- Streams rows from Postgres using a named cursor (server-side) to avoid large memory usage.
- Upserts documents into Cosmos DB using azure-cosmos SDK with bulk enabled.
Configure table mapping below and Airflow connections before use.
"""
from datetime import datetime, timedelta
import json
import logging
from typing import Iterable, Dict, Any

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from azure.cosmos import CosmosClient, PartitionKey, exceptions

# === CONFIG: update these for your environment ===
POSTGRES_CONN_ID = "postgres_default"      # Airflow Postgres connection id
COSMOS_CONN_ID = "azure_cosmos_conn"       # (optional) identity; code below reads from env or extras
COSMOS_DATABASE = "my_cosmos_db"
COSMOS_CONTAINER = "my_container"          # you may have one container per source table or a shared container
# List of tables to load: each entry defines source table, primary key column and partition key for Cosmos.
TABLES_TO_LOAD = [
    {"table": "table_a", "pk": "id", "partition_key": "/partition_key_a"},
    {"table": "table_b", "pk": "id", "partition_key": "/partition_key_b"},
    {"table": "table_c", "pk": "id", "partition_key": "/partition_key_c"},
]
BATCH_SIZE = 1000                            # number of rows per batch to upsert
COSMOS_CLIENT_KW = {"enable_bulk": True}     # enable bulk support
# =================================================

log = logging.getLogger(__name__)

def get_cosmos_client_from_env_or_conn():
    """
    Create CosmosClient. Prefer environment variables:
      - AZURE_COSMOS_ENDPOINT
      - AZURE_COSMOS_KEY
    Otherwise read from Airflow connection extras (not implemented here).
    """
    import os
    endpoint = os.environ.get("AZURE_COSMOS_ENDPOINT")
    key = os.environ.get("AZURE_COSMOS_KEY")
    if not endpoint or not key:
        # Optionally, read an Airflow connection for secrets. For security, prefer env/KeyVault/ManagedIdentity.
        raise RuntimeError(
            "Cosmos credentials not found. Set AZURE_COSMOS_ENDPOINT and AZURE_COSMOS_KEY in environment."
        )
    return CosmosClient(endpoint, key, **COSMOS_CLIENT_KW)

def stream_rows_from_postgres(table: str, pg_conn_id: str, fetch_size: int = 1000) -> Iterable[Dict[str, Any]]:
    """
    Stream rows from Postgres using a server-side cursor to avoid loading entire table into memory.
    Yields dict per row (column names preserved).
    """
    hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = hook.get_conn()
    # Use a named cursor for server-side iteration
    cur_name = f"csr_{table}"
    cur = conn.cursor(name=cur_name)
    cur.itersize = fetch_size
    # Adjust SELECT list if you want specific columns only
    cur.execute(f"SELECT * FROM {table};")
    cols = [desc[0] for desc in cur.description]
    while True:
        rows = cur.fetchmany(fetch_size)
        if not rows:
            break
        for row in rows:
            yield dict(zip(cols, row))
    cur.close()
    conn.close()

def prepare_cosmos_doc(row: Dict[str, Any], pk_column: str) -> Dict[str, Any]:
    """
    Convert a Postgres row to a Cosmos document.
    Ensure 'id' exists and is a string (Cosmos requirement).
    Use pk_column to generate id if needed, or combine for composite uniqueness.
    """
    doc = dict(row)  # shallow copy
    # Ensure id is present and a string
    pk_val = doc.get(pk_column)
    if pk_val is None:
        # fallback: create a synthetic id (not recommended for deterministic upsert)
        raise ValueError(f"Primary key column '{pk_column}' is NULL for row: {doc}")
    doc["id"] = str(pk_val)
    # Optionally remove keys that Cosmos should not store or convert datetimes to isoformat
    # Convert datetime objects to ISO strings
    for k, v in list(doc.items()):
        import datetime
        if isinstance(v, datetime.datetime) or isinstance(v, datetime.date):
            doc[k] = v.isoformat()
    return doc

def upsert_batch_to_cosmos(container, docs):
    """
    Upsert a batch of docs into the supplied Cosmos container.
    Uses container.upsert_item; when client created with enable_bulk=True this will be efficient.
    """
    results = []
    for d in docs:
        try:
            results.append(container.upsert_item(d))
        except exceptions.CosmosHttpResponseError as e:
            # Log and optionally collect failed docs for retry or DLQ
            log.exception("Failed to upsert doc id=%s: %s", d.get("id"), e)
            raise
    return results

def load_table_to_cosmos(table: str, pk: str, partition_key: str, pg_conn_id: str = POSTGRES_CONN_ID,
                         cosmos_database: str = COSMOS_DATABASE, cosmos_container: str = COSMOS_CONTAINER,
                         batch_size: int = BATCH_SIZE, **kwargs):
    """
    Main loader: stream from Postgres and upsert into Cosmos in batches.
    """
    log.info("Starting load for table=%s into Cosmos container=%s (pk=%s, partition_key=%s)",
             table, cosmos_container, pk, partition_key)
    client = get_cosmos_client_from_env_or_conn()
    database = client.get_database_client(cosmos_database)
    container = database.get_container_client(cosmos_container)

    batch = []
    count = 0
    for row in stream_rows_from_postgres(table, pg_conn_id, fetch_size=batch_size):
        doc = prepare_cosmos_doc(row, pk)
        # Ensure partition key property exists in doc (Cosmos requires partition key value for upsert)
        # User must map a column name or set static partition; here we assume the partition_key path is like "/col"
        pk_prop = partition_key.lstrip("/")

        if pk_prop not in doc:
            # If partition key absent, we can choose to set it to pk value or fail
            doc[pk_prop] = doc["id"]  # example fallback
        batch.append(doc)

        if len(batch) >= batch_size:
            upsert_batch_to_cosmos(container, batch)
            count += len(batch)
            log.info("Upserted %d documents so far for table %s", count, table)
            batch = []

    # upsert remainder
    if batch:
        upsert_batch_to_cosmos(container, batch)
        count += len(batch)
        log.info("Upserted final %d documents for table %s", len(batch), table)

    log.info("Completed load for table=%s: total upserted=%d", table, count)

# === DAG definition ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="postgres_to_cosmos_bulk_load",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["cosmos", "postgres", "etl"],
) as dag:

    tasks = []
    for t in TABLES_TO_LOAD:
        task = PythonOperator(
            task_id=f"load_{t['table']}_to_cosmos",
            python_callable=load_table_to_cosmos,
            op_kwargs={
                "table": t["table"],
                "pk": t["pk"],
                "partition_key": t["partition_key"],
                "pg_conn_id": POSTGRES_CONN_ID,
                "cosmos_database": COSMOS_DATABASE,
                "cosmos_container": COSMOS_CONTAINER,
                "batch_size": BATCH_SIZE,
            },
            dag=dag,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )
        tasks.append(task)

    # Example: run tables in parallel (no dependencies)
    # If you want them sequential: chain(*tasks)