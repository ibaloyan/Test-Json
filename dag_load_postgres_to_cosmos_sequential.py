"""
Airflow DAG: load multiple Postgres tables into Azure Cosmos DB (SQL API) sequentially
- Streams rows from Postgres using a named cursor (no OOM).
- Upserts documents into Cosmos DB using azure-cosmos SDK with bulk enabled.
- Tasks run one-by-one (chain(*tasks)).
Configure table mapping and Airflow/environment secrets before use.
"""
from datetime import datetime, timedelta
import logging
from typing import Iterable, Dict, Any

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain
from azure.cosmos import CosmosClient, exceptions

# === CONFIG: update these for your environment ===
POSTGRES_CONN_ID = "postgres_default"      # Airflow Postgres connection id
COSMOS_DATABASE = "my_cosmos_db"
COSMOS_CONTAINER = "my_container"          # you may use one container per source table or a shared container
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
    Otherwise raise — for production, integrate KeyVault or Managed Identity.
    """
    import os
    endpoint = os.environ.get("AZURE_COSMOS_ENDPOINT")
    key = os.environ.get("AZURE_COSMOS_KEY")
    if not endpoint or not key:
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
    cur_name = f"csr_{table}"
    cur = conn.cursor(name=cur_name)
    cur.itersize = fetch_size
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
    Convert datetime objects to ISO strings.
    """
    doc = dict(row)
    pk_val = doc.get(pk_column)
    if pk_val is None:
        raise ValueError(f"Primary key column '{pk_column}' is NULL for row: {doc}")
    doc["id"] = str(pk_val)
    for k, v in list(doc.items()):
        import datetime
        if isinstance(v, (datetime.datetime, datetime.date)):
            doc[k] = v.isoformat()
    return doc

def upsert_batch_to_cosmos(container, docs):
    """
    Upsert a batch of docs into the supplied Cosmos container.
    Uses container.upsert_item in a loop; with enable_bulk=True SDK will batch requests.
    """
    for d in docs:
        try:
            container.upsert_item(d)
        except exceptions.CosmosHttpResponseError as e:
            log.exception("Failed to upsert doc id=%s: %s", d.get("id"), e)
            raise

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
    pk_prop = partition_key.lstrip("/")  # partition key property name expected in doc
    for row in stream_rows_from_postgres(table, pg_conn_id, fetch_size=batch_size):
        doc = prepare_cosmos_doc(row, pk)
        if pk_prop not in doc:
            # fallback: assign partition key from id (adjust as appropriate)
            doc[pk_prop] = doc["id"]
        batch.append(doc)
        if len(batch) >= batch_size:
            upsert_batch_to_cosmos(container, batch)
            count += len(batch)
            log.info("Upserted %d documents so far for table %s", count, table)
            batch = []

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
    dag_id="postgres_to_cosmos_bulk_load_sequential",
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
            retries=3,
            retry_delay=timedelta(minutes=5),
        )
        tasks.append(task)

    # Ensure tasks run sequentially in the listed order
    chain(*tasks)