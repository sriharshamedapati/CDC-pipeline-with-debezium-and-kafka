import json
import os
import time
import psycopg2
from kafka import KafkaConsumer

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "dbserver1.public.products")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")

DB_HOST = os.environ.get("DB_HOST", "target-db")
DB_NAME = os.environ.get("DB_NAME", "target_db")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")

# Fields that require transformation rather than direct copy.
# Maps source field name -> (target column name, transform function)
FIELD_TRANSFORMS = {
    "price_cents": ("price_dollars", lambda v: round(v / 100, 2) if v is not None else None),
    "created_at":  ("source_created_at", lambda v: v),
}

# Source fields that are NOT copied to the target at all.
EXCLUDED_SOURCE_FIELDS = {"price_cents", "created_at"}


def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
            conn.autocommit = False
            print("Connected to target DB")
            return conn
        except Exception as e:
            print("DB connection failed, retrying in 5s...", e)
            time.sleep(5)


def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="cdc-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
            )
            print("Connected to Kafka, subscribed to:", KAFKA_TOPIC)
            return consumer
        except Exception as e:
            print("Kafka connection failed, retrying in 5s...", e)
            time.sleep(5)


def build_upsert(table: str, row: dict) -> tuple:
    """
    Dynamically build an UPSERT for any set of columns.
    Returns (sql, values_tuple).
    """
    columns = list(row.keys())
    placeholders = ["%s"] * len(columns)
    update_set = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in columns if col != "id"
    )
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({', '.join(placeholders)}) "
        f"ON CONFLICT (id) DO UPDATE SET {update_set};"
    )
    return sql, tuple(row[c] for c in columns)


def map_row(after: dict) -> dict:
    """
    Transform a Debezium 'after' payload into a target row dict.
    Handles known transformations and passes through unknown new columns
    so that schema evolution works without crashing.
    """
    row = {}
    for src_field, value in after.items():
        if src_field in FIELD_TRANSFORMS:
            target_col, transform = FIELD_TRANSFORMS[src_field]
            row[target_col] = transform(value)
        elif src_field not in EXCLUDED_SOURCE_FIELDS:
            # Pass-through: new columns added later are forwarded automatically.
            row[src_field] = value
    return row


def ensure_columns(cursor, table: str, row: dict):
    """
    Add any columns present in the row that don't yet exist in the target table.
    This makes the consumer resilient to schema evolution without crashing.
    """
    cursor.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public';
        """,
        (table,),
    )
    existing = {r[0] for r in cursor.fetchall()}
    for col in row:
        if col not in existing:
            print(f"Schema evolution: adding column '{col}' to {table}")
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} VARCHAR(255);")


def process_message(message, cursor):
    # Tombstone: null value — skip silently.
    if message.value is None:
        print("Tombstone message received, skipping.")
        return

    payload = message.value.get("payload")
    if payload is None:
        return

    op = payload.get("op")
    after = payload.get("after")
    before = payload.get("before")

    if op in ("c", "u") and after:
        row = map_row(after)
        ensure_columns(cursor, "products_replicated", row)
        sql, values = build_upsert("products_replicated", row)
        cursor.execute(sql, values)
        print(f"Upserted id={row.get('id')}")

    elif op == "d" and before:
        cursor.execute(
            "DELETE FROM products_replicated WHERE id = %s;",
            (before["id"],),
        )
        print(f"Deleted id={before['id']}")

    else:
        print(f"Unhandled op='{op}', skipping.")


def main():
    consumer = create_consumer()
    conn = connect_db()
    cursor = conn.cursor()

    print("Consumer started, waiting for messages...")

    for message in consumer:
        try:
            process_message(message, cursor)
            conn.commit()
        except psycopg2.OperationalError as e:
            print("DB connection lost, reconnecting...", e)
            conn = connect_db()
            cursor = conn.cursor()
        except Exception as e:
            print("Error processing message:", e)
            conn.rollback()


if __name__ == "__main__":
    main()
