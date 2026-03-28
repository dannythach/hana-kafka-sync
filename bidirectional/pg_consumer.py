from kafka import KafkaConsumer
import json
import psycopg2
import yaml
import logging
import time
from datetime import datetime
from psycopg2.extras import execute_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# =========================
# CONFIG LOAD
# =========================
def load_config(path):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# =========================
# POSTGRES CONNECTION
# =========================
def get_pg_connection(pg_cfg):
    while True:
        try:
            conn = psycopg2.connect(**pg_cfg)
            conn.autocommit = False
            logging.info("✅ Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            logging.error(f"❌ PostgreSQL connection failed: {e}. Retrying in 5s...")
            time.sleep(5)


# =========================
# VALUE CONVERTER
# =========================
def convert_value(val):
    # Convert epoch milliseconds → datetime
    if isinstance(val, int) and val > 1000000000000:
        try:
            return datetime.fromtimestamp(val / 1000)
        except:
            return val
    return val


# =========================
# BUILD UPSERT SQL
# =========================
def build_upsert_sql(table_name, cols, pk):
    col_list = ",".join([f'"{c}"' for c in cols])
    placeholders = ",".join(["%s"] * len(cols))

    update_cols = [
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols if c.lower() != pk.lower()
    ]

    sql = f"""
        INSERT INTO {table_name} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ("{pk}")
        DO UPDATE SET {",".join(update_cols)}
    """
    return sql


# =========================
# MAIN
# =========================
def run():
    try:
        tables = load_config("config/tables.yml")["tables"]
        pg_cfg = load_config("config/postgres.yml")
        kafka_cfg = load_config("config/kafka.yml")
    except Exception as e:
        logging.error(f"❌ Failed to load config: {e}")
        return

    # Map topic → table config
    topic_map = {
        t["topic"]: t
        for t in tables if t["direction"] == "hana_to_pg"
    }

    if not topic_map:
        logging.warning("⚠️ No hana_to_pg topics found. Exiting.")
        return

    consumer = KafkaConsumer(
        *topic_map.keys(),
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="pg-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,   # 🔥 IMPORTANT
        max_poll_records=500
    )

    logging.info(f"📡 Subscribed to topics: {list(topic_map.keys())}")

    conn = get_pg_connection(pg_cfg)

    batch_size = 1
    buffer = {}

    try:
        for msg in consumer:
            table_cfg = topic_map.get(msg.topic)
            if not table_cfg:
                continue

            # 🔥 FIX: lấy payload
            data = msg.value.get("payload") or msg.value

            table_name = f'{table_cfg.get("schema", "public")}.{table_cfg["name"]}'
            pk = table_cfg["pk"]

            # Init buffer per table
            if table_name not in buffer:
                buffer[table_name] = []

            # Convert values
            row = {k: convert_value(v) for k, v in data.items()}
            buffer[table_name].append(row)

            # Flush batch
            if len(buffer[table_name]) >= batch_size:
                flush_table(conn, consumer, table_name, buffer, table_cfg)

    except Exception as e:
        logging.error(f"❌ Fatal error: {e}", exc_info=True)

    finally:
        try:
            # Flush remaining
            for table_name in buffer:
                if buffer[table_name]:
                    flush_table(conn, consumer, table_name, buffer, topic_map[msg.topic])
        except:
            pass


# =========================
# FLUSH FUNCTION
# =========================
def flush_table(conn, consumer, table_name, buffer, table_cfg):
    rows = buffer[table_name]
    if not rows:
        return

    try:
        with conn.cursor() as cursor:
            cols = list(rows[0].keys())
            pk = table_cfg["pk"]

            sql = build_upsert_sql(table_name, cols, pk)

            values = [
                [row.get(col) for col in cols]
                for row in rows
            ]

            execute_batch(cursor, sql, values, page_size=100)

        conn.commit()
        consumer.commit()

        logging.info(f"✅ Flushed {len(rows)} rows → {table_name}")

        buffer[table_name] = []

    except Exception as e:
        logging.error(f"❌ Error flushing table {table_name}: {e}", exc_info=True)
        conn.rollback()