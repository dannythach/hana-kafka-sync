from kafka import KafkaConsumer
import json
import psycopg2
import yaml
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_pg_connection(pg_cfg):
    while True:
        try:
            conn = psycopg2.connect(**pg_cfg)
            logging.info("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            logging.error(f"PostgreSQL connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

def load_config(path):
    """Loads a YAML configuration file."""
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def run():
    try:
        tables = load_config("config/tables.yml")["tables"]
        pg_cfg = load_config("config/postgres.yml")
        kafka_cfg = load_config("config/kafka.yml")
    except (FileNotFoundError, KeyError) as e:
        logging.error(f"Failed to load configuration: {e}")
        return

    topic_map = {}
    for t in tables:
        if t["direction"] == "hana_to_pg":
            topic_map[t["topic"]] = t

    if not topic_map:
        logging.warning("No hana_to_pg topics found in config. Exiting.")
        return

    consumer = KafkaConsumer(
        *topic_map.keys(),
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="pg-consumer",
        auto_offset_reset="earliest"
    )
    logging.info(f"Subscribed to topics: {list(topic_map.keys())}")

    conn = get_pg_connection(pg_cfg)

    for msg in consumer:
        try:
            with conn.cursor() as cursor:
                table = topic_map.get(msg.topic)
                if not table:
                    logging.warning(f"No table config for topic {msg.topic}")
                    continue

                data = msg.value.get("payload", {})
                
                # Prevent SQL injection by quoting column names
                if not data:
                    logging.warning(f"Empty payload for topic {msg.topic}")
                    continue

                cols = [f'"{c}"' for c in data.keys()]
                vals = list(data.values())
                
                # This is a simple INSERT. For production, you might want an UPSERT (ON CONFLICT)
                sql = f"""
                INSERT INTO {table['name']}
                ({",".join(cols)})
                VALUES ({",".join(["%s"]*len(vals))})
                """

                cursor.execute(sql, vals)
                conn.commit()
                logging.info(f"Processed message for topic {msg.topic}")
        except Exception as e:
            logging.error(f"Error processing message from topic {msg.topic}: {e}")
            logging.error(f"Payload: {msg.value}")
            if conn.closed:
                conn = get_pg_connection(pg_cfg)
            else:
                conn.rollback()

if __name__ == "__main__":
    run()