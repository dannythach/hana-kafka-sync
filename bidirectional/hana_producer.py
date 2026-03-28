from kafka import KafkaProducer
import json
from sync.hana import get_hana_connection
from sync.state import load_state, save_state
import yaml

producer.send(
    topic,
    key=record[table["pk"]],   # PK ở key
    value=record
)

def load_tables():
    with open("config/tables.yml") as f:
        return yaml.safe_load(f)["tables"]

def run():

    tables = load_tables()

    conn = get_hana_connection()
    cursor = conn.cursor()

    for table in tables:

        if table["direction"] != "hana_to_pg":
            continue

        topic = table["topic"]
        sql = table["sql"]
        state_file = table["state_file"]

        last_value = load_state(state_file)

        cursor.execute(sql, [last_value])

        columns = [c[0].lower() for c in cursor.description]

        for row in cursor.fetchall():

            record = dict(zip(columns, row))

            producer.send(topic, {
                "payload": record
            })

            if "updatedate" in record:
                save_state(state_file, record["updatedate"])

    producer.flush()