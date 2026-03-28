import yaml
import requests
import os
import time
import sys

CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "192.168.0.200")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5434")
POSTGRES_DB = os.getenv("POSTGRES_DB", "Datalake")
POSTGRES_USER = os.getenv("POSTGRES_USER", "metabase")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secret123")

SINK_CLASS = "io.confluent.connect.jdbc.JdbcSinkConnector"
SOURCE_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector"


# ------------------------------------------------
# Load tables.yml
# ------------------------------------------------

def load_tables():

    path = "config/tables.yml"

    if not os.path.exists(path):
        raise RuntimeError("tables.yml not found")

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    return data.get("tables", [])


# ------------------------------------------------
# Wait for Kafka Connect
# ------------------------------------------------

def wait_for_connect(timeout=120):

    print("⏳ Waiting for Kafka Connect...")

    for i in range(timeout):

        try:
            r = requests.get(f"{CONNECT_URL}/connectors")

            if r.status_code == 200:
                print("✅ Kafka Connect ready")
                return

        except Exception:
            pass

        time.sleep(1)

    raise RuntimeError("Kafka Connect not available")


# ------------------------------------------------
# Connector utilities
# ------------------------------------------------

def connector_exists(name):

    r = requests.get(f"{CONNECT_URL}/connectors/{name}")

    return r.status_code == 200


def create_connector(name, config):

    payload = {
        "name": name,
        "config": config
    }

    r = requests.post(
        f"{CONNECT_URL}/connectors",
        json=payload
    )

    print("CREATE", name)
    print(r.text)


def update_connector(name, config):

    r = requests.put(
        f"{CONNECT_URL}/connectors/{name}/config",
        json=config
    )

    print("UPDATE", name)
    print(r.text)


# ------------------------------------------------
# Build Sink connector
# (Kafka → Postgres)
# ------------------------------------------------

def build_sink_connector(table):

    topic = table["topic"]
    table_name = table["name"]
    pk = table["pk"]   # BẮT BUỘC

    return {

        "connector.class": SINK_CLASS,
        "tasks.max": "1",

        "topics": topic,

        "connection.url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
        "connection.user": POSTGRES_USER,
        "connection.password": POSTGRES_PASSWORD,

        # TABLE
        "table.name.format": table_name,
        "auto.create": "false",
        "auto.evolve": "false",

        # UPSERT
        "insert.mode": "upsert",
        "pk.mode": "record_value",
        "pk.fields": pk,

        "delete.enabled": "false",

        # CONVERTERS
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",

        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",

        # PERFORMANCE
        "batch.size": "3000",
        "max.retries": "10",
        "retry.backoff.ms": "3000",

        # ERROR HANDLING
        "errors.tolerance": "all",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",

        "errors.deadletterqueue.topic.name": f"dlq_{topic}",
        "errors.deadletterqueue.topic.replication.factor": "1",
        "errors.deadletterqueue.context.headers.enable": "true"
    }

# ------------------------------------------------
# Build Source connector
# (Postgres → Kafka)
# ------------------------------------------------

def build_source_connector(table):

    postgres_table = table["postgres_table"]
    topic = table["topic"]

    incrementing = table.get("incrementing_column")
    timestamp = table.get("timestamp_column")

    config = {

        "connector.class": SOURCE_CLASS,
        "tasks.max": "1",

        "connection.url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
        "connection.user": POSTGRES_USER,
        "connection.password": POSTGRES_PASSWORD,

        "table.whitelist": postgres_table,

        "topic.prefix": "",

        "poll.interval.ms": "5000",

        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",

        "key.converter": "org.apache.kafka.connect.storage.StringConverter"
    }

    if incrementing and timestamp:

        config["mode"] = "timestamp+incrementing"
        config["incrementing.column.name"] = incrementing
        config["timestamp.column.name"] = timestamp

    elif incrementing:

        config["mode"] = "incrementing"
        config["incrementing.column.name"] = incrementing

    elif timestamp:

        config["mode"] = "timestamp"
        config["timestamp.column.name"] = timestamp

    else:

        raise RuntimeError(
            f"{postgres_table} must define incrementing_column or timestamp_column"
        )

    return config


# ------------------------------------------------
# Validate table config
# ------------------------------------------------

def validate_table(table):

    if "direction" not in table:
        raise RuntimeError(f"Table {table.get('name')} missing direction")

    if "topic" not in table:
        raise RuntimeError(f"Table {table.get('name')} missing topic")


# ------------------------------------------------
# Deploy connectors
# ------------------------------------------------

def deploy():

    wait_for_connect()

    tables = load_tables()

    if not tables:
        print("No tables configured")
        return

    for table in tables:

        validate_table(table)

        direction = table["direction"]

        if direction == "hana_to_pg":

            name = f"sink-{table['name']}"
            config = build_sink_connector(table)

        elif direction == "pg_to_hana":

            name = f"source-{table['name']}"
            config = build_source_connector(table)

        else:

            print("Unknown direction:", direction)
            continue

        if connector_exists(name):
            update_connector(name, config)
        else:
            create_connector(name, config)


# ------------------------------------------------
# Main
# ------------------------------------------------

if __name__ == "__main__":

    try:
        deploy()

    except Exception as e:

        print("❌ ERROR:", e)
        sys.exit(1)