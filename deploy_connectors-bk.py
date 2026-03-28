import yaml
import requests
import sys
import os
import time

CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "192.168.0.200")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5434")
POSTGRES_DB = os.getenv("POSTGRES_DB", "Datalake")
POSTGRES_USER = os.getenv("POSTGRES_USER", "metabase")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secret123")

CONNECTOR_CLASS = "io.confluent.connect.jdbc.JdbcSinkConnector"


def load_tables():
    with open("config/tables.yml", "r") as f:
        data = yaml.safe_load(f)
    return data["tables"]


def wait_for_connect(timeout=60):
    print("Waiting for Kafka Connect...")
    for _ in range(timeout):
        try:
            r = requests.get(f"{CONNECT_URL}/connectors")
            if r.status_code == 200:
                print("✅ Kafka Connect ready.")
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("Kafka Connect not available.")


def build_connector_config(table_name, topic, pk_field):
    return {
        "connector.class": CONNECTOR_CLASS,
        "tasks.max": "1",
        "topics": topic,

        # JDBC
        "connection.url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
        "connection.user": POSTGRES_USER,
        "connection.password": POSTGRES_PASSWORD,

        # Table behavior
        "auto.create": "false",
        "auto.evolve": "false",
        "insert.mode": "upsert",

        # 🔥 FIX HERE
        "pk.mode": "record_value",
        "pk.fields": pk_field,

        "table.name.format": table_name,

        # Plain JSON
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",

        "key.converter": "org.apache.kafka.connect.storage.StringConverter",

        "delete.enabled": "false",

        # Error handling
        "errors.tolerance": "all",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true"
        
    }


def connector_exists(name):
    r = requests.get(f"{CONNECT_URL}/connectors/{name}")
    return r.status_code == 200


def create_connector(name, config):
    payload = {"name": name, "config": config}
    r = requests.post(f"{CONNECT_URL}/connectors", json=payload)
    print(r.text)


def update_connector(name, config):
    r = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=config)
    print(r.text)


def deploy():
    wait_for_connect()
    tables = load_tables()

    for table in tables:
        table_name = table["name"]
        topic = table["topic"]
        pk_field = table["pk"]

        connector_name = f"pg-{table_name}-sink"
        config = build_connector_config(table_name, topic, pk_field)

        if connector_exists(connector_name):
            update_connector(connector_name, config)
        else:
            create_connector(connector_name, config)


if __name__ == "__main__":
    deploy()