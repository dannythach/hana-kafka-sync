import yaml
import os
import sys
import time
import threading

import logging
from sync.hana import HanaClient
from sync.kafka import ConnectProducer
from sync.runner import SyncRunner
from bidirectional.hana_consumer import HanaConsumer

from hdbcli import dbapi as hana_dbapi  # type: ignore

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


INTERVAL = int(os.getenv("SYNC_INTERVAL", "10"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

def load_cfg(path):
    with open(path) as f:
        return yaml.safe_load(os.path.expandvars(f.read()))


# ------------------------------------------------
# HANA CONNECTION
# ------------------------------------------------

def connect_to_hana(cfg):

    while True:
        try:
            log.info("Attempting to connect to HANA...")
            hana = HanaClient(cfg)
            log.info("✅ Connected to HANA.")
            return hana

        except hana_dbapi.Error as e:
            log.error(f"❌ HANA connection failed: {e}")
            time.sleep(INTERVAL)


# ------------------------------------------------
# KAFKA CONNECTION
# ------------------------------------------------

def connect_to_kafka(bootstrap_servers):

    while True:
        try:
            log.info("Attempting to connect to Kafka...")
            producer = ConnectProducer(bootstrap_servers)
            log.info("✅ Connected to Kafka.")
            return producer

        except Exception as e:
            log.error(f"❌ Kafka connection failed: {e}")
            time.sleep(INTERVAL)


# ------------------------------------------------
# HANA → KAFKA LOOP
# ------------------------------------------------

def hana_to_kafka_loop(runner, tables, hana_cfg):

    while True:

        for tbl in tables:

            if tbl["direction"] != "hana_to_pg":
                continue

            try:
                runner.run_table(tbl)

            except hana_dbapi.Error as e:

                log.error(f"HANA connection lost: {e}")
                runner.hana.close()

                new_hana = connect_to_hana(hana_cfg)
                runner.hana = new_hana

            except Exception as e:
                log.error(f"Error processing table {tbl['name']}", exc_info=True)

        time.sleep(INTERVAL)


# ------------------------------------------------
# KAFKA → HANA LOOP
# ------------------------------------------------

def kafka_to_hana_loop(hana, tables, kafka_cfg):

    consumer = HanaConsumer(
        kafka_cfg["bootstrap_servers"],
        hana,
        tables
    )

    consumer.run()


# ------------------------------------------------
# MAIN
# ------------------------------------------------

def main():

    tables = load_cfg("config/tables.yml")["tables"]
    hana_cfg = load_cfg("config/hana.yml")
    kafka_cfg = load_cfg("config/kafka.yml")

    hana = connect_to_hana(hana_cfg)
    producer = connect_to_kafka(kafka_cfg["bootstrap_servers"])

    runner = SyncRunner(hana, producer)

    log.info(f"🚀 Started sync engine with {len(tables)} tables")

    t1 = threading.Thread(
        target=hana_to_kafka_loop,
        args=(runner, tables, hana_cfg),
        daemon=True
    )

    t2 = threading.Thread(
        target=kafka_to_hana_loop,
        args=(hana, tables, kafka_cfg),
        daemon=True
    )

    t1.start()
    t2.start()

    try:
        while True:
            time.sleep(60)

    except KeyboardInterrupt:

        log.info("Shutting down...")

        hana.close()
        producer.flush()


if __name__ == "__main__":
    # Configure logging
    main()