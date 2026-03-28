from kafka import KafkaConsumer
import json
import time
import logging
from datetime import datetime

log = logging.getLogger(__name__)


class HanaConsumer:

    def __init__(self, bootstrap_servers, hana, tables):

        self.hana = hana
        self.tables = tables

        # Logging should be configured in the main application entry point.
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="hana-consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=False
        )

        self.topic_map = {}

        for t in tables:
            if t["direction"] == "pg_to_hana":
                self.topic_map[t["topic"]] = t

        topics = list(self.topic_map.keys())

        if topics:
            self.consumer.subscribe(topics)
            log.info(f"Subscribed to topics: {topics}")
        else:
            log.warning("No pg_to_hana topics found")

    def get_connection(self):

        retry = 0

        while retry < 30:
            try:
                conn = self.hana.conn
                cursor = conn.cursor()
                log.info("Connected to HANA")
                return conn, cursor
            except Exception as e:
                retry += 1
                log.error(f"HANA connection failed ({retry}/30): {e}")
                time.sleep(5)

        raise Exception("Unable to connect to HANA")

    def convert_value(self, val):
        # This logic assumes any large integer is a millisecond timestamp.
        # This can be brittle. It's better to have this defined per-column in config if possible.
        if isinstance(val, int) and val > 1000000000000:
            try:
                # Assuming timestamp is in milliseconds
                return datetime.fromtimestamp(val / 1000)
            except (ValueError, TypeError, OSError):
                # Not a valid timestamp, return original value
                return val

        return val

    def run(self):

        conn, cursor = self.get_connection()

        batch_count = 0
        batch_size = 10

        for msg in self.consumer:

            table = self.topic_map.get(msg.topic)

            if not table:
                continue

            data = msg.value

            log.info(f"Processing topic={msg.topic} payload={data}")

            try:

                schema = table.get("schema")
                target_table = table["target_table"]

                if schema:
                    target = f'"{schema}"."{target_table}"'
                else:
                    target = f'"{target_table}"'

                column_map = table["column_map"]

                cols = []
                vals = []

                for src, tgt in column_map.items():

                    cols.append(tgt)

                    val = data.get(src)
                    val = self.convert_value(val)

                    vals.append(val)

                # build update set (exclude DocEntry)
                update_cols = [
                    f'T."{c}" = S."{c}"'
                    for c in cols if c.lower() != "docentry"
                ]

                insert_cols = ",".join([f'"{c}"' for c in cols])
                source_cols = ",".join([f'S."{c}"' for c in cols])

                placeholders = ",".join(["?"] * len(vals))

                source_select = ",".join([
                                            f'? AS "{c}"'
                                            for c in cols
                                        ])

                sql = f"""
                MERGE INTO {target} T
                USING (
                    SELECT {source_select}
                    FROM DUMMY
                ) S
                ON T."DocEntry" = S."DocEntry"
                WHEN MATCHED THEN
                    UPDATE SET {",".join(update_cols)}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({source_cols})
                """

                cursor.execute(sql, vals)

                batch_count += 1

                if batch_count >= batch_size:

                    conn.commit()
                    self.consumer.commit()

                    batch_count = 0

                    log.info("Committed batch to HANA + Kafka offset")

            except Exception as e:

                log.error(f"Error processing message: {e}", exc_info=True)
                log.error(f"Topic: {msg.topic}")
                log.error(f"Payload: {data}")

                try:
                    conn.rollback()
                except:
                    pass

                conn, cursor = self.get_connection()

        try:
            if batch_count > 0:
                conn.commit()
                self.consumer.commit()
                log.info("Committed remaining batch")
        except Exception as e:
            log.error(f"Failed to commit final batch: {e}")