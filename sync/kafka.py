import json
from kafka import KafkaProducer
from decimal import Decimal


def convert(o):
    if isinstance(o, Decimal):
        return float(o)
    return o


class ConnectProducer:
    def __init__(self, servers):
        self.p = KafkaProducer(
            bootstrap_servers=servers,
            acks="all",
            retries=5,
            value_serializer=lambda v: json.dumps(v, default=convert).encode(),
            key_serializer=lambda v: v.encode()
        )

    def send(self, topic, key, schema, payload):
        self.p.send(
            topic,
            key=str(key),
            value={"schema": schema, "payload": payload}
        ).get(timeout=10)

    def flush(self):
        self.p.flush()