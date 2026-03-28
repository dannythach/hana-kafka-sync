from datetime import datetime, date, timezone, timedelta
import json
from sync.state import StateManager

VN_TZ = timezone(timedelta(hours=7))
UTC = timezone.utc

class SyncRunner:
    def __init__(self, hana, producer):
        self.hana = hana
        self.producer = producer
        self.state = StateManager()

    def to_epoch_millis(self, dt: datetime) -> int:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=VN_TZ)
        return int(dt.astimezone(UTC).timestamp() * 1000)

    def run_table(self, tbl):
        last = self.state.load(tbl["state_file"])
        rows = self.hana.query(tbl["sql"], last)
        if not rows:
            return

        schema = json.load(open(f"schemas/{tbl['schema']}.json"))

        for r in rows:
            updated_value = r[tbl["updated_column"]]   # 🔥 giữ bản gốc

            # clone row để gửi Kafka
            record = r.copy()

            for k, v in record.items():
                if isinstance(v, datetime):
                    record[k] = self.to_epoch_millis(v)
                elif isinstance(v, date):
                    dt = datetime(v.year, v.month, v.day)
                    record[k] = self.to_epoch_millis(dt)

            self.producer.send(tbl["topic"], record[tbl["pk"]], schema, record)

            # 🔥 lưu state bằng datetime gốc (string chuẩn SQL)
            if isinstance(updated_value, datetime):
                last = updated_value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                last = str(updated_value)

        self.producer.flush()
        self.state.save(tbl["state_file"], last)