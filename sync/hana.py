from hdbcli import dbapi  # type: ignore
import time
import logging

logger = logging.getLogger(__name__)


class HanaClient:
    def __init__(self, cfg, max_retries=30, retry_interval=5):
        self.cfg = cfg
        self.conn = None
        self._connect_with_retry(max_retries, retry_interval)

    def _connect_with_retry(self, max_retries, retry_interval):
        attempt = 0
        while attempt < max_retries:
            try:
                self.conn = dbapi.connect(**self.cfg)
                logger.info("Connected to HANA successfully")
                return
            except Exception as e:
                attempt += 1
                logger.warning(
                    f"HANA connection failed (attempt {attempt}/{max_retries}): {e}"
                )
                time.sleep(retry_interval)

        raise RuntimeError("Unable to connect to HANA after retries")

    def _ensure_connection(self):
        if self.conn is None:
            self._connect_with_retry(10, 5)

    def query(self, sql, param):
        self._ensure_connection()
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (param,))
            rows = cur.fetchall()
            cols = [c[0].lower() for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]
        except Exception as e:
            logger.error(f"HANA query failed: {e}")
            self.conn = None  # force reconnect next time
            raise
        finally:
            cur.close()

    def close(self):
        if self.conn:
            self.conn.close()
# ✅ ADD THIS FUNCTION
def get_hana_connection():
    import os
    from hdbcli import dbapi

    return dbapi.connect(
        address=os.getenv("HANA_HOST"),
        port=int(os.getenv("HANA_PORT")),
        user=os.getenv("HANA_USER"),
        password=os.getenv("HANA_PASSWORD")
    )