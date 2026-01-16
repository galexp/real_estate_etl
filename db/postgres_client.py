import psycopg2
from psycopg2.extras import execute_batch

class PostgresClient:
    def __init__(self, host, db, user, password):
        self.conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password
        )
        self.conn.autocommit = True

    def execute_batch(self, query, data):
        with self.conn.cursor() as cur:
            execute_batch(cur, query, data)

    def close(self):
        self.conn.close()

    def fetch_existing_property_ids(self):
        query = "SELECT property_id FROM properties"
        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
        return set(row[0] for row in results)

