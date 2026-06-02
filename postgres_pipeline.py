from typing import List
import psycopg2
import requests
import re


class Pipeline:
    def __init__(self):
        self.name = "PostgreSQL ULTRA STABLE"
        self.schema = {}
        self.table_priority = []

    async def on_startup(self):
        conn = self.get_conn()
        self.schema = self.load_schema(conn)
        self.table_priority = self.detect_priority_tables()
        conn.close()

    def get_conn(self):
        conn = psycopg2.connect(
            dbname="p_829_1_UVAO",
            user="Natasha",
            password="",
            host="host.docker.internal",
            port=5432
        )
        conn.autocommit = True
        return conn

    # ================= SCHEMA =================

    def load_schema(self, conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
        """)
        schema = {}
        for t, c in cur.fetchall():
            schema.setdefault(t, []).append(c)
        cur.close()
        return schema

    def detect_priority_tables(self):
        priority = []
        for t in self.schema:
            if any(k in t.lower() for k in ["citizen", "person", "human", "individual"]):
                priority.append(t)
        return priority if priority else list(self.schema.keys())

    # ================= ROUTER =================

    def is_db_query(self, text):
        return any(k in text.lower() for k in [
            "покажи", "найди", "записи", "таблица",
            "база", "данные", "сколько"
        ])

    # ================= SQL =================

    def generate_sql(self, query):
        try:
            prompt = f"""
ONLY SQL. ONLY SELECT. NO TEXT.

Task: {query}

SQL:
SELECT
"""

            r = requests.post(
                "http://host.docker.internal:11434/api/generate",
                json={
                    "model": "deepseek-coder:6.7b",
                    "prompt": prompt,
                    "stream": False,
                    "temperature": 0,
                    "num_predict": 100
                },
                timeout=30
            )

            data = r.json()

            # 🔥 главный фикс
            if "response" not in data:
                print("OLLAMA ERROR:", data)
                return None

            sql = data["response"]

            # чистка
            sql = sql.replace("<s>", "").replace("</s>", "")
            sql = re.sub(r"```.*?```", "", sql, flags=re.S)

            match = re.search(r"(SELECT .*?)(;|\n|$)", sql, re.S | re.I)
            return match.group(1).strip() if match else None

        except Exception as e:
            print("GEN ERROR:", e)
            return None

    # ================= FALLBACK =================

    def fallback(self, user_message):
        msg = user_message.lower()
        t = self.table_priority[0]

        if "сколько" in msg:
            return f"SELECT COUNT(*) FROM {t}"

        if "люди" in msg:
            return f"SELECT * FROM {t} LIMIT 10"

        return f"SELECT * FROM {t} LIMIT 5"

    # ================= MAIN =================

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict):

        if not self.is_db_query(user_message):
            return None

        try:
            conn = self.get_conn()
            cur = conn.cursor()

            if "таблиц" in user_message.lower():
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                """)
                res = "\n".join(r[0] for r in cur.fetchall())
                cur.close()
                conn.close()
                return res

            sql = self.generate_sql(user_message)

            if not sql:
                sql = self.fallback(user_message)

            print("SQL:", sql)

            cur.execute(sql)

            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

            result = f"`{sql}`\n\n"
            result += " | ".join(cols) + "\n"
            result += "-" * 40 + "\n"

            for r in rows[:50]:
                result += " | ".join(str(x) for x in r) + "\n"

            cur.close()
            conn.close()

            return result

        except Exception as e:
            return f"Ошибка: {str(e)}"
