from typing import List, Union, Generator, Iterator
import psycopg2
import requests
import re


class Pipeline:
    def __init__(self):
        self.name = "PostgreSQL Stable Agent"
        self.full_schema = {}

    async def on_startup(self):
        conn = self.get_conn()
        self.full_schema = self.load_schema(conn)
        conn.close()

    def get_conn(self):
        conn = psycopg2.connect(
            dbname="p_829_1_UVAO",
            user="Natasha",
            password="",
            host="host.docker.internal",
            port=5432
        )
        conn.autocommit = True  # 🔥 ключевой фикс
        return conn

    # ================= SCHEMA =================

    def load_schema(self, conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)

        schema = {}
        for t, c in cur.fetchall():
            schema.setdefault(t, []).append(c)

        cur.close()
        return schema

    # ================= NORMALIZE =================

    def normalize(self, text):
        mapping = {
            "люди": "citizens",
            "человек": "citizens",
            "призывники": "priz",
            "адрес": "adres",
            "документы": "documents"
        }

        text = text.lower()

        for k, v in mapping.items():
            if k in text:
                text += f" {v}"

        return text

    # ================= SQL =================

    def generate_sql(self, query):
        schema_text = "\n".join(
            f"{t}({', '.join(cols[:5])})"
            for t, cols in list(self.full_schema.items())[:30]
        )

        prompt = f"""
Schema:
{schema_text}

Rules:
- Only SELECT
- Use LIMIT 50
- Use existing tables only

Task:
{query}

SQL:
SELECT
"""

        r = requests.post(
            "http://host.docker.internal:11434/api/generate",
            json={
                "model": "sqlcoder:7b",
                "prompt": prompt,
                "stream": False,
                "temperature": 0
            }
        )

        sql = "SELECT " + r.json()["response"].strip()

        if "```" in sql:
            sql = re.sub(r"```.*?```", "", sql, flags=re.S)

        return sql.strip()

    # ================= VALIDATION =================

    def is_valid(self, sql):
        s = sql.lower()

        if not s.startswith("select"):
            return False
        if " from" not in s:
            return False

        # проверка таблиц
        tables = re.findall(r'from\s+([a-zA-Z0-9_]+)', s)
        for t in tables:
            if t not in self.full_schema:
                return False

        return True

    def fallback_query(self):
        # берём первую нормальную таблицу
        for t in self.full_schema:
            return f"SELECT * FROM {t} LIMIT 5"

    # ================= MAIN =================

    def pipe(
        self,
        user_message: str,
        model_id: str,
        messages: List[dict],
        body: dict
    ):

        try:
            conn = self.get_conn()
            cur = conn.cursor()

            # список таблиц
            if "таблиц" in user_message.lower():
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    LIMIT 100
                """)
                return "\n".join(r[0] for r in cur.fetchall())

            user_message = self.normalize(user_message)

            sql = self.generate_sql(user_message)
            print("SQL:", sql)

            if not self.is_valid(sql):
                print("BAD SQL -> fallback")
                sql = self.fallback_query()

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
