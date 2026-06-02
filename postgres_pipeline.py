from typing import List
import psycopg2
import requests
import re


class Pipeline:
    def __init__(self):
        self.name = "PostgreSQL PRO Agent"
        self.schema = {}
        self.table_priority = []

    # ================= INIT =================

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
        conn.autocommit = True  # 🔥 фикс транзакций
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

    def detect_priority_tables(self):
        priority = []
        for t in self.schema:
            name = t.lower()
            if any(k in name for k in ["citizen", "person", "people", "human", "individual"]):
                priority.append(t)
        return priority if priority else list(self.schema.keys())

    # ================= ROUTER =================

    def is_db_query(self, text):
        keywords = [
            "покажи", "найди", "записи",
            "таблица", "база", "данные",
            "сколько", "фамилия", "адрес"
        ]
        return any(k in text.lower() for k in keywords)

    # ================= NORMALIZE =================

    def normalize(self, text):
        mapping = {
            "люди": "citizen",
            "человек": "citizen",
            "призывники": "citizen",
            "адрес": "address",
            "документы": "document"
        }

        text = text.lower()

        for k, v in mapping.items():
            if k in text:
                text += f" {v}"

        return text

    # ================= SQL GENERATION =================

    def generate_sql(self, query):
        schema_text = "\n".join(
            f"{t}({', '.join(cols[:5])})"
            for t, cols in list(self.schema.items())[:20]
        )

        prompt = f"""
You are PostgreSQL expert.

Schema:
{schema_text}

Rules:
- ONLY SQL
- ONLY SELECT
- NO explanations
- LIMIT 50
- Use real table names only

Task:
{query}

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
                "num_predict": 120
            }
        )

        sql = r.json()["response"]

        # 🔥 ЖЕСТКАЯ очистка мусора
        sql = sql.replace("<s>", "").replace("</s>", "")
        sql = re.sub(r"Cached.*", "", sql)
        sql = re.sub(r"```.*?```", "", sql, flags=re.S)

        match = re.search(r"(SELECT .*?)(;|\n|$)", sql, re.S | re.I)
        if match:
            sql = match.group(1)
        else:
            return None

        return sql.strip()

    # ================= VALIDATION =================

    def is_valid(self, sql):
        if not sql:
            return False

        s = sql.lower()

        if not s.startswith("select"):
            return False

        if " from " not in s:
            return False

        tables = re.findall(r'from\s+([a-zA-Z0-9_]+)', s)

        for t in tables:
            if t not in self.schema:
                return False

        return True

    # ================= FALLBACK =================

    def fallback_query(self, user_message):
        msg = user_message.lower()

        # если спрашивают "сколько"
        if "сколько" in msg:
            t = self.table_priority[0]
            return f"SELECT COUNT(*) FROM {t}"

        # если "люди"
        if "люди" in msg or "человек" in msg:
            t = self.table_priority[0]
            return f"SELECT * FROM {t} LIMIT 10"

        # дефолт
        t = self.table_priority[0]
        return f"SELECT * FROM {t} LIMIT 5"

    # ================= MAIN =================

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict):

        # 👉 НЕ БД → не трогаем
        if not self.is_db_query(user_message):
            return None

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
                res = "\n".join(r[0] for r in cur.fetchall())
                cur.close()
                conn.close()
                return res

            user_message = self.normalize(user_message)

            sql = self.generate_sql(user_message)

            if not self.is_valid(sql):
                sql = self.fallback_query(user_message)

            print("FINAL SQL:", sql)

            cur.execute(sql)

            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

            result = f"`{sql}`\n\n"
            result += " | ".join(cols) + "\n"
            result += "-" * 50 + "\n"

            for r in rows[:50]:
                result += " | ".join(str(x) for x in r) + "\n"

            cur.close()
            conn.close()

            return result

        except Exception as e:
            return f"Ошибка: {str(e)}"
