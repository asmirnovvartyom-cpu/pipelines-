from typing import List
import psycopg2
import re


class Pipeline:

    def __init__(self):
        self.schema = {}
        self.tables = []
        self.views = []

        self.core = []
        self.journals = []
        self.priz = []
        self.spr = []
        self.system = []

        self.cache = {}

    # ================= INIT =================

    def on_startup(self):
        conn = self.get_conn()
        self.load_schema(conn)
        conn.close()
        self.classify()

    # ================= DB =================

    def get_conn(self):
        return psycopg2.connect(
            dbname="p_829_1_UVAO",
            user="Natasha",
            password="",
            host="host.docker.internal",
            port=5432
        )

    # ================= SCHEMA =================

    def load_schema(self, conn):
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema='public'
        """)

        self.schema = {}
        for t, c in cur.fetchall():
            self.schema.setdefault(t, []).append(c)

        self.tables = list(self.schema.keys())
        cur.close()

    # ================= CLASSIFY =================

    def classify(self):
        for t in self.tables:
            tl = t.lower()

            if t.startswith("v_"):
                self.views.append(t)

            elif "journal" in tl or "log" in tl:
                self.journals.append(t)

            elif t.startswith("priz"):
                self.priz.append(t)

            elif t.startswith("spr"):
                self.spr.append(t)

            elif any(k in tl for k in ["system", "access", "lock", "dit", "dm"]):
                self.system.append(t)

            else:
                self.core.append(t)

    # ================= INTENT =================

    def intent(self, q: str):
        q = q.lower()

        if any(k in q for k in ["сколько", "число", "count"]):
            return "count"

        if any(k in q for k in ["распределение", "групп", "по типу"]):
            return "group"

        if any(k in q for k in ["последние", "журнал", "лог"]):
            return "timeline"

        if any(k in q for k in ["покажи", "список", "все"]):
            return "list"

        return "llm"

    # ================= SOURCE =================

    def pick_source(self, intent):
        if self.views:
            return self.views[0]
        if self.core:
            return self.core[0]
        if self.journals:
            return self.journals[0]
        if self.tables:
            return self.tables[0]
        return None

    # ================= SQL BUILDER =================

    def build_sql(self, intent, table):

        if not table:
            return None

        if intent == "count":
            return f"SELECT COUNT(*) FROM {table}"

        if intent == "list":
            return f"SELECT * FROM {table} LIMIT 10"

        if intent == "timeline":
            date_col = self.find_date_column(table)
            if date_col:
                return f"SELECT * FROM {table} ORDER BY {date_col} DESC LIMIT 20"
            return f"SELECT * FROM {table} LIMIT 20"

        if intent == "group":
            col = self.find_group_column(table)
            if col:
                return f"SELECT {col}, COUNT(*) as cnt FROM {table} GROUP BY {col} ORDER BY cnt DESC LIMIT 20"
            return f"SELECT * FROM {table} LIMIT 10"

        return None

    # ================= COLUMN DETECT =================

    def find_date_column(self, table):
        cols = self.schema.get(table, [])
        for c in cols:
            if any(k in c.lower() for k in ["date", "time", "created", "updated"]):
                return c
        return None

    def find_group_column(self, table):
        cols = self.schema.get(table, [])

        priority = ["type", "status", "category", "role", "name"]

        for p in priority:
            for c in cols:
                if p in c.lower():
                    return c

        return None

    # ================= VALIDATION =================

    def validate_sql(self, sql):
        if not sql:
            return False

        sql = sql.lower().strip()

        forbidden = ["delete", "update", "insert", "drop", "alter", "truncate"]

        if any(f in sql for f in forbidden):
            return False

        return sql.startswith("select")

    # ================= OPTIONAL LLM =================

    def llm_sql(self, query):
        try:
            import requests

            schema_text = "\n".join(
                f"{t}: {', '.join(cols[:6])}"
                for t, cols in self.schema.items()
            )

            prompt = f"""
ONLY SQL.
NO TEXT.
NO MARKDOWN.
ONLY SELECT.

Schema:
{schema_text}

Question:
{query}

SQL:
"""

            r = requests.post(
                "http://host.docker.internal:11434/api/generate",
                json={
                    "model": "deepseek-coder:6.7b",
                    "prompt": prompt,
                    "stream": False,
                    "temperature": 0
                },
                timeout=20
            )

            data = r.json()

            if "response" not in data:
                return None

            match = re.search(r"(SELECT .*?)(;|\\n|$)", data["response"], re.S | re.I)
            return match.group(1).strip() if match else None

        except:
            return None

    # ================= MAIN PIPE =================

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict):

        # cache
        if user_message in self.cache:
            return self.cache[user_message]

        intent = self.intent(user_message)
        table = self.pick_source(intent)

        sql = self.build_sql(intent, table)

        if not self.validate_sql(sql):
            sql = None

        if not sql:
            sql = self.llm_sql(user_message)

        if not sql and table:
            sql = f"SELECT * FROM {table} LIMIT 10"

        if not self.validate_sql(sql):
            return "No valid SQL generated"

        try:
            conn = self.get_conn()
            cur = conn.cursor()

            cur.execute(sql)

            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.rowcount != 0 else []

            out = f"`{sql}`\n\n"

            if cols:
                out += " | ".join(cols) + "\n"
                out += "-" * 50 + "\n"

            for r in rows[:50]:
                out += " | ".join(str(x) for x in r) + "\n"

            self.cache[user_message] = out

            return out

        except Exception as e:
            return f"DB error: {str(e)}"

        finally:
            try:
                cur.close()
                conn.close()
            except:
                pass
