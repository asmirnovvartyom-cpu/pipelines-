from typing import List
import psycopg2
import re


class Pipeline:

    def __init__(self):
        self.schema = {}
        self.tables = []
        self.journals = []
        self.initialized = False
        self.cache = {}

    # ================= DB =================

    def get_conn(self):
        return psycopg2.connect(
            dbname="p_829_1_UVAO",
            user="Natasha",
            password="",
            host="host.docker.internal",
            port=5432
        )

    # ================= INIT =================

    def init_schema(self):
        if self.initialized:
            return

        conn = self.get_conn()
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

        # journals cache
        self.journals = [t for t in self.tables if "journal" in t.lower() or "log" in t.lower()]

        cur.close()
        conn.close()

        self.initialized = True

    # ================= NORMALIZE =================

    def norm(self, text: str):
        return re.sub(r"[^a-z0-9а-я_]+", "", text.lower().replace(" ", "_"))

    # ================= TABLE MATCH =================

    def extract_table(self, query: str):
        q = self.norm(query)

        best = None
        best_score = 0

        for t in self.tables:
            tn = self.norm(t)

            score = 0

            if tn in q:
                score += 100
            if q in tn:
                score += 80

            # token overlap
            q_tokens = set(re.findall(r"[a-zа-я0-9]+", q))
            t_tokens = set(re.findall(r"[a-zа-я0-9]+", tn))

            if q_tokens and t_tokens:
                score += len(q_tokens & t_tokens) * 20

            if score > best_score:
                best_score = score
                best = t

        if best_score < 20:
            return None

        return best

    # ================= 🔥 FIX: RESOLVE TABLE (ГЛАВНЫЙ ФИКС) =================

    def resolve_table(self, query: str):
        # 1. smart match
        t = self.extract_table(query)
        if t:
            return t

        q = query.lower()

        # 2. semantic hints
        if any(k in q for k in ["журнал", "лог", "события"]):
            return self.journals[0] if self.journals else self.tables[0]

        if any(k in q for k in ["access", "доступ"]):
            for t in self.tables:
                if "access" in t.lower():
                    return t

        if any(k in q for k in ["покажи", "список", "данные", "все", "строки", "записи"]):
            return self.tables[0]

        # 3. hard fallback (НЕ ЛОМАЕТСЯ НИКОГДА)
        return self.tables[0]

    # ================= INTENT =================

    def intent(self, q: str):
        q = q.lower()

        if any(k in q for k in ["сколько", "count", "число"]):
            return "count"

        if any(k in q for k in ["групп", "распределение"]):
            return "group"

        if any(k in q for k in ["журнал", "лог"]):
            return "timeline"

        if any(k in q for k in ["покажи", "список", "все", "данные"]):
            return "list"

        return "list"

    # ================= SQL =================

    def build_sql(self, intent, table):

        if intent == "count":
            return f"SELECT COUNT(*) FROM {table}"

        if intent == "group":
            cols = self.schema.get(table, [])
            col = cols[0] if cols else "*"
            return f"SELECT {col}, COUNT(*) FROM {table} GROUP BY {col} LIMIT 20"

        if intent == "timeline":
            return f"SELECT * FROM {table} LIMIT 20"

        return f"SELECT * FROM {table} LIMIT 10"

    # ================= VALIDATION =================

    def validate_sql(self, sql):
        if not sql:
            return False
        sql = sql.lower()
        forbidden = ["delete", "update", "insert", "drop", "alter", "truncate"]
        return sql.startswith("select") and not any(f in sql for f in forbidden)

    # ================= PIPE =================

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict):

        self.init_schema()

        intent = self.intent(user_message)

        # 🔥 FIXED ROUTING
        table = self.resolve_table(user_message)

        sql = self.build_sql(intent, table)

        if not self.validate_sql(sql):
            sql = f"SELECT * FROM {table} LIMIT 10"

        try:
            conn = self.get_conn()
            cur = conn.cursor()

            cur.execute(sql)

            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []

            out = f"`{sql}`\n\n"

            if cols:
                out += " | ".join(cols) + "\n"
                out += "-" * 50 + "\n"

            for r in rows[:50]:
                out += " | ".join(str(x) for x in r) + "\n"

            return out

        except Exception as e:
            return f"DB error: {str(e)}"

        finally:
            try:
                cur.close()
                conn.close()
            except:
                pass
