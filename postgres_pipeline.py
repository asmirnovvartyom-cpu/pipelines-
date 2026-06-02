from typing import List, Union, Generator, Iterator
import psycopg2
import requests
import re


class Pipeline:
    def __init__(self):
        self.name = "PostgreSQL Smart Agent"
        self.full_schema = {}

    async def on_startup(self):
        conn = self.get_conn()
        self.full_schema = self.load_full_schema(conn)
        conn.close()

    async def on_shutdown(self):
        pass

    # ================= CONNECTION =================

    def get_conn(self):
        return psycopg2.connect(
            dbname="p_829_1_UVAO",
            user="Natasha",
            password="",
            host="host.docker.internal",
            port=5432
        )

    # ================= SCHEMA =================

    def load_full_schema(self, conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name NOT LIKE 'pg_%'
            AND table_name NOT LIKE 'sql_%'
            ORDER BY table_name, ordinal_position;
        """)

        schema = {}
        for table, column, dtype in cur.fetchall():
            schema.setdefault(table, set()).add(f"{column}({dtype})")

        cur.close()

        clean_schema = {}
        for t, cols in schema.items():
            if len(cols) > 2:
                clean_schema[t] = list(cols)

        return clean_schema

    # ================= NORMALIZATION =================

    def normalize_query(self, text):
        mapping = {
            "люди": "citizens",
            "человек": "citizens",
            "призывники": "priz",
            "адрес": "adres",
            "документы": "documents"
        }

        text_lower = text.lower()

        for k, v in mapping.items():
            if k in text_lower:
                text_lower += f" (table hint: {v})"

        return text_lower

    # ================= TABLE SELECTION =================

    def select_relevant_tables(self, query):
        query_words = set(query.lower().split())

        scored = []
        for table, cols in self.full_schema.items():
            score = 0

            if table.lower() in query.lower():
                score += 3

            for col in cols:
                col_name = col.split("(")[0]
                if col_name.lower() in query_words:
                    score += 1

            if score > 0:
                scored.append((table, score))

        scored.sort(key=lambda x: x[1], reverse=True)

        top_tables = [t[0] for t in scored[:50]]

        if not top_tables:
            top_tables = list(self.full_schema.keys())[:20]

        return top_tables

    def build_schema_prompt(self, tables):
        parts = []
        for t in tables:
            cols = self.full_schema[t][:15]
            parts.append(f"{t}({', '.join(cols)})")
        return "\n".join(parts)

    # ================= SQL =================

    def generate_sql(self, query, schema):
        prompt = f"""
### Database schema:
{schema}

### Rules:
- PostgreSQL 9.3
- ONLY SELECT
- NO DELETE/UPDATE/INSERT
- Use JOIN if needed
- LIMIT 50

### Task:
{query}

### SQL:
SELECT
"""

        response = requests.post(
            "http://host.docker.internal:11434/api/generate",
            json={
                "model": "sqlcoder:7b",
                "prompt": prompt,
                "stream": False,
                "temperature": 0.0
            }
        )

        sql = "SELECT " + response.json()["response"].strip()

        if "```" in sql:
            sql = re.sub(r"```.*?```", "", sql, flags=re.S)

        return sql.strip()

    # ================= VALIDATION =================

    def is_valid_sql(self, sql):
        s = sql.lower()
        if not s.startswith("select"):
            return False
        if "select ." in s:
            return False
        if " from" not in s:
            return False
        return True

    def is_safe(self, sql):
        bad = ["delete", "update", "insert", "drop", "alter"]
        return not any(b in sql.lower() for b in bad)

    # ================= MAIN =================

    def pipe(
        self,
        user_message: str,
        model_id: str,
        messages: List[dict],
        body: dict
    ) -> Union[str, Generator, Iterator]:

        conn = None
        cur = None

        try:
            conn = self.get_conn()

            # лечим aborted транзакции
            try:
                conn.rollback()
            except:
                pass

            cur = conn.cursor()

            # fallback: список таблиц
            if "таблиц" in user_message.lower():
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    ORDER BY table_name
                    LIMIT 100;
                """)
                rows = cur.fetchall()
                return "\n".join(r[0] for r in rows)

            user_message = self.normalize_query(user_message)

            tables = self.select_relevant_tables(user_message)
            schema = self.build_schema_prompt(tables)

            sql = self.generate_sql(user_message, schema)

            print("SQL:", sql)

            if not self.is_valid_sql(sql):
                return f"❌ некорректный SQL:\n{sql}"

            if not self.is_safe(sql):
                return "❌ Запрос заблокирован (опасная операция)"

            try:
                cur.execute(sql)
            except Exception as e:
                fix_prompt = f"""
Fix PostgreSQL query:

{sql}

Error:
{str(e)}

Return only SQL:
"""
                response = requests.post(
                    "http://host.docker.internal:11434/api/generate",
                    json={
                        "model": "sqlcoder:7b",
                        "prompt": fix_prompt,
                        "stream": False,
                        "temperature": 0.0
                    }
                )

                fixed_sql = response.json()["response"].strip()
                print("FIXED:", fixed_sql)

                if not self.is_valid_sql(fixed_sql):
                    return f"❌ не удалось исправить SQL:\n{fixed_sql}"

                cur.execute(fixed_sql)
                sql = fixed_sql

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()[:50]

            result = f"`{sql}`\n\n"
            result += " | ".join(columns) + "\n"
            result += "-" * 50 + "\n"

            for r in rows:
                result += " | ".join(str(x) for x in r) + "\n"

            return result

        except Exception as e:
            return f"Ошибка: {str(e)}"

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
