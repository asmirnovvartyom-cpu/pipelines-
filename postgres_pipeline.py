from typing import List, Union, Generator, Iterator
import psycopg2
import requests
import re

class Pipeline:
def **init**(self):
self.name = "PostgreSQL Smart Agent"
self.conn = None
self.full_schema = None

```
async def on_startup(self):
    self.conn = psycopg2.connect(
        dbname="p_829_1_UVAO",
        user="Natasha",
        password="",
        host="host.docker.internal",
        port=5432
    )
    self.full_schema = self.load_full_schema()

async def on_shutdown(self):
    if self.conn:
        self.conn.close()

# ---------- SCHEMA ----------
def load_full_schema(self):
    cur = self.conn.cursor()
    cur.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
    """)

    schema = {}
    for table, column, dtype in cur.fetchall():
        schema.setdefault(table, []).append(f"{column}({dtype})")

    cur.close()
    return schema

def format_schema(self, tables):
    result = []
    for t in tables:
        if t in self.full_schema:
            cols = ", ".join(self.full_schema[t][:20])
            result.append(f"{t}: {cols}")
    return "\n".join(result)

# ---------- TABLE SELECTION ----------
def select_relevant_tables(self, user_query):
    tables = list(self.full_schema.keys())[:100]

    prompt = f"""
```

You are a database expert.

Tables:
{", ".join(tables)}

User question:
{user_query}

Return ONLY a comma separated list of table names that are relevant.
"""

```
    response = requests.post(
        "http://host.docker.internal:11434/api/generate",
        json={
            "model": "sqlcoder:7b",
            "prompt": prompt,
            "stream": False,
            "temperature": 0.0
        }
    )

    text = response.json()["response"].lower()
    selected = []

    for t in tables:
        if t.lower() in text:
            selected.append(t)

    return selected[:5] if selected else tables[:3]

# ---------- SAMPLE DATA ----------
def get_sample_data(self, tables):
    cur = self.conn.cursor()
    samples = []

    for t in tables[:3]:
        try:
            cur.execute(f"SELECT * FROM {t} LIMIT 3;")
            rows = cur.fetchall()
            samples.append(f"{t} sample: {rows}")
        except:
            continue

    cur.close()
    return "\n".join(samples)

# ---------- SQL GENERATION ----------
def generate_sql(self, user_query, schema, samples):
    prompt = f"""
```

You are a PostgreSQL expert.

Schema:
{schema}

Samples:
{samples}

Rules:

* ONLY SELECT
* NO DELETE, UPDATE, INSERT
* Use JOIN explicitly
* Use LIMIT 50
* PostgreSQL 9.3

User question:
{user_query}

SQL:
SELECT
"""

````
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
        sql = sql.split("```")[1]

    return sql.strip()

# ---------- VALIDATION ----------
def is_safe_sql(self, sql):
    banned = ["insert", "update", "delete", "drop", "alter"]
    sql_lower = sql.lower()
    return not any(b in sql_lower for b in banned)

# ---------- AUTO FIX ----------
def fix_sql(self, bad_sql, error):
    prompt = f"""
````

Fix this PostgreSQL query.

Query:
{bad_sql}

Error:
{error}

Return ONLY fixed SQL:
"""

```
    response = requests.post(
        "http://host.docker.internal:11434/api/generate",
        json={
            "model": "sqlcoder:7b",
            "prompt": prompt,
            "stream": False,
            "temperature": 0.0
        }
    )

    return response.json()["response"].strip()

# ---------- MAIN PIPE ----------
def pipe(
    self,
    user_message: str,
    model_id: str,
    messages: List[dict],
    body: dict
) -> Union[str, Generator, Iterator]:

    try:
        # 1. найти таблицы
        tables = self.select_relevant_tables(user_message)

        # 2. собрать схему
        schema = self.format_schema(tables)

        # 3. взять примеры данных
        samples = self.get_sample_data(tables)

        # 4. сгенерить SQL
        sql = self.generate_sql(user_message, schema, samples)

        print("SQL:", sql)

        if not self.is_safe_sql(sql):
            return f"❌ Заблокировано:\n{sql}"

        # 5. выполнить
        cur = self.conn.cursor()
        cur.execute(sql)

        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()[:50]

        result = f"`{sql}`\n\n"
        result += " | ".join(columns) + "\n"
        result += "-" * 40 + "\n"

        for row in rows:
            result += " | ".join(str(x) for x in row) + "\n"

        return result

    except Exception as e:
        try:
            fixed_sql = self.fix_sql(sql, str(e))
            cur = self.conn.cursor()
            cur.execute(fixed_sql)

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()[:50]

            result = f"🔧 Fixed query:\n`{fixed_sql}`\n\n"
            result += " | ".join(columns) + "\n"
            result += "-" * 40 + "\n"

            for row in rows:
                result += " | ".join(str(x) for x in row) + "\n"

            return result

        except Exception as e2:
            return f"❌ Ошибка:\n{str(e2)}\n\nSQL: {sql}"
```
