from typing import List, Union, Generator, Iterator
import psycopg2
import requests
import json

class Pipeline:
    def __init__(self):
        self.name = "PostgreSQL Agent"
        self.conn = None

    async def on_startup(self):
        self.conn = psycopg2.connect(
            dbname="p_829_1_UVAO",
            user="Natasha",
            password="",
            host="host.docker.internal",
            port=5432
        )

    async def on_shutdown(self):
        if self.conn:
            self.conn.close()

    def get_schema(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog','information_schema')
            AND table_type = 'BASE TABLE'
            ORDER BY table_name LIMIT 50;
        """)
        tables = [row[0] for row in cur.fetchall()]
        schema = []
        for table in tables:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position;
            """, (table,))
            cols = cur.fetchall()
            col_str = ", ".join(f"{c[0]} ({c[1]})" for c in cols)
            schema.append(f"{table}: {col_str}")
        cur.close()
        return "\n".join(schema)

    def ask_sqlcoder(self, user_query, schema):
        prompt = f"""Ты SQL-ассистент для PostgreSQL 9.3.
Схема базы:
{schema}

Запрос пользователя: "{user_query}"
Сгенерируй ТОЛЬКО SQL-запрос без пояснений:"""

        response = requests.post(
            "http://host.docker.internal:11434/api/generate",
            json={
                "model": "sqlcoder:latest",
                "prompt": prompt,
                "stream": False,
                "temperature": 0.0
            }
        )
        sql = response.json()["response"].strip()
        if "```" in sql:
            sql = sql.split("```")[1]
            if sql.startswith("sql"):
                sql = sql[3:]
        return sql.strip()

    def pipe(
        self,
        user_message: str,
        model_id: str,
        messages: List[dict],
        body: dict
    ) -> Union[str, Generator, Iterator]:

        try:
            schema = self.get_schema()
            sql = self.ask_sqlcoder(user_message, schema)

            cur = self.conn.cursor()
            cur.execute(sql)

            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                result = f"SQL: {sql}\n\n"
                result += " | ".join(columns) + "\n"
                result += "-" * 50 + "\n"
                for row in rows:
                    result += " | ".join(str(x) for x in row) + "\n"
                return result
            else:
                self.conn.commit()
                return f"SQL: {sql}\n\nЗапрос выполнен успешно."

        except Exception as e:
            self.conn.rollback()
            return f"Ошибка: {str(e)}"
