from typing import List, Union, Generator, Iterator
import psycopg2

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

    def pipe(
        self,
        user_message: str,
        model_id: str,
        messages: List[dict],
        body: dict
    ) -> Union[str, Generator, Iterator]:

        try:
            cur = self.conn.cursor()
            cur.execute(user_message)
            
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                result = " | ".join(columns) + "\n"
                result += "-" * 50 + "\n"
                for row in rows:
                    result += " | ".join(str(x) for x in row) + "\n"
                return result
            else:
                self.conn.commit()
                return "Запрос выполнен успешно."
                
        except Exception as e:
            self.conn.rollback()
            return f"Ошибка: {str(e)}"
