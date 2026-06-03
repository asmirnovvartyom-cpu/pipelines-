"""
UVAO Database Pipeline for Open WebUI
Модель: qwen2.5-coder:14b
База данных: uvao (PostgreSQL 127.0.0.1:5432)
Поддерживает: SELECT, INSERT, UPDATE, DELETE
"""

import re
import json
import psycopg2
import requests
from typing import Generator, Iterator

# ─── ПОЛНАЯ СХЕМА БАЗЫ ДАННЫХ ────────────────────────────────────────────────
DB_SCHEMA = {
    "access_config": ["id (integer)", "username (character varying)", "section (smallint)", "mode (boolean)"],
    "all_spec": ["code (text)", "name (character varying)"],
    "all_spec_selected": ["code (text)", "name (character varying)", "parent (text)"],
    "citizens_ern": ["id (bigint)", "guid (character varying)", "iduk (character varying)", "idern (character varying)", "error_code (character varying)", "error_type (character varying)", "error_text (character varying)", "date_in (timestamp without time zone)", "surname (character varying)", "name (character varying)", "middle_name (character varying)", "birth_date (date)", "birth_place (character varying)", "iduk_vk (character varying)", "ervu_load (smallint)", "date_idern (timestamp without time zone)", "id_info (character varying)", "old_idern (character varying)", "date_annul (timestamp without time zone)", "status (smallint)", "idervu (character varying)"],
    "delayed_individual": ["id (integer)", "id_asovu (character varying)", "surname (character varying)", "name (character varying)", "patronymic (character varying)", "birthday (date)", "year_of_birth (integer)", "place_of_birth (character varying)", "snils (character varying)", "inn (character varying)", "passport_serial_number (character varying)", "passport_number (character varying)", "passport_date_of_issue (date)", "passport_issued_by (character varying)", "education_place (character varying)", "education_level (character varying)", "phone_number (character varying)", "home_phone_number (character varying)", "work_phone_number (character varying)", "vu_serial_number (character varying)", "vu_number (character varying)", "vu_date_of_issue (date)", "orphan (boolean)", "dead (boolean)", "having_children (integer)", "family_status (character varying)", "death_date (date)", "ernid (character varying)", "residence_address (character varying)", "registration_adress (character varying)", "registration_adress_city (character varying)", "registration_district (character varying)", "registration_adress_street (character varying)", "registration_adress_house (character varying)", "registration_adress_flat (character varying)", "registration_adress_postalcode (character varying)", "vu_category (character varying)", "suitability_category (character varying)", "blood_type (character varying)", "rhesus (character varying)", "height (character varying)", "body_mass (character varying)", "sex (character varying)", "outside_rf (boolean)", "illness_incompatible_military (boolean)", "difficult_family_situation (boolean)", "criminal_record (boolean)", "speciality (character varying)", "course_number (integer)", "work_place (character varying)", "comissariat_name (character varying)", "comissariat_id (character varying)", "iduk (character varying)", "idasovu (character varying)", "is_found (boolean)", "has_duplicates (boolean)", "cossack (boolean)"],
    "delayed_relatives": ["id (integer)", "name (character varying)", "relation_degree (character varying)", "phone_number (character varying)", "birthday (date)", "place_of_birth (character varying)", "work_place (character varying)", "address (character varying)", "fk_delayed_individual (integer)", "surname (character varying)", "patronymic (character varying)"],
    "delayed_sports": ["id (integer)", "sport_type (character varying)", "qualification (character varying)", "fk_delayed_individual (integer)"],
    "dit_data": ["id (integer)", "p001 (bigint)", "dit_attribute (character varying)", "irs_table (character varying)", "irs_column (character varying)", "dit_value (character varying)", "prev_value_as_string (character varying)", "was_matched (boolean)", "was_updated (boolean)", "read_timestamp (timestamp with time zone)"],
    "dit_reg_adress": ["id (integer)", "p001 (bigint)", "registration_adress (character varying)", "registration_adress_city (character varying)", "registration_district (character varying)", "registration_adress_street (character varying)", "registration_adress_house (character varying)", "registration_adress_housing (character varying)", "registration_adress_building (character varying)", "registration_adress_flat (character varying)", "registration_adress_postalcode (character varying)"],
    "dit_sync": ["ordinal_num (integer)", "p001 (text)", "iduk (text)", "id_asovu_before (text)", "id_asovu_after (text)", "change_time (timestamp without time zone)", "id_file (text)"],
    "dit_verification": ["id (integer)", "p001 (integer)", "id_asovu (character varying)", "calling_reason (character varying)", "appearence_date (timestamp with time zone)", "verification (boolean)", "cancellation_reason (character varying)", "date_checked (timestamp with time zone)", "was_found (boolean)", "iduk (character varying)", "ern_id (character varying)", "surname (character varying)", "name (character varying)", "patronymic (character varying)", "birthday (date)", "passport_serial_number (character varying)", "passport_number (character varying)", "passport_issued_by (character varying)", "passport_date_of_issue (date)", "inn (character varying)", "snils (character varying)", "guid (character varying)"],
    "dm_version": ["id (integer)", "revision (integer)", "package_id (character varying)", "comment (text)", "gen_date (date)"],
    "documents": ["id (integer)", "p001 (integer)", "ext (character varying)", "r5014 (character varying)", "date_in (timestamp without time zone)", "dop_text (character varying)"],
    "dpriz01": ["p001 (integer)", "p005 (character varying)", "p006 (character varying)", "p007 (character varying)", "k011 (integer)", "r6011 (character varying)", "p069 (integer)", "r2011 (character varying)", "p070 (character varying)", "p080 (character varying)", "p081 (character varying)", "p013 (integer)", "r7103 (character varying)", "r7703 (character varying)", "p411 (integer)", "p412 (timestamp without time zone)", "r7013 (character varying)", "p413 (integer)", "p014 (timestamp without time zone)", "r1016 (character varying)", "p002 (timestamp without time zone)", "p003 (character varying)", "p004 (character varying)", "p072 (timestamp without time zone)", "p100 (character varying)", "l0002 (character varying)"],
    "dpriz02": ["p001 (integer)", "p073 (timestamp without time zone)", "r4171 (character varying)"],
    "gir": ["p001 (bigint)", "fam (character varying)", "im (character varying)", "ot (character varying)", "dr (date)", "mr_addr_text (character varying)", "mr_addr_index (character varying)", "mr_addr_region (character varying)", "mr_addr_city (character varying)", "mr_addr_locality (character varying)", "mr_addr_house (character varying)", "mr_addr_flat (character varying)", "mp_addr_text (character varying)", "mp_addr_city (character varying)", "mp_addr_house (character varying)", "mp_addr_flat (character varying)"],
    "gsp_nar": ["id (integer)", "p2001_g7 (timestamp without time zone)", "r6205_g7 (character varying)", "p302_g7 (character varying)", "p049_g7 (character varying)", "r7012_g7 (character varying)", "p105_g7 (timestamp without time zone)", "r9004_g7 (character varying)", "p304_g7 (integer)", "p107_g7 (character varying)", "p113_g7 (character varying)", "r4043_g7 (character varying)", "p104_g7 (character varying)", "p106_g7 (timestamp without time zone)", "r4054_g7 (character varying)", "r1016_g7 (character varying)", "r6206_g7 (character varying)"],
    "index_address": ["id (integer)", "post_index (character varying)", "region (character varying)", "city (character varying)", "street (character varying)", "house (character varying)"],
    "locktable": ["id (integer)", "locked (boolean)", "locked_by (character varying)", "locked_at (timestamp without time zone)"],
    "log": ["id (integer)", "username (character varying)", "action (character varying)", "table_name (character varying)", "record_id (character varying)", "old_value (text)", "new_value (text)", "action_time (timestamp without time zone)"],
    "month": ["id (integer)", "name (character varying)", "short_name (character varying)"],
    "vizov": ["p001 (integer)", "d_viz (timestamp without time zone)", "l0010 (character varying)", "d_pk (timestamp without time zone)", "r4049 (character varying)", "pr (character varying)", "typ (character varying)", "p_num (character varying)", "l0078 (character varying)", "guid_poves (character varying)", "guid_spisok (character varying)", "was_exported (boolean)", "was_signed (boolean)"],
    "vizov_ppgvu": ["p001 (integer)", "d_viz (timestamp without time zone)", "l0010 (character varying)", "d_pk (timestamp without time zone)", "r4049 (character varying)", "pr (character varying)", "typ (character varying)", "p_num (character varying)"],
    "vremmery": ["id (integer)", "p001 (integer)", "isapply (boolean)", "r8012 (character varying)", "reshenapply_num (character varying)", "reshenapply_date (date)", "apply_date (date)", "reshencancel_num (character varying)", "reshencancel_date (date)", "cancel_date (date)", "tip (character varying)", "status (character varying)"],
    "weeklyspr": ["id (integer)", "codevk (character varying)", "id_name (integer)", "id_date (integer)", "a1 (numeric)", "a2 (numeric)", "a3 (numeric)", "a4 (numeric)", "a5 (numeric)", "a6 (numeric)", "a7 (numeric)", "a8 (numeric)", "a9 (numeric)", "a10 (numeric)", "a11 (numeric)", "a12 (numeric)", "a13 (numeric)", "a14 (numeric)", "a15 (numeric)"],
    "weeklyspr_names": ["id (integer)", "number (integer)", "name (character varying)", "r7012 (character varying)", "spr (integer)", "gr (integer)", "yr (character varying)", "formula (character varying)", "control (character varying)"],
}

# ─── НАСТРОЙКИ ───────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "uvao",
    "user": "postgres",
    "password": "your_password_here",  # ← ЗАМЕНИ НА СВОЙ ПАРОЛЬ
    "connect_timeout": 10,
}

OLLAMA_URL = "http://host.docker.internal:11434"
MODEL = "qwen2.5-coder:14b-instruct-q4_K_M"

# Запрещённые операции
FORBIDDEN = ["DROP ", "TRUNCATE ", "ALTER ", "CREATE ", "GRANT ", "REVOKE "]


# ─── PIPELINE CLASS ───────────────────────────────────────────────────────────
class Pipeline:
    def __init__(self):
        self.name = "UVAO Database Pipeline"
        self.description = "Работа с базой данных УВАО через qwen2.5-coder:14b"
        # Кэш схемы для быстрого поиска нужных таблиц
        self._schema_cache = None

    def get_schema_cache(self):
        """Возвращает строку со всей схемой для промпта."""
        if self._schema_cache is None:
            lines = []
            for table, cols in DB_SCHEMA.items():
                lines.append(f"TABLE {table}: {', '.join(cols)}")
            self._schema_cache = "\n".join(lines)
        return self._schema_cache

    def find_relevant_tables(self, user_message: str) -> str:
        """Находит таблицы релевантные вопросу пользователя."""
        msg_lower = user_message.lower()
        relevant = []

        # Ключевые слова → таблицы
        keywords = {
            "citizens_ern": ["гражданин", "citizen", "ern", "физлиц", "человек", "лицо"],
            "delayed_individual": ["призывник", "delayed", "individual", "отсрочк", "военнообязан", "снилс", "инн", "паспорт", "фамили", "имя", "отчеств"],
            "delayed_relatives": ["родственник", "relative", "семья"],
            "delayed_sports": ["спорт", "sport", "квалификац"],
            "dit_data": ["дит", "dit", "атрибут", "синхрониз"],
            "dit_verification": ["верификац", "проверк", "явк"],
            "documents": ["документ", "document", "файл"],
            "gir": ["гир", "gir", "адрес", "address", "прописк", "регистрац", "проживан"],
            "gsp_nar": ["наряд", "gsp", "расписан"],
            "log": ["лог", "log", "история", "действи", "журнал событ"],
            "vizov": ["вызов", "vizov", "повестк", "призыв"],
            "vizov_ppgvu": ["ppgvu", "вызов"],
            "vremmery": ["мера", "врем", "врemmery", "решени", "отмен"],
            "weeklyspr": ["недел", "weekly", "отчёт", "отчет", "сводк"],
            "weeklyspr_names": ["недел", "weekly", "название"],
            "index_address": ["индекс", "почтов", "index_address"],
            "access_config": ["доступ", "access", "пользовател", "роль"],
            "dm_version": ["верси", "version", "ревизи"],
            "dpriz01": ["призыв", "dpriz", "dpriz01"],
            "dpriz02": ["призыв", "dpriz02"],
            "month": ["месяц", "month"],
        }

        for table, kws in keywords.items():
            for kw in kws:
                if kw in msg_lower:
                    if table in DB_SCHEMA:
                        relevant.append(table)
                    break

        # Если ничего не нашли — даём основные таблицы
        if not relevant:
            relevant = ["delayed_individual", "citizens_ern", "gir", "documents", "vizov"]

        # Убираем дубликаты, формируем строку схемы
        relevant = list(dict.fromkeys(relevant))
        lines = []
        for table in relevant:
            if table in DB_SCHEMA:
                cols = ", ".join(DB_SCHEMA[table])
                lines.append(f"TABLE {table}: {cols}")

        return "\n".join(lines)

    def build_system_prompt(self, relevant_schema: str) -> str:
        return f"""Ты — SQL-ассистент для базы данных PostgreSQL (база: uvao).
Ты умеешь читать, добавлять, изменять и удалять данные.

СХЕМА ДОСТУПНЫХ ТАБЛИЦ:
{relevant_schema}

ПРАВИЛА:
1. Всегда возвращай ТОЛЬКО SQL запрос — без пояснений, без markdown, без ```sql блоков
2. Для SELECT запросов добавляй LIMIT 50 если пользователь не указал другое
3. Для UPDATE и DELETE ВСЕГДА используй WHERE — никогда не обновляй/удаляй всю таблицу
4. Запрещено: DROP, TRUNCATE, ALTER, CREATE, GRANT, REVOKE
5. Имена колонок и таблиц пиши точно как в схеме
6. Отвечай только SQL, ничего лишнего"""

    def extract_sql(self, text: str) -> str:
        """Извлекает SQL из ответа модели."""
        # Убираем markdown блоки
        text = re.sub(r'```sql\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'```\s*', '', text)
        text = text.strip()

        # Берём первый SQL запрос
        sql_match = re.search(
            r'(SELECT|INSERT|UPDATE|DELETE)\s+.+',
            text,
            re.IGNORECASE | re.DOTALL
        )
        if sql_match:
            return sql_match.group(0).strip()
        return text.strip()

    def is_safe(self, sql: str) -> tuple[bool, str]:
        """Проверяет безопасность SQL."""
        sql_upper = sql.upper()
        for forbidden in FORBIDDEN:
            if forbidden in sql_upper:
                return False, f"Запрещённая операция: {forbidden.strip()}"

        # Проверяем UPDATE/DELETE без WHERE
        if re.search(r'\bUPDATE\b', sql_upper) and not re.search(r'\bWHERE\b', sql_upper):
            return False, "UPDATE без WHERE запрещён — укажите условие"
        if re.search(r'\bDELETE\b', sql_upper) and not re.search(r'\bWHERE\b', sql_upper):
            return False, "DELETE без WHERE запрещён — укажите условие"

        return True, ""

    def execute_sql(self, sql: str) -> str:
        """Выполняет SQL и возвращает результат."""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute(sql)

            sql_upper = sql.strip().upper()

            if sql_upper.startswith("SELECT"):
                rows = cur.fetchall()
                if not rows:
                    conn.close()
                    return "Запрос выполнен. Данных не найдено."

                # Форматируем результат
                col_names = [desc[0] for desc in cur.description]
                result_lines = [" | ".join(col_names)]
                result_lines.append("-" * len(result_lines[0]))
                for row in rows:
                    result_lines.append(" | ".join(str(v) if v is not None else "NULL" for v in row))

                conn.close()
                return f"Найдено строк: {len(rows)}\n\n" + "\n".join(result_lines)

            elif sql_upper.startswith("INSERT"):
                conn.commit()
                affected = cur.rowcount
                conn.close()
                return f"✅ Вставлено строк: {affected}"

            elif sql_upper.startswith("UPDATE"):
                conn.commit()
                affected = cur.rowcount
                conn.close()
                return f"✅ Обновлено строк: {affected}"

            elif sql_upper.startswith("DELETE"):
                conn.commit()
                affected = cur.rowcount
                conn.close()
                return f"✅ Удалено строк: {affected}"

            else:
                conn.commit()
                conn.close()
                return "✅ Запрос выполнен."

        except psycopg2.Error as e:
            return f"❌ Ошибка PostgreSQL: {e}"
        except Exception as e:
            return f"❌ Ошибка: {e}"

    def ask_model(self, system_prompt: str, user_message: str) -> str:
        """Отправляет запрос в Ollama."""
        try:
            response = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 512,
                    }
                },
                timeout=120,
            )
            data = response.json()
            return data["message"]["content"].strip()
        except requests.exceptions.ConnectionError:
            return "ERROR: Не удалось подключиться к Ollama. Проверь что Ollama запущена."
        except Exception as e:
            return f"ERROR: {e}"

    def pipe(
        self,
        user_message: str,
        model_id: str,
        messages: list,
        body: dict,
    ) -> str:
        """Основной метод pipeline."""

        # 1. Находим релевантные таблицы
        relevant_schema = self.find_relevant_tables(user_message)

        # 2. Строим системный промпт
        system_prompt = self.build_system_prompt(relevant_schema)

        # 3. Запрашиваем SQL у модели
        sql_raw = self.ask_model(system_prompt, user_message)

        if sql_raw.startswith("ERROR:"):
            return sql_raw

        # 4. Извлекаем чистый SQL
        sql = self.extract_sql(sql_raw)

        if not sql:
            return f"❌ Модель не вернула SQL запрос.\nОтвет модели: {sql_raw}"

        # 5. Проверяем безопасность
        safe, reason = self.is_safe(sql)
        if not safe:
            return f"🚫 Запрос заблокирован: {reason}\n\nSQL: {sql}"

        # 6. Выполняем SQL
        result = self.execute_sql(sql)

        # 7. Возвращаем результат с SQL для прозрачности
        return f"**SQL запрос:**\n```sql\n{sql}\n```\n\n**Результат:**\n{result}"
