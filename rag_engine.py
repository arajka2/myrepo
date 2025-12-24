import os
import json
import mysql.connector
from dotenv import load_dotenv
import ollama
import tkinter as tk
from tkinter import scrolledtext
import logging
import re

# =========================================================
# Logging
# =========================================================
logging.basicConfig(
    filename="chatbot.log",
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# =========================================================
# Load ENV
# =========================================================
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# =========================================================
# Load Table Metadata (HARDENED)
# =========================================================
try:
    with open("table_metadata.json", "r", encoding="utf-8") as f:
        TABLE_METADATA = json.load(f)
except Exception as e:
    logger.critical(f"Failed to load table_metadata.json: {e}")
    raise SystemExit("table_metadata.json missing or corrupted")

if isinstance(TABLE_METADATA, dict):
    TABLE_METADATA = [TABLE_METADATA]

# =========================================================
# Intent Detection
# =========================================================
def is_ranking_query(query: str) -> bool:
    return any(
        k in query.lower()
        for k in ["top", "highest", "lowest", "most", "least",
                  "maximum", "minimum", "rank", "order"]
    )

# =========================================================
# SAFE TABLE SELECTION (FIXED)
# =========================================================
def select_relevant_tables(query: str):
    query_words = set(query.lower().split())
    scored = []

    for table in TABLE_METADATA:
        table_name = str(table.get("table_name", "")).lower()
        description = str(table.get("description", "")).lower()

        raw_columns = table.get("columns", [])
        columns = []

        for c in raw_columns:
            if isinstance(c, dict) and "name" in c:
                columns.append(c["name"].lower())
            elif isinstance(c, str):
                columns.append(c.lower())
            else:
                logger.warning(f"Invalid column format in {table_name}: {c}")

        keywords = set(table_name.split() + description.split() + columns)
        score = len(query_words.intersection(keywords))

        if score > 0:
            scored.append((score, table))

    if not scored:
        return TABLE_METADATA[:3]

    scored.sort(key=lambda x: x[0], reverse=True)
    return [t for _, t in scored[:3]]

# =========================================================
# SQL GENERATION
# =========================================================
def generate_sql_query(user_query: str):
    tables = select_relevant_tables(user_query)

    schema = "MySQL Schema:\n"
    for t in tables:
        schema += f"\nTable: {t['table_name']}\n"
        for c in t.get("columns", []):
            if isinstance(c, dict):
                schema += f"  - {c['name']} ({c.get('type','')})\n"
            elif isinstance(c, str):
                schema += f"  - {c}\n"

    prompt = f"""
You are a senior MySQL expert.
Generate ONLY a valid SELECT query.

{schema}

Question:
{user_query}

Rules:
- SELECT only
- No explanation
- No markdown
"""

    try:
        response = ollama.chat(
            model="mistral",
            messages=[{"role": "user", "content": prompt}]
        )

        sql = response["message"]["content"].strip()
        sql = re.sub(r"```.*?```", "", sql, flags=re.DOTALL).strip()

        return sql, None
    except Exception as e:
        logger.error(f"Ollama failure: {e}")
        return None, str(e)

# =========================================================
# EXECUTE SQL
# =========================================================
def execute_sql(sql: str):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DATABASE,
            user=DB_USERNAME,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        conn.close()
        return rows, columns, None
    except mysql.connector.Error as e:
        logger.error(f"MySQL error: {e}")
        return None, None, str(e)

# =========================================================
# ANSWER FORMATTING
# =========================================================
def frame_answer(query, rows, cols):
    if not rows:
        return "No results found."

    ranking = is_ranking_query(query)
    text = ""

    for i, row in enumerate(rows, 1):
        line = ", ".join(f"{c}: {v}" for c, v in zip(cols, row))
        text += f"Rank {i}: {line}\n" if ranking else line + "\n"

    return text.strip()

# =========================================================
# TKINTER UI
# =========================================================
def chatbot_ui():
    def handle_query():
        user_query = input_box.get("1.0", tk.END).strip()
        if not user_query:
            return

        output.insert(tk.END, f"You: {user_query}\n\n")

        sql, err = generate_sql_query(user_query)
        if err:
            output.insert(tk.END, f"Error: {err}\n\n")
            return

        output.insert(tk.END, f"SQL:\n{sql}\n\n")

        rows, cols, db_err = execute_sql(sql)
        if db_err:
            output.insert(tk.END, f"DB Error: {db_err}\n\n")
            return

        answer = frame_answer(user_query, rows, cols)
        output.insert(tk.END, f"Bot:\n{answer}\n\n")

        input_box.delete("1.0", tk.END)

    root = tk.Tk()
    root.title("MySQL RAG Chatbot (Ollama)")

    output = scrolledtext.ScrolledText(root, width=100, height=25)
    output.pack(padx=10, pady=10)

    input_box = tk.Text(root, width=100, height=3)
    input_box.pack(padx=10)

    tk.Button(root, text="Ask", command=handle_query).pack(pady=5)
    root.mainloop()
    
    
    # ....................
    
print("DB_HOST:", DB_HOST)
print("DB_PORT:", DB_PORT)
print("DB_DATABASE:", DB_DATABASE)
print("DB_USERNAME:", DB_USERNAME)
    
      


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("Starting chatbot")
    chatbot_ui()
