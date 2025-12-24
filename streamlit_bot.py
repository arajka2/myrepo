import os
import json
import mysql.connector
from dotenv import load_dotenv
import ollama
import logging
import re
import streamlit as st

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
# Load Table Metadata
# =========================================================
try:
    with open("table_metadata.json", "r", encoding="utf-8") as f:
        TABLE_METADATA = json.load(f)
except Exception as e:
    logger.critical(f"Failed to load table_metadata.json: {e}")
    st.error("table_metadata.json missing or corrupted")
    st.stop()

if isinstance(TABLE_METADATA, dict):
    TABLE_METADATA = [TABLE_METADATA]

# =========================================================
# Intent Detection
# =========================================================
def is_ranking_query(query: str) -> bool:
    return any(
        k in query.lower()
        for k in [
            "top", "highest", "lowest", "most", "least",
            "maximum", "minimum", "rank", "order"
        ]
    )

# =========================================================
# Table Selection
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

        keywords = set(table_name.split() + description.split() + columns)
        score = len(query_words.intersection(keywords))

        if score > 0:
            scored.append((score, table))

    if not scored:
        return TABLE_METADATA[:3]

    scored.sort(key=lambda x: x[0], reverse=True)
    return [t for _, t in scored[:3]]

# =========================================================
# SQL Generation
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
        logger.error(f"Ollama error: {e}")
        return None, str(e)

# =========================================================
# Execute SQL
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
# Answer Formatting
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
# Streamlit UI — PERSONAL BOT
# =========================================================
st.set_page_config(
    page_title="Personal Bot",
    layout="wide"
)

st.title("🤖 Personal Bot")
st.caption("MySQL RAG Chatbot powered by Ollama")

# Sidebar
with st.sidebar:
    st.header("System Info")
    st.write("Database:", DB_DATABASE)
    st.write("Model:", "mistral")
    st.markdown("---")
    if st.button("Clear Chat"):
        st.session_state.chat = []

# Initialize chat history
if "chat" not in st.session_state:
    st.session_state.chat = []

# Input
user_query = st.text_input("Ask your database question:")

if st.button("Submit") and user_query:
    logger.info(f"User Query: {user_query}")

    sql, err = generate_sql_query(user_query)
    if err:
        st.error(err)
    else:
        rows, cols, db_err = execute_sql(sql)
        if db_err:
            st.error(db_err)
        else:
            answer = frame_answer(user_query, rows, cols)
            st.session_state.chat.append({
                "question": user_query,
                "sql": sql,
                "answer": answer
            })

# Display chat history
for chat in reversed(st.session_state.chat):
    st.markdown("### You")
    st.write(chat["question"])

    st.markdown("### Generated SQL")
    st.code(chat["sql"], language="sql")

    st.markdown("### Bot")
    st.text(chat["answer"])
