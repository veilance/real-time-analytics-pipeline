from flask import Flask, render_template
import psycopg2
import os

app = Flask(__name__)

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "stocks")
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASS = os.getenv("DB_PASS", "test123")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )

@app.route("/")
def index():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, window_start, window_end, avg_price, max_price, min_price
        FROM stock_aggregates
        ORDER BY window_start DESC
        LIMIT 50;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("dashboard.html", rows=rows)

@app.route("/api/latest")
def api_latest():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, window_start, window_end, avg_price, max_price, min_price
        FROM stock_aggregates
        ORDER BY window_start DESC
        LIMIT 50;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"data": rows}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
