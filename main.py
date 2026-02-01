from sqlalchemy import create_engine
from sqlalchemy import text
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2

app = FastAPI()

DATABASE_URL = "postgresql://postgres:root@localhost:5432/Retail_BI"
@app.get("/")
def root():
    return {"message": "Retail BI Backend Running"}

@app.get("/retail")
def retail():
    return {"status": "Retail API working"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

conn = psycopg2.connect(
    host="localhost", database="Retail_BI", user="postgres", password="root"
)
cur = conn.cursor()


@app.get("/kpis")
def get_kpis():
    cur.execute("""
        SELECT
            COALESCE(SUM(total_amount), 0) AS total_sales,
            COALESCE(SUM(quantity), 0) AS total_units,
            COALESCE(
                ROUND(
                    SUM(total_amount) / NULLIF(COUNT(DISTINCT order_id), 0),
                    2
                ),
                0
            ) AS avg_order_value
        FROM sales;
    """)

    row = cur.fetchone()

    return {
        "total_sales": float(row[0]),
        "total_units": int(row[1]),
        "avg_order_value": float(row[2])
    }



@app.get("/top-products")
def top_products():
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            p.product_name,
            SUM(s.total_amount) AS total_sales
        FROM sales s
        JOIN products p ON s.product_id = p.product_id
        GROUP BY p.product_name
        ORDER BY total_sales DESC
        LIMIT 5;
    """)
    rows = cur.fetchall()
    return [{"product": r[0], "total_sales": float(r[1])} for r in rows]

@app.get("/test-db")
def test_db():
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        return {"db_status": "connected"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/health-sales")
def check_sales():
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM sales;")
    count = cur.fetchone()[0]
    return {"status": "ok", "total_rows": count}

@app.get("/sales-total")
def total_sales():
    cur = conn.cursor()
    cur.execute("SELECT SUM(total_amount) FROM sales;")
    total = cur.fetchone()[0] or 0
    return {"total_sales": float(total)}

@app.get("/sales-by-date")
def sales_by_date():
    cur = conn.cursor()
    cur.execute("""
        SELECT sale_date, SUM(total_amount)
        FROM sales
        GROUP BY sale_date
        ORDER BY sale_date;
    """)
    rows = cur.fetchall()
    return [{"date": str(r[0]), "total": float(r[1])} for r in rows]

@app.get("/top-stores")
def top_stores():
    cur = conn.cursor()
    cur.execute("""
        SELECT store_id, SUM(total_amount) AS total_sales
        FROM sales
        GROUP BY store_id
        ORDER BY total_sales DESC;
    """)
    rows = cur.fetchall()
    return [{"store": r[0], "total_sales": float(r[1])} for r in rows]