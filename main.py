from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2

app = FastAPI()

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
    cur.execute(
        """
        SELECT
            SUM(total_amount) AS total_sales,
            SUM(quantity) AS units_sold,
            ROUND(
              SUM(total_amount) / NULLIF(COUNT(DISTINCT order_id), 0),
              2
            ) AS atv,
            ROUND(
              SUM(total_amount) / NULLIF(COUNT(DISTINCT order_id), 0),
              2
            ) AS aov
        FROM sales;
    """
    )
    row = cur.fetchone()

    return {
        "total_sales": row[0] or 0,
        "units_sold": row[1] or 0,
        "atv": row[2] or 0,
        "aov": row[3] or 0,
    }

    @app.get("/top-products")
    def top_products():
        cur.execute(
            """
        SELECT product_name, SUM(total_amount)
        FROM sales
        GROUP BY product_name
        ORDER BY SUM(total_amount) DESC
        LIMIT 5
    """
        )

    rows = cur.fetchall()
    return [{"product": r[0], "sales": r[1]} for r in rows]


@app.get("/")
def read_root():
    return {"message": "API is running"}
