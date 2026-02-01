from fastapi import FastAPI
from sqlalchemy import text
from app.db.session import engine

app = FastAPI()
from app.routers import sales
app.include_router(sales.router, prefix="/api")

@app.get("/")
def root():
    return {"message": "Retail BI Backend Running"}

@app.get("/api/test-db")
def test_db():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        return {"db_status": "connected"}