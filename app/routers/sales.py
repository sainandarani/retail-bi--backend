from fastapi import APIRouter
from sqlalchemy import text
from app.db.session import engine

router = APIRouter()

@router.get("/sales/total")
def total_sales():
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT SUM(total_amount) FROM sales_fact")
        )
        total = result.scalar()
    return {"total_sales": total}