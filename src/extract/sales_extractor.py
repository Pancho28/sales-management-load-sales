from typing import List, Tuple
from config.db import DBConnection

def extract_sales(db: DBConnection, username: str, local_id: int, formatted_date: str) -> Tuple[List, List]:
    """
    Extrae las ventas y las ventas pagadas de un local específico.
    """
    sales = db.get_sales(username, local_id, formatted_date)
    sales_paid = db.get_sales_paid(username, local_id, formatted_date)
    return sales, sales_paid
