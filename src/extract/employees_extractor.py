from typing import List
from config.db import DBConnection

def extract_employees(db: DBConnection, username: str, local_id: int, formatted_date: str) -> List:
    """
    Extrae las ventas de empleados de un local específico.
    """
    employees = db.get_for_employee(username, local_id, formatted_date)
    return employees
