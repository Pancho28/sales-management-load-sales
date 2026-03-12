from typing import List, Tuple
from config.db import DBConnection

def extract_payments(db: DBConnection, username: str, local_id: int, formatted_date: str) -> Tuple[List, List]:
    """
    Extrae los pagos y pagos pagados de un local específico.
    """
    payments = db.get_payments(username, local_id, formatted_date)
    payments_paid = db.get_payments_paid(username, local_id, formatted_date)
    return payments, payments_paid
