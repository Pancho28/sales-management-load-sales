from typing import List
from config.db import DBConnection

def extract_unpaid(db: DBConnection, username: str, local_id: int) -> List:
    """
    Extrae los pagos pendientes (unpaid) de un local específico.
    """
    unpaid = db.get_payments_unpaid(username, local_id)
    return unpaid
