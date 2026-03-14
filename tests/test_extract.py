import pytest
from src.extract.employees_extractor import extract_employees
from src.extract.payments_extractor import extract_payments
from src.extract.sales_extractor import extract_sales
from src.extract.unpaid_extractor import extract_unpaid

def test_extract_sales(mocker):
    """
    Prueba la extracción de ventas.
    Verifica que se llame a get_sales y get_sales_paid con los argumentos correctos
    y que retorne los datos simulados.
    """
    mock_db = mocker.Mock()
    mock_db.get_sales.return_value = [{"id": 1}]
    mock_db.get_sales_paid.return_value = [{"id": 2}]
    
    sales, sales_paid = extract_sales(mock_db, "user", 1, "2026-03-13")
    
    assert sales == [{"id": 1}]
    assert sales_paid == [{"id": 2}]
    mock_db.get_sales.assert_called_once_with("user", 1, "2026-03-13")
    mock_db.get_sales_paid.assert_called_once_with("user", 1, "2026-03-13")

def test_extract_payments(mocker):
    """
    Prueba la extracción de pagos.
    Verifica que se llame a los métodos de la DB correspondientes a pagos y retorne los mocks.
    """
    mock_db = mocker.Mock()
    mock_db.get_payments.return_value = [{"id": 3}]
    mock_db.get_payments_paid.return_value = [{"id": 4}]
    
    payments, payments_paid = extract_payments(mock_db, "user", 1, "2026-03-13")
    
    assert payments == [{"id": 3}]
    assert payments_paid == [{"id": 4}]
    mock_db.get_payments.assert_called_once_with("user", 1, "2026-03-13")
    mock_db.get_payments_paid.assert_called_once_with("user", 1, "2026-03-13")

def test_extract_employees(mocker):
    """
    Prueba la extracción de consumos de empleados.
    Verifica la llamada al método get_for_employee del objeto de conexión.
    """
    mock_db = mocker.Mock()
    mock_db.get_for_employee.return_value = [{"id": 5}]
    
    employees = extract_employees(mock_db, "user", 1, "2026-03-13")
    
    assert employees == [{"id": 5}]
    mock_db.get_for_employee.assert_called_once_with("user", 1, "2026-03-13")

def test_extract_unpaid(mocker):
    """
    Prueba la extracción de cuentas por pagar (unpaid).
    Verifica que se llame a get_payments_unpaid usando solo usuario y ID de tienda.
    """
    mock_db = mocker.Mock()
    mock_db.get_payments_unpaid.return_value = [{"id": 6}]
    
    unpaid = extract_unpaid(mock_db, "user", 1)
    
    assert unpaid == [{"id": 6}]
    mock_db.get_payments_unpaid.assert_called_once_with("user", 1)
