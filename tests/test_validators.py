import pytest
import pandas as pd
from src.utils.validators import validate_sales_vs_payments, validate_locals_list

def test_validate_sales_vs_payments_ok(mocker):
    """
    Prueba el escenario donde las ventas y pagos están cuadrados.
    Verifica que se llame a send_email con un asunto de 'Éxito' y el conteo correcto.
    """
    # Mock send_email to avoid sending real emails
    mock_send_email = mocker.patch("src.utils.validators.send_email")
    
    # Setup data
    dfSales = pd.DataFrame({"venta": [1, 2, 3]})
    dfPayments = pd.DataFrame({"venta": [1, 2, 3], "cantidad": [10, 20, 30]})
    
    validate_sales_vs_payments(dfSales, dfPayments, "Local Test", "dev")
    
    # Assert
    mock_send_email.assert_called_once()
    assert "Éxito" in mock_send_email.call_args[1]['subject']
    assert "3 ventas registradas" in mock_send_email.call_args[1]['body']

def test_validate_sales_vs_payments_mismatch(mocker):
    """
    Prueba el escenario de descuadre (Ventas != Pagos).
    Verifica que se llame a send_email alertando del descuadre y use el prefijo de producción.
    """
    mock_send_email = mocker.patch("src.utils.validators.send_email")
    
    # Sales (3) != Payments (2)
    dfSales = pd.DataFrame({"venta": [1, 2, 3]})
    dfPayments = pd.DataFrame({"venta": [1, 2], "cantidad": [10, 20]})
    
    validate_sales_vs_payments(dfSales, dfPayments, "Local Test", "prod")
    
    # Assert
    mock_send_email.assert_called_once()
    assert "Descuadre" in mock_send_email.call_args[1]['subject']
    assert "Sales Management - " in mock_send_email.call_args[1]['subject']

def test_validate_locals_list_empty(mocker):
    """
    Valida la reacción cuando la lista de locales de la DB está vacía.
    Verifica que retorne False y envíe un correo de advertencia.
    """
    mock_send_email = mocker.patch("src.utils.validators.send_email")
    
    result = validate_locals_list([], "2026-03-13", "dev")
    
    assert result is False
    mock_send_email.assert_called_once()
    assert "Warning: Sin locales" in mock_send_email.call_args[1]['subject']

def test_validate_locals_list_ok(mocker):
    """
    Valida que si hay locales, retorne True y no envíe correos de advertencia.
    """
    mock_send_email = mocker.patch("src.utils.validators.send_email")
    
    result = validate_locals_list(["Local1", "Local2"], "2026-03-13", "prod")
    
    assert result is True
    mock_send_email.assert_not_called()
