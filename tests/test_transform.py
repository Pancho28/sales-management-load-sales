import pandas as pd
from src.transform.employees_transformer import transform_employees
from src.transform.payments_transformer import transform_payments
from src.transform.sales_transformer import transform_sales
from src.transform.unpaid_transformer import transform_unpaid

def test_transform_employees():
    """
    Valida la transformación de datos crudos de empleados.
    Verifica:
    1. Que el resultado sea un DataFrame.
    2. Que se añada la columna 'fechacierre'.
    3. Que se asigne correctamente el nombre del local.
    4. Que se eliminen columnas temporales como 'fechacreacion'.
    """
    employees = [
        [1, 10.0, 400.0, "2026-03-13 10:00:00", "Prod1", "Cat1", 10.0, 1]
    ]
    close_date = "2026-03-13"
    local_name = "Test Local"
    
    df = transform_employees(employees, close_date, local_name)
    
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 1
    assert "fechacierre" in df.columns
    assert df.iloc[0]["local"] == local_name
    assert "fechacreacion" not in df.columns

def test_transform_sales():
    """
    Valida la consolidación y transformación de ventas.
    Verifica:
    1. El mapeo del día de la semana.
    2. La extracción correcta de la hora desde la fecha de creación.
    3. La estructura del DataFrame resultante.
    """
    sales = [
        [1, 50.0, 2000.0, "2026-03-13 12:00:00", "Prod A", "Cat A", 50.0, 1, "2026-03-13"]
    ]
    sales_paid = []
    close_date = "2026-03-13"
    local_name = "Sales Local"
    
    df = transform_sales(sales, sales_paid, close_date, local_name)
    
    assert df.shape[0] == 1
    assert "dia" in df.columns
    assert "hora" in df.columns
    assert df.iloc[0]["hora"] == "12"

def test_transform_payments():
    """
    Valida la transformación de pagos recibidos.
    Verifica:
    1. La concatenación de pagos normales y pagos ya procesados.
    2. La limpieza de duplicados y eliminación de columnas innecesarias.
    """
    payments = [
        [1, 50.0, 2000.0, 50.0, "Cash", "USD", "2026-03-13 12:00:00"]
    ]
    payments_paid = [
        [2, 30.0, 1200.0, 30.0, "Card", "USD", "2026-03-13 13:00:00"]
    ]
    close_date = "2026-03-13"
    local_name = "Payment Local"
    
    df = transform_payments(payments, payments_paid, close_date, local_name)
    
    assert df.shape[0] == 2
    assert df.iloc[0]["local"] == local_name
    assert "fechacreacion" not in df.columns

def test_transform_unpaid():
    """
    Valida la transformación de cuentas por pagar (unpaid).
    Verifica la asignación del local y que el DataFrame mantenga la integridad de los datos crudos.
    """
    unpaid = [
        [1, 100.0, 4000.0, "John", "Doe", "2026-03-13 10:00:00"]
    ]
    local_name = "Unpaid Local"
    
    df = transform_unpaid(unpaid, local_name)
    
    assert df.shape[0] == 1
    assert df.iloc[0]["local"] == local_name
    assert "nombre" in df.columns
