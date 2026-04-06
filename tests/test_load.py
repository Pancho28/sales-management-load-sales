import pandas as pd
from src.load.excel_loader import load_to_excel
from src.load.sql_loader import load_to_sql

def test_load_to_excel(mocker):
    """
    Prueba el guardado en Excel.
    Asegura que se intente crear el directorio de destino y se llame al escritor de Excel de Pandas
    sin llegar a tocar el disco real.
    """
    # Mock os.makedirs and pd.ExcelWriter to avoid IO
    mocker.patch("os.makedirs")
    # Patch pd.ExcelWriter at the module level
    mocker.patch("src.load.excel_loader.pd.ExcelWriter")
    # Also patch to_excel to be absolutely sure no IO happens
    mock_to_excel = mocker.patch("pandas.DataFrame.to_excel")
    
    df = pd.DataFrame({"col": [1]})
    load_to_excel("user", "2026-03-13", df, df, df, df)
    
    # Check that to_excel was called for sales and payments
    assert mock_to_excel.call_count >= 2

def test_load_to_sql(mocker):
    """
    Prueba el guardado en base de datos SQL.
    Verifica:
    1. Que si check_drop es True, se realice el truncate de la tabla especificada.
    2. Que se invoque el método to_sql de los DataFrames.
    """
    mock_alchemy = mocker.Mock()
    mock_motor = mocker.Mock()
    # Mock pd.DataFrame.to_sql directly
    mock_to_sql = mocker.patch("pandas.DataFrame.to_sql")
    
    df = pd.DataFrame({"col": [1]})
    
    # Test with check_drop = True
    result = load_to_sql(mock_alchemy, mock_motor, "user", df, df, df, df, True)
    
    mock_alchemy.truncate_table.assert_called_once_with('por_pagar')
    assert result is False
    assert mock_to_sql.call_count >= 2
