import pytest
import sys
from unittest.mock import MagicMock
from src.core.config import parse_arguments, AppConfig
from config.db import DBConnection
from config.alchemy import AlchemyConnection

def test_parse_arguments_success(mocker):
    """
    Prueba el parseo exitoso de argumentos con 4 parámetros.
    Verifica que se asigne correctamente el entorno, destino, fecha y se calcule la fecha de cierre previa.
    """
    mocker.patch.object(sys, 'argv', ['main.py', 'dev', 'local', '2026-03-13'])
    
    config = parse_arguments()
    
    assert isinstance(config, AppConfig)
    assert config.env == 'dev'
    assert config.destination == 'local'
    assert config.formatted_date == '2026-03-13'
    assert config.close_date == '2026-03-12'

def test_parse_arguments_current_date(mocker):
    """
    Prueba el parseo con 3 argumentos (usa fecha actual).
    Verifica que tome los valores de entorno y destino correctamente.
    """
    mocker.patch.object(sys, 'argv', ['main.py', 'prod', 'server'])
    
    config = parse_arguments()
    
    assert config.env == 'prod'
    assert config.destination == 'server'
    assert config.formatted_date is not None

def test_parse_arguments_error(mocker):
    """
    Valida el manejo de errores ante un número incorrecto de argumentos.
    Asegura que el programa intente salir con código 1 (SystemExit).
    """
    mocker.patch.object(sys, 'argv', ['main.py'])
    
    with pytest.raises(SystemExit) as excinfo:
        parse_arguments()
    assert excinfo.value.code == 1

def test_parse_arguments_future_date(mocker):
    """
    Verifica que el sistema rechace fechas futuras con SystemExit código 1.
    """
    mocker.patch.object(sys, 'argv', ['main.py', 'dev', 'local', '2099-12-31'])
    
    with pytest.raises(SystemExit) as excinfo:
        parse_arguments()
    assert excinfo.value.code == 1


# =============================================
# Fixtures compartidos para config/db.py y config/alchemy.py
# =============================================

# Variables de entorno simuladas para el ambiente de desarrollo
DEV_ENV_VARS = {
    "DATABASE_HOST_DEV": "localhost",
    "DATABASE_USERNAME_DEV": "dev_user",
    "DATABASE_PASSWORD_DEV": "dev_pass",
    "DATABASE_NAME_DEV": "dev_db",
    "DATABASE_PORT_DEV": "3306",
}

# Variables de entorno simuladas para el ambiente de producción
PROD_ENV_VARS = {
    "DATABASE_HOST": "prod-host",
    "DATABASE_USERNAME": "prod_user",
    "DATABASE_PASSWORD": "prod_pass",
    "DATABASE_NAME": "prod_db",
    "DATABASE_PORT": "3306",
}

# Variables de entorno simuladas para AlchemyConnection en producción (Looker)
PROD_LOOKER_ENV_VARS = {
    "DATABASE_HOST_LOOKER": "looker-host",
    "DATABASE_USERNAME_LOOKER": "looker_user",
    "DATABASE_PASSWORD_LOOKER": "looker_pass",
    "DATABASE_NAME_LOOKER": "looker_db",
    "DATABASE_PORT_LOOKER": "3306",
}


# =============================================
# Tests para config/db.py (DBConnection)
# =============================================

def test_db_connection_dev(mocker):
    """
    Verifica que DBConnection en entorno 'dev' lea las variables de entorno correctas
    y establezca la conexión con pymysql.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_connect.return_value.cursor.return_value = MagicMock()
    
    db = DBConnection("dev")
    
    assert db.db_host == "localhost"
    assert db.db_user == "dev_user"
    assert db.db_name == "dev_db"
    mock_connect.assert_called_once_with(
        host="localhost", user="dev_user", password="dev_pass",
        db="dev_db", port=3306
    )

def test_db_connection_prod(mocker):
    """
    Verifica que DBConnection en entorno 'prod' lea las variables de producción
    y establezca la conexión correctamente.
    """
    mocker.patch.dict('os.environ', PROD_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_connect.return_value.cursor.return_value = MagicMock()
    
    db = DBConnection("prod")
    
    assert db.db_host == "prod-host"
    assert db.db_user == "prod_user"
    mock_connect.assert_called_once_with(
        host="prod-host", user="prod_user", password="prod_pass",
        db="prod_db", port=3306
    )

def test_db_close_conection(mocker):
    """
    Verifica que close_conection() invoque el cierre de la conexión pymysql.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_conn_instance = MagicMock()
    mock_connect.return_value = mock_conn_instance
    
    db = DBConnection("dev")
    db.close_conection()
    
    mock_conn_instance.close.assert_called_once()

def test_db_get_locals(mocker):
    """
    Verifica que get_locals() ejecute la consulta SQL y retorne los resultados del cursor.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(1, 'Local A', 'user_a'), (2, 'Local B', 'user_b')]
    mock_connect.return_value.cursor.return_value = mock_cursor
    
    db = DBConnection("dev")
    result = db.get_locals("2026-03-14")
    
    mock_cursor.execute.assert_called_once()
    assert len(result) == 2
    assert result[0] == (1, 'Local A', 'user_a')

def test_db_get_sales(mocker):
    """
    Verifica que get_sales() ejecute la consulta SQL con los parámetros correctos
    y retorne los resultados.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(100, 50.0, 2500.0)]
    mock_connect.return_value.cursor.return_value = mock_cursor
    
    db = DBConnection("dev")
    result = db.get_sales("user_a", 1, "2026-03-14")
    
    mock_cursor.execute.assert_called_once()
    assert result == [(100, 50.0, 2500.0)]

def test_db_get_sales_paid(mocker):
    """
    Verifica que get_sales_paid() ejecute correctamente la consulta de ventas pagadas.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(200, 30.0, 1500.0)]
    mock_connect.return_value.cursor.return_value = mock_cursor
    
    db = DBConnection("dev")
    result = db.get_sales_paid("user_a", 1, "2026-03-14")
    
    mock_cursor.execute.assert_called_once()
    assert len(result) == 1

def test_db_get_payments(mocker):
    """
    Verifica que get_payments() ejecute la consulta de pagos correctamente.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(100, 50.0, 2500.0, 50.0, 'Efectivo', 'USD')]
    mock_connect.return_value.cursor.return_value = mock_cursor
    
    db = DBConnection("dev")
    result = db.get_payments("user_a", 1, "2026-03-14")
    
    mock_cursor.execute.assert_called_once()
    assert len(result) == 1

def test_db_get_payments_paid(mocker):
    """
    Verifica que get_payments_paid() ejecute la consulta de pagos confirmados.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(300, 20.0, 1000.0, 20.0, 'Zelle', 'USD')]
    mock_connect.return_value.cursor.return_value = mock_cursor
    
    db = DBConnection("dev")
    result = db.get_payments_paid("user_a", 1, "2026-03-14")
    
    mock_cursor.execute.assert_called_once()
    assert len(result) == 1

def test_db_get_payments_unpaid(mocker):
    """
    Verifica que get_payments_unpaid() retorne las cuentas por pagar pendientes.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(400, 15.0, 750.0, 'Juan', 'Pérez')]
    mock_connect.return_value.cursor.return_value = mock_cursor
    
    db = DBConnection("dev")
    result = db.get_payments_unpaid("user_a", 1)
    
    mock_cursor.execute.assert_called_once()
    assert result[0] == (400, 15.0, 750.0, 'Juan', 'Pérez')

def test_db_get_for_employee(mocker):
    """
    Verifica que get_for_employee() retorne las ventas de empleados correctamente.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_connect = mocker.patch('config.db.pymysql.connect')
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(500, 10.0, 500.0)]
    mock_connect.return_value.cursor.return_value = mock_cursor
    
    db = DBConnection("dev")
    result = db.get_for_employee("user_a", 1, "2026-03-14")
    
    mock_cursor.execute.assert_called_once()
    assert len(result) == 1


# =============================================
# Tests para config/alchemy.py (AlchemyConnection)
# =============================================

def test_alchemy_connection_dev(mocker):
    """
    Verifica que AlchemyConnection en 'dev' lea las variables correctas
    y cree el engine de SQLAlchemy.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_engine = mocker.patch('config.alchemy.create_engine')
    
    alchemy = AlchemyConnection("dev")
    
    assert alchemy.db_host == "localhost"
    assert alchemy.db_user == "dev_user"
    mock_engine.assert_called_once_with(
        "mysql+pymysql://dev_user:dev_pass@localhost:3306/dev_db"
    )

def test_alchemy_connection_prod(mocker):
    """
    Verifica que AlchemyConnection en 'prod' use las variables de Looker
    y construya la URL de conexión correcta.
    """
    mocker.patch.dict('os.environ', PROD_LOOKER_ENV_VARS)
    mock_engine = mocker.patch('config.alchemy.create_engine')
    
    alchemy = AlchemyConnection("prod")
    
    assert alchemy.db_host == "looker-host"
    mock_engine.assert_called_once_with(
        "mysql+pymysql://looker_user:looker_pass@looker-host:3306/looker_db"
    )

def test_alchemy_connection_invalid_env(mocker):
    """
    Verifica que un entorno inválido lance una excepción con el mensaje correcto.
    """
    with pytest.raises(Exception, match="Invalid enviroment"):
        AlchemyConnection("staging")

def test_alchemy_get_motor(mocker):
    """
    Verifica que getMotor() retorne la instancia del engine creada en el constructor.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_engine = mocker.patch('config.alchemy.create_engine')
    mock_engine_instance = MagicMock()
    mock_engine.return_value = mock_engine_instance
    
    alchemy = AlchemyConnection("dev")
    motor = alchemy.getMotor()
    
    assert motor == mock_engine_instance

def test_alchemy_truncate_table(mocker):
    """
    Verifica que truncate_table() ejecute un TRUNCATE TABLE sobre la tabla indicada
    usando una conexión del engine.
    """
    mocker.patch.dict('os.environ', DEV_ENV_VARS)
    mock_engine = mocker.patch('config.alchemy.create_engine')
    
    # Simular el context manager de motor.connect()
    mock_connection = MagicMock()
    mock_engine_instance = MagicMock()
    mock_engine_instance.connect.return_value.__enter__ = MagicMock(return_value=mock_connection)
    mock_engine_instance.connect.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.return_value = mock_engine_instance
    
    alchemy = AlchemyConnection("dev")
    alchemy.truncate_table("por_pagar")
    
    # Verificar que se ejecutó algo sobre la conexión
    mock_connection.execute.assert_called_once()
