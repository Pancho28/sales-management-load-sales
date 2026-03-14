import pytest
import sys
from src.core.config import parse_arguments, AppConfig

def test_parse_arguments_success(mocker):
    """
    Prueba el parseo exitoso de argumentos con 4 parámetros.
    Verifica que se asigne correctamente el entorno, destino, fecha y se calcule la fecha de cierre previa.
    """
    # Mock sys.argv
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
    # formatted_date will be today, so we just check it exists
    assert config.formatted_date is not None

def test_parse_arguments_error(mocker):
    """
    Valida el manejo de errores ante un número incorrecto de argumentos.
    Asegura que el programa intente salir con código 1 (SystemExit).
    """
    mocker.patch.object(sys, 'argv', ['main.py']) # Wrong number of args
    
    with pytest.raises(SystemExit) as excinfo:
        parse_arguments()
    assert excinfo.value.code == 1
