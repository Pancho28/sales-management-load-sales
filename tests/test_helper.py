import os
from helper.email_sender import send_email

def test_send_email_success(mocker):
    """
    Prueba el envío exitoso de un email.
    Verifica:
    1. Que se lean correctamente las variables de entorno.
    2. Que se inicialice e interactúe con el servidor SMTP (starttls, login, send_message).
    """
    # Mock environment variables
    mocker.patch.dict(os.environ, {
        'SMTP_SERVER': 'localhost',
        'SMTP_PORT': '587',
        'SMTP_USER': 'user',
        'SMTP_PASSWORD': 'password',
        'EMAIL_SENDER': 'sender@test.com',
        'EMAIL_RECIPIENT': 'recipient@test.com'
    })
    
    # Mock smtplib
    mock_smtp = mocker.patch("smtplib.SMTP")
    mock_server = mock_smtp.return_value
    
    send_email("Subject", "Body")
    
    mock_smtp.assert_called_once_with("localhost", 587)
    mock_server.starttls.assert_called_once()
    mock_server.login.assert_called_once_with("user", "password")
    mock_server.send_message.assert_called_once()
    mock_server.quit.assert_called_once()

def test_send_email_missing_vars(mocker):
    """
    Valida que no se intente enviar el email si faltan variables de configuración.
    """
    # Missing variables
    mocker.patch.dict(os.environ, {}, clear=True)
    
    # Mock smtplib to ensure it's NOT called
    mock_smtp = mocker.patch("smtplib.SMTP")
    
    send_email("Subject", "Body")
    
    mock_smtp.assert_not_called()

def test_send_email_exception(mocker):
    """
    Valida que el código maneje (capture) una excepción durante el envío sin detener el programa.
    """
    mocker.patch.dict(os.environ, {
        'SMTP_SERVER': 'localhost',
        'SMTP_PORT': '587',
        'SMTP_USER': 'user',
        'SMTP_PASSWORD': 'password',
        'EMAIL_SENDER': 'sender@test.com',
        'EMAIL_RECIPIENT': 'recipient@test.com'
    })
    
    mock_smtp = mocker.patch("smtplib.SMTP", side_effect=Exception("SMTP Error"))
    
    # This should not raise an exception but log an error
    send_email("Subject", "Body")
    
    mock_smtp.assert_called_once()
