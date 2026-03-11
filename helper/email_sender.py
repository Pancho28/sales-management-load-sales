import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from loguru import logger
import os

def send_email(subject: str, body: str):
    """
    Envía un correo electrónico utilizando la configuración SMTP de las variables de entorno.
    
    Args:
        subject (str): Asunto del correo.
        body (str): Cuerpo o mensaje principal del correo.
    """
    smtp_server = os.environ.get('SMTP_SERVER', '')
    smtp_port = os.environ.get('SMTP_PORT', '')
    smtp_user = os.environ.get('SMTP_USER', '')
    smtp_password = os.environ.get('SMTP_PASSWORD', '')
    sender = os.environ.get('EMAIL_SENDER', '')
    recipients_str = os.environ.get('EMAIL_RECIPIENT', '')
    
    if not all([smtp_server, smtp_port, smtp_user, smtp_password, sender, recipients_str]):
        logger.error("No se pudo enviar el email. Faltan variables de entorno SMTP.")
        return
        
    # Soportar múltiples correos separados por comas
    recipients = [email.strip() for email in recipients_str.split(',')]

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()
        logger.info(f"Email enviado correctamente")
    except Exception as e:
        logger.error(f"Error al enviar el email: {e}")
