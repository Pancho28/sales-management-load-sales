from loguru import logger
import pandas as pd
from helper.email_sender import send_email

def validate_sales_vs_payments(dfSales: pd.DataFrame, dfPayments: pd.DataFrame, local_name: str):
    """
    Valida el cuadre de Ventas vs Pagos y envía un correo con el resultado.
    """
    sales_count = dfSales.venta.nunique() if 'venta' in dfSales.columns else 0
    payments_count = dfPayments.venta.nunique() if 'venta' in dfPayments.columns else 0
    
    if sales_count == payments_count:
        logger.info(f'Validación OK: Ventas ({sales_count}) == Pagos ({payments_count})')
        # Se prepara mensaje de éxito
        total_usd = dfPayments['cantidad'].sum() if 'cantidad' in dfPayments.columns else 0
        send_email(
            subject=f"Sales Management - Éxito: {local_name}",
            body=f"El ETL para el local '{local_name}' procesó y se guardó exitosamente.\nLos datos están cuadrados: {sales_count} ventas registradas.\nTotal procesado: ${total_usd:,.2f}"
        )
    else:
        logger.warning(f'Descuadre en {local_name}: Ventas ({sales_count}) != Pagos ({payments_count})')
        # Se prepara mensaje de error/descuadre
        send_email(
            subject=f"Sales Management - Descuadre de datos: {local_name}",
            body=f"Se detectó un descuadre en la información para el local '{local_name}'.\n\nMétricas guardadas:\n- Total Ventas Únicas procesadas: {sales_count}\n- Total Pagos Únicos procesados: {payments_count}\n\nFavor revisar la información origen del local."
        )

def validate_locals_list(locals_list: list, formatted_date: str) -> bool:
    """
    Valida si la lista de locales obtenidos de la base de datos está vacía.
    Envía una advertencia si no hay locales.
    Retorna True si hay locales a procesar, False en caso contrario.
    """
    if len(locals_list) == 0:
        logger.warning('No locals to process')
        send_email(
            subject=f"Sales Management - Warning: Sin locales a procesar",
            body=f"La ejecución del ETL para la fecha {formatted_date} no retornó locales activos para procesar."
        )
        return False
        
    logger.info(f'Total locals {len(locals_list)}')
    return True
