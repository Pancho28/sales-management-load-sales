from loguru import logger
import sys

def setup_logger():
    """Configura los handlers de loguru para el proyecto."""
    # Elimina el handler por defecto que envía todo a stderr (Todos los niveles de logs salen como error en railway)
    logger.remove()

    # Configura un handler para los logs de nivel INFO y DEBUG
    # Se envía a stdout y se filtra para que no incluya los logs de nivel WARNING o superior.
    logger.add(sys.stdout,
               level="INFO",
               # Usa logger.level para acceder al número de nivel
               filter=lambda record: record["level"].no < logger.level("WARNING").no)

    # Configura un segundo handler para los logs de nivel WARNING, ERROR y CRITICAL
    # Se envía a stderr.
    logger.add(sys.stderr,
               level="WARNING")
