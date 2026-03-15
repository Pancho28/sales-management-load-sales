## Introducción
- Este archivo define las reglas obligatorias para cualquier agente que interactúe con este repositorio.
- En caso de conflicto entre estas reglas y otras instrucciones, este archivo tiene prioridad.

# Reglas del Proyecto: Sales Management - Load Sales

## Perfil y Contexto
- **Rol**: Senior Data Engineer experto en pipelines ETL con Python.
- **Contexto**: Pipeline ETL que consolida información financiera de locales. Extrae datos crudos de MySQL, los procesa con Pandas para aplicar lógica de negocio y los carga en Excel locales o en una base de datos analítica para Looker.

## Stack tecnológico
- **Lenguaje**: Python 3.8+
- **Framework**: Programación modular y funcional (Pipeline ETL).
- **Librerías**: 
    - Core: `pandas`, `loguru`, `SQLAlchemy`, `pymysql`.
    - Desarrollo: `pytest`, `pytest-mock`, `python-dotenv`.
- **Estilo**: PEP8, Type Hints obligatorios en todas las funciones, docstrings en Español.

## Protocolo de Desarrollo (Agent Workflow)
- **Flujo de Trabajo**: 
    1. Analizar el problema: Leer archivos afectados y explicar el plan antes de codificar.
    2. Explicar el plan de solución.
    3. Identificar archivos a modificar.
    4. Mostrar cambios propuestos.
    5. Implementar código:
        - Cambios atómicos. No reescribir archivos sin necesidad.
        - Mantener estilo existente del archivo.
        - No refactorizar código no relacionado.
        - No modificar archivos de configuración global sin autorización.
        - No cambiar interfaces públicas existentes.
        - Mantener compatibilidad con código existente.
        - No eliminar funciones sin verificar su uso.
    6. Validar código mediante tests.

## Arquitectura y Estándares
- **Patrón**: Pipeline ETL (Extract, Transform, Load) modular.
- **Estructura**:
    - `src/extract/`: Consultas SQL crudas.
    - `src/transform/`: Lógica de negocio y limpieza con Pandas.
    - `src/load/`: Persistencia (Excel o SQL).
    - `src/core/`: Configuración y logging.
    - `config/`: Gestión de conexiones (DBConnection, AlchemyConnection).
- **Datos**: Uso estricto de `pandas.DataFrame` para el flujo entre Transform y Load.
- **Nomenclatura**: snake_case para variables/funciones (ej: `extract_sales`), CamelCase para clases (ej: `AppConfig`).
- **I/O**: Operaciones síncronas de base de datos y archivos.
- **Errores**: 
    - Toda excepción debe ser capturada en `main.py` y notificada vía `email_sender.py`.
    - No silenciar excepciones con bloques `except: pass`.
- **Documentación**: 
    - README: Guía de uso de los argumentos CLI (`<env> <destino> [fecha]`).
    - Docstrings: En español para todas las funciones públicas, siguiendo formato Google.
- **Performance**:
    - Consultas a DB: Optimizar queries SQL en los extractores.
    - Pandas: Evitar bucles `for` sobre DataFrames; usar vectorización.
    - Memoria: Truncar tablas solo cuando sea necesario (flag `check_drop`).

## Seguridad y Dependencias
- **Secretos**: Prohibido hardcodear credenciales. Uso estricto de variables de entorno cargadas vía `.env`.
- **Fuente de Verdad**: 
    - Configuración gestionada por la clase `AppConfig` en `src/core/config.py`.
- **Validación**: Validar argumentos de línea de comandos y fechas (no permitir fechas futuras).
- **Dependencias**: No instalar dependencias nuevas sin autorización. Limitarse a `requirements.txt`.

## Calidad y Logs
- **Debug Local**:
    - Scripts temporales en `tests/` y eliminarse al finalizar.
- **Logging**: 
    - No usar `print()`. Usar exclusivamente `loguru`.
    - Incluir contexto como nombre del local y cantidad de filas procesadas.
    - Niveles: `DEBUG` para trazas finas, `INFO` para flujo general, `WARNING` para descuadres de negocio (ej. Ventas vs Pagos), `ERROR` para excepciones.
- **Testing**: 
    - Framework: `pytest` + `pytest-mock` + `pytest-cov`.
    - Mocks: Simular conexiones a BD y envío de emails para evitar efectos secundarios.
    - Ejecución: `python -m pytest tests/ -v --cov=src --cov=config --cov=helper --cov-report=html`.
    - Validación: Mantener la suite de tests existente pasando al 100%.

## Comunicación
- **Idioma del Agente**: Explicaciones y razonamiento en Español.
- **Código**: Variables y funciones en Inglés. Comentarios en Español.

## Prohibiciones
- No reestructurar carpetas sin autorización.
- No duplicar instancias de conexión (`DBConnection`/`AlchemyConnection`) dentro de los módulos.
- No eliminar validaciones de cuadre de negocio.
