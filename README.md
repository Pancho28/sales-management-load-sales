# Sales Management - ETL de Ventas

Este proyecto implementa un proceso ETL (Extract, Transform, Load) para consolidar la información financiera y operativa de los locales de Sales Management. 

Extrae datos crudos directamente desde una base de datos MySQL (ventas, pagos, cuentas por pagar, ventas de empleados), aplica transformaciones y validaciones de negocio utilizando `pandas`, y carga los resultados consolidados ya sea en archivos Excel locales o en una base de datos analítica (para visualización en Looker).

## 🚀 Características Principales

- **Arquitectura Modular (ETL):** Procesos de extracción, transformación y carga completamente separados.
- **Validación Automática:** Sistema de alertas y cuadres automáticos entre "Ventas" vs "Pagos".
- **Notificaciones por Correo:** Avisos de éxito o descuadres (Alertas de falta de locales).
- **Procesamiento de Fechas Pasadas:** Permite reprocesar fechas específicas en caso de fallos.

## 📂 Estructura del Proyecto

```text
src/
├── core/         # Configuración y logueo (Loguru)
├── extract/      # Consultas SQL para extracción de datos crudos
├── transform/    # Lógica de negocio y limpieza utilizando Pandas
├── load/         # Carga y persistencia de datos (Excel o SQL Model)
└── utils/        # Validaciones y correos de notificación
tests/            # Pruebas unitarias y mocks de sistema
agent.md          # Guía de reglas y estándares para agentes IA
```

## ⚙️ Requisitos e Instalación

1. Asegúrate de tener **Python 3.8+** instalado.
2. Clona el repositorio y crea un entorno virtual (recomendado):
   ```bash
   # En Windows:
   python -m venv load_sales
   .\load_sales\Scripts\activate
   
   # En macOS o Linux:
   python3 -m venv load_sales
   source load_sales/bin/activate
   ```
3. Instala las dependencias (incluyendo herramientas de desarrollo):
   ```bash
   pip install -r requirements.txt
   ```
4. Configura el archivo `.env`: Copia el archivo `.env.sample` a un nuevo archivo `.env` y llena las variables correspondientes (credenciales de DB, correos, etc).

## 💻 Uso

El proyecto se ejecuta a través de la terminal mediante el script `main.py`. Requiere **tres argumentos posicionales**, con un cuarto argumento **opcional**.

```bash
python main.py <ambiente> <destino> [fecha_reproceso]
```

### Argumentos:

- `<ambiente>`: Determina qué BD usar. 
  - `dev` = Ambiente de desarrollo/pruebas.
  - `prod` = Ambiente de producción (Ventas reales).
- `<destino>`: Determina dónde guardar los datos procesados.
  - `local` = Genera archivos de reporte `.xlsx` en el directorio `../locals sales`.
  - `server` = Guarda los resúmenes en la base de datos de Looker.
- `[fecha_reproceso]` *(Opcional)*: Permite correr el proceso en el pasado en formato `YYYY-MM-DD`. Si se omite, se ejecuta con la fecha de hoy.

### Ejemplos Prácticos

1. **Ejecutar el proceso para el día actual en producción y guardar en Looker:**
   ```bash
   python main.py prod server
   ```

2. **Probar el proceso localmente (generando Excel) con la base de datos de desarrollo:**
   ```bash
   python main.py dev local
   ```

3. **Reprocesar una fecha específica de días pasados (ej. 25 de Junio 2024):**
   ```bash
   python main.py dev local "2024-06-25"
   ```

## 🧪 Pruebas Unitarias y Cobertura

El proyecto cuenta con una robusta suite de pruebas unitarias y reportes de cobertura para garantizar la fiabilidad del proceso ETL.

### Ejecución de Pruebas:

Para ejecutar todos los tests con reporte de cobertura en consola:

```bash
python -m pytest tests/ -v --cov=src --cov=config --cov=helper
```

Para generar un reporte detallado en HTML (se crea la carpeta `htmlcov/`):

```bash
python -m pytest tests/ -v --cov=src --cov=config --cov=helper --cov-report=html
```

### Cobertura de la Suite (98% Total):

- **Config & DB (100%):** Pruebas completas de conexión (MySQL y SQLAlchemy) y manejo de argumentos.
- **Extract (100%):** Simula la comunicación con la base de datos MySQL (Mocks).
- **Transform (~90%):** Valida la lógica de limpieza y consolidación de Pandas.
- **Load (100%):** Simula la escritura de archivos Excel y la carga en SQL.
- **Utils/Validators (100%):** Prueba el sistema de cuadre Ventas vs Pagos y notificaciones.

## 🤖 Guía para Agentes (agent.md)

Este repositorio incluye un archivo `agent.md` que define las reglas, estándares de codificación, arquitectura y protocolos de desarrollo que cualquier agente de IA debe seguir al interactuar con el código. Es la **fuente de verdad** para mantener la consistencia del proyecto.
