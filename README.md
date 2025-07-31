# sales-management-load-sales

proceso que descarga las ventas y pagos de los locales de sales management por local

Argumento 2 prod o dev, depende del ambiente, para pruebas se usa dev y prod para las ventas reales

Argumento 3 local o server, con local se ejecutara el etl y dejara un archivo excel en la ruta "../locals sales", con server guardara las ventas en la base de datos de railway

Argumento 4 solo se usara para reprocesar el dia colocado

ejemplo de procesar el dia actual
python main.py prod server

ejemplo de reproceso
python main.py dev local "2024-06-25"