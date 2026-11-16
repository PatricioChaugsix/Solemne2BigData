Solemne 02 - Generación, ingesta y análisis de eventos con Python, Kafka y Spark

INTRODUCCIÓN
Este proyecto implementa un flujo de generación, ingesta y análisis de eventos de consumo de una plataforma de streaming, utilizando:
- Python
- Apache Kafka
- Apache Spark

Se generan eventos que representan reproducciones de películas, series y documentales desde distintos dispositivos. Estos eventos se envían desde un productor hacia Kafka, para luego ser recibidos por un consumidor y finalmente ser almacenados en un archivo JSON Lines, dejando los datos listos para ser procesados y analizados con PySpark.

Las tecnologías que utilizamos para llevar a cabo la solemne fueron:

    • Python: Para la generación, envío y recepción de eventos.
    • Apache Kafka 3.8.0: Para transmitir los eventos en tiempo real.
    • Docker Compose: Para desplegar Kafka.
    • Apache Spark (PySpark): Para procesar y analizar los datos.
    • Pandas: Para convertir y manipular los resultados.
    • Matplotlib: Generación de visualizaciones.
    • kafka-python: Se usó para comunicar Python con Kafka.

La estructura del proyecto es:

Solemne2BigData
|
|-- productor.py
|-- consumidor.py
|-- verificador.py
|-- spark_prep.py
|-- spark_eda.py
|-- visualizaciones.py
|-- docker-compose.yml
|-- requirements.txt
|-- eventos_streaming.jsonl
|-- grafico_descriptivo.png
|-- grafico_categorias.png
|-- grafico_temporal.png
|__ README.md

Antes de ejecutar el proyecto se debe tener instalado:
    • Python 3.11.
    • Docker Desktop con Docker Compose.
    • Java compatible con la versión de PySpark utilizada.

También es necesario instalar algunas dependencias en Python:
    • pip install -r requirements.txt

CONFIGURACIÓN E INICIO DE KAFKA
Para la solemne usamos Docker Compose para iniciar Apache Kafka en modo KRaft, por lo que no es necesario usar ZooKeeper. Desde la carpeta principal del repositorio, se deben ejecutar los siguientes comandos:

    • docker compose up -d : Para iniciar Kafka y permitir la conexión mediante localhost:9092
    • docker compose ps : Para comprobar que el contenedor está levantado y funcionando
    • docker compose down : Para detener Kafka

ORDEN PARA EJECUTAR EL PROYECTO

    1. Iniciar el consumidor
    Primero hay que abrir una terminal CMD y ejecutar:
    • python consumidor.py

    Esto hará que el consumidor se conecte al tópico que creamos "topic_streaming" y comenzará a recibir los eventos enviados por el productor, los datos recibidos se almacenan en "eventos_streaming.jsonl". La ventaja de iniciar el consumidor antes que el productor, es que de esta manera nos aseguramos que el consumidor reciba todos los eventos desde el primero que se genere, evitando que se pierdan mensajes por no tener el consumidor activo.
    PD: el consumidor permanece activo hasta que se presione Ctrl + C

    2. Ejecutar el productor
    Se debe abrir otra terminal CMD (sin cerrar la del consumidor) y ejecutar:
    • python productor.py 5000

    De esta manera el productor genera al menos 5000 eventos, los cuales serán enviados al tópico "topic_streaming". La cantidad final puede ser mayor debido a la generación de eventos adicionales de Smart TV durante el horario nocturno.
    Los eventos generados contienen los siguientes atributos:

    • id_reproduccion: identificador único de reproducción.
    • timestamp: fecha y hora del evento.
    • tipo_contenido: película, serie o documental.
    • minutos_reproducidos: duración de la reproducción.
    • dispositivo: Smart TV, celular o PC.

    Además se incorporan anomalías en el 5% de los registros, reemplazando aleatoriamente los minutos reproducidos por valores nulos o iguales a cero.

    3. Detener el consumidor
    Cuando el productor termine de enviar eventos se debe presionar Ctrl + C, haciendo que el consumidor deje de consumir los mensajes del tópico, y se genere un resumen de los eventos guardados

    4. Verificar los mensajes de Kafka
    Para comprobar la cantidad de mensajes disponibles en el tópico, ejecutar:
    • python verificador.py
    De esta manera se consulta al tópico, y se muestra la cantidad de mensajes que el verificador pudo leer.

    5. Procesamiento y limpieza de datos
    El archivo "spark_prep.py" carga los datos desde "eventos_streaming.jsonl" usando PySpark. Durante este proceso se realizan las siguientes operaciones:
        • Lectura del archivo JSON Lines.
        • Inferencia y visualización del esquema de datos.
        • Conversión del atributo timestamp al tipo TimestampType.
        • Identificación de registros anómalos.
        • Eliminación de registros con minutos nulos o menores o iguales a cero
        • Comparación de la cantidad de registros antes y después de la limpieza.

    6. Análisis exploratorio de datos
    El archivo "spark_eda.py" realiza el análisis exploratorio sobre los datos ya limpios.
    Se calculan estadísticas descriptivas, promedios por tipo de contenido y cantidad de reproducciones por hora del día. Para ejecutar el análisis exploratorio de datos se debe ejecutar:
    • python spark_eda.py 

    7. Generación de gráficos
    Por último, al ejecutar: 
    • python visualizaciones.py
    Se utilizarán los resultados del análisis exploratorio para generar tres gráficos:
        • grafico_descriptivo.png: Muestra la distribución de los minutos reproducidos.
        • grafico_categorias.png: Muestra el promedio de minutos reproducidos por tipo de contenido.
        • grafico_temporal.png: Muestra la cantidad de reproducciones por hora del día.

    Los gráficos se guardan en la carpeta principal del proyecto.


    DEMO (MUESTRA DE 150 EVENTOS)
    La demo se guarda en un archivo aparte (eventos_demo.jsonl) para no modificar el archivo completo. Antes de grabar se reinicia Kafka para que el tópico parta vacío:
    • docker compose down
    • docker compose up -d

    Luego, en el mismo orden de siempre:
    • python consumidor.py eventos_demo.jsonl  (terminal 1, detener con Ctrl + C al terminar)
    • python productor.py 150  (terminal 2)
    • python spark_prep.py eventos_demo.jsonl
    • python spark_eda.py eventos_demo.jsonl
    • python visualizaciones.py eventos_demo.jsonl  (genera demo_grafico_*.png)

    Importante: no ejecutar "python consumidor.py" sin nombre de archivo durante la demo, porque sobrescribe eventos_streaming.jsonl.


    RESULTADOS ESPERADOS
    El procesamiento permite identificar patrones de consumo, comparar los tiempos de reproducción entre categorías y observar aquellos horarios con mayor actividad. También nos permite evaluar la calidad de los datos mediante la detección y eliminación de registros anómalos.

    CONSIDERACIONES FINALES
    • El consumidor siempre debe iniciarse antes que el productor.
    • Ejecutar nuevamente el consumidor sobrescribe el archivo JSONL.
    • Kafka debe estar activo al momento de ejecutar al productor y al consumidor.
    • El análisis de Spark y la generación de gráficos trabajan con el archivo JSONL, por lo que no se necesita tener Kafka activo.