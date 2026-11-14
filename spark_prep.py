# ==============================================================================
# DESCRIPCIÓN: Este archivo lo que hace es leer eventos_streaming.jsonl, valida 
#              el esquema, convierte tipos, trata anomalías (nulos o ceros)
#              y compara el conteo antes/después.
# ==============================================================================
def iniciar_spark():
    """
    Inicializa y configura la sesión de Apache Spark en modo local.
    """
    # se crea la SparkSession con un nombre descriptivo para la interfaz web de Spark
    spark = SparkSession.builder \
        .appName("Solemne2_BigData_Preparacion_Datos") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "America/Santiago") \
        .getOrCreate()
    
    # Reducimos el nivel de logs en consola (evita exceso de mensajes informacioon de Spark)
    spark.sparkContext.setLogLevel("ERROR")
    return spark
