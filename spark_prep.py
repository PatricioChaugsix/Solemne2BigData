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

def cargar_y_limpiar_datos(spark, ruta_archivo="eventos_streaming.jsonl"):
    """
    Carga el archivo JSON Lines, transforma los tipos de datos y filtra registros raros.
    Retorna el DataFrame limpio listo para el Análisis Exploratorio (EDA).
    """
    print("\n" + "="*70)
    print(" 1. CARGA DE DATOS DESDE ARCHIVO JSON LINES (.jsonl)")
    print("="*70)

    # se valida que el archivo generado por consumidor.py exista en el directorio que tengamos
    if not os.path.exists(ruta_archivo):
        raise FileNotFoundError(f"Error: No se encontró el archivo '{ruta_archivo}'.")

    # lee el archivo .jsonl usando el lector nativo
    df_crudo = spark.read.json(ruta_archivo)