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

    # --------------------------------------------------------------------------
    # tarea 1: Mostrar el esquema  y verificar tipos de datos
    # --------------------------------------------------------------------------
    print("\n--- Esquema Original Inferido por PySpark ---")
    df_crudo.printSchema()

    # se visualiza una pequeña muestra de 5 filas para verificar visualmente los datos
    print("--- Muestra de los primeros 5 registros crudos ---")
    df_crudo.show(5, truncate=False) 

    # --------------------------------------------------------------------------
    # tarea 2: Convertir timestamp (string) a TimestampType
    # --------------------------------------------------------------------------
    print("\n" + "="*70)
    print(" 2. CONVERSIÓN DE TIPOS DE DATOS (STRING -> TIMESTAMP)")
    print("="*70)

    # Convertimos la columna 'timestamp' desd e String a formato fecha-hora real (TimestampType)
    # Nota: to_timestamp soporta automáticamente el formato estándar ISO-8601 (YYYY-MM-DD HH:MM:SS)
    df_tipado = df_crudo.withColumn(
        "timestamp", 
        to_timestamp(col("timestamp"))
    )

    # Verificamos en el esquema que 'timestamp' ahora figura como 'timestamp' (TimestampType)
    print("\n--- Esquema Actualizado tras conversión de Timestamp ---")
    df_tipado.printSchema()

    # --------------------------------------------------------------------------
    # tarea 3: Conteo ANTES de limpiar y diagnóstico de anomalías
    # --------------------------------------------------------------------------
    print("\n" + "="*70)
    print(" 3. TRATAMIENTO DE CALIDAD DE DATOS (ANOMALÍAS EN MINUTOS)")
    print("="*70)

    # cuenta el total de registros originales recibidos desde kKafka
    conteo_antes = df_tipado.count()

    # calcula cuántos registros tienen minutos_reproducidos nulo o igual a 0
    registros_anomalos = df_tipado.filter(
        col("minutos_reproducidos").isNull() | (col("minutos_reproducidos") <= 0)
    ).count()

    print(f"Total de registros ANTES de la limpieza : {conteo_antes}")
    print(f"Registros anómalos detectados (Null/<=0): {registros_anomalos}")

    # --------------------------------------------------------------------------
    # REQUISITO 4: Filtrar/Eliminar registros con minutos_reproducidos nulo o 0
    # --------------------------------------------------------------------------
    # se eliminan los registros con minutos_reproducidos NULL o <= 0 porque representan
    # fallas del reproductor o clics accidentales sin reproducción efectiva.
    # Mantenerlos afectaria el promedio real de consumo y la desviación estándar.
    df_limpio = df_tipado.filter(
        col("minutos_reproducidos").isNotNull() & (col("minutos_reproducidos") > 0)
    )