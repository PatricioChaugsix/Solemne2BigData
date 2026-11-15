from pyspark.sql.functions import avg, col, count , hour , max, min, stddev #Importacion de funciones matematicas de Spark 
from spark_prep import cargar_y_limpiar_datos, iniciar_spark #Importacion de archivo de Kamila

def ejecutar_eda(ruta_archivo = "eventos_streaming.jsonl"):
    spark = iniciar_spark() 
    df_limpio = cargar_y_limpiar_datos(spark, ruta_archivo) #Carga del DataSet limpio 
    
    #1. Tiempo promedio de reproduccion por sesion 
    df_descriptiva = df_limpio.select(  #Se escogen las columnas para analizarlas con las funciones matematicas de Spark, para dar lo que va en cada fila
        count("minutos_reproducidos").alias("total_sesiones_validas"), # El alias es para renombrar la columna de la tabla
        avg("minutos_reproducidos").alias("promedio_minutos"),
        stddev("minutos_reproducidos").alias("desciacion_estandar_minutos"),
        min("minutos_reproducidos").alias("minimo_minutos"),
        max("minutos_reproducidos").alias("maximo_minutos"),
    )
    df_descriptiva.show() #El show es para mostrar la tabla completa que se hizo, lo procesa
    
    #tipo de contenido que tiene mayor tiempo PROMEDIO de reproduccion 
    df_categorias =(
       df_limpio.groupBy("tipo_contenido") #Agrupa en categorias de contenido
       .agg( count("*").alias("total_reproducciones_horas"), avg("minutos_reproducidos").alias("promedio_minutos"),).orderBy(
           col("promedio_minutos").desc() #resultado de mayor a menor 
       ) # El agg permite hacer las funciones dentro del groupBy
    )
    df_categorias.show() 
  
    #que tanto varia la cantidad de reproducciones en un dia por hora
    df_temporal = (
        df_limpio.withColumn("hora",hour(col("timestamp"))).groupBy("hora").agg( #creamos un nuevo DF y agregamos una columna que es la hora de 
            count("*").alias("cantidad_reproducciones"), avg("minutos_reproducidos").alias("promedio_minutos_hora"),
        ).orderBy("hora")
    )
    df_temporal.show(24, truncate=False) #El truncate es para que no corte la tabla y se vea completa. el 24 es para que muestre las 24 horas
    
    pdf_minutos = df_limpio.select("minutos_reproducidos").toPandas() #hace que cada uno se pueda usar despues en un grafico con pasandolo a pandas
    pdf_categorias = df_categorias.toPandas() #pasa de DF de Spark a DF de Pandas para poder graficarlos
    pdf_temporal = df_temporal.toPandas()
    
    return spark, pdf_minutos, pdf_categorias, pdf_temporal
    
if __name__ == "__main__": #hace que se ejecue solo en la terminal
    spark,pdf_minutos, pdf_categorias, pdf_temporal = ejecutar_eda()
    spark.stop() #se detiene el proceso limpiando la RAM