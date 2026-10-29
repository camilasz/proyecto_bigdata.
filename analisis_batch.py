"""
Procesamiento por lotes con PySpark.
Lee datos/lecturas_sensores.csv, limpia valores inválidos y genera un resumen.
Ejecución desde la carpeta del proyecto:
    spark-submit codigo/analisis_batch.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min as min_, max as max_, count, when

spark = SparkSession.builder.appName("AnalisisBatchSensores").getOrCreate()

ruta = "datos/lecturas_sensores.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(ruta)

print("=== ESQUEMA ORIGINAL ===")
df.printSchema()
print("=== MUESTRA ORIGINAL ===")
df.show(10, truncate=False)

# Limpieza: convertir valor a número, eliminar nulos y valores físicamente improbables
df_limpio = (
    df.withColumn("valor", col("valor").cast("double"))
      .filter(col("valor").isNotNull())
      .filter(
          ((col("sensor") == "Temperatura") & col("valor").between(-20, 60)) |
          ((col("sensor") == "Humedad") & col("valor").between(0, 100)) |
          ((col("sensor") == "Presion") & col("valor").between(800, 1200))
      )
)

print("Registros originales:", df.count())
print("Registros después de limpieza:", df_limpio.count())
print("=== ESTADÍSTICAS POR SENSOR ===")
resumen = (df_limpio.groupBy("sensor", "unidad")
           .agg(count("*").alias("cantidad"),
                avg("valor").alias("promedio"),
                min_("valor").alias("minimo"),
                max_("valor").alias("maximo"))
           .orderBy("sensor"))
resumen.show(truncate=False)

# Resultado exportado para visualizarlo o incluirlo en el informe
resumen.coalesce(1).write.mode("overwrite").option("header", True).csv("resultados/resumen_sensores")
print("Resumen guardado en la carpeta resultados/resumen_sensores")
spark.stop()
