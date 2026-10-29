"""
Consumidor de Kafka mediante Spark Structured Streaming.
Ejecutar con spark-submit incluyendo el conector Kafka compatible con la versión
de Spark instalada. Consultar README.md: el artefacto debe coincidir con Spark/Scala.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = (SparkSession.builder
         .appName("StreamingSensoresKafka")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("id", IntegerType(), True),
    StructField("sensor", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("unidad", StringType(), True),
    StructField("tiempo", StringType(), True),
    StructField("ubicacion", StringType(), True)
])

flujo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "sensor_data")
         .option("startingOffsets", "latest")
         .load())

lecturas = (flujo.selectExpr("CAST(value AS STRING) AS json")
            .select(from_json(col("json"), esquema).alias("dato"))
            .select("dato.*")
            .filter(col("valor").isNotNull()))

# Conteo y promedio por sensor en cada micro-lote de streaming
resumen = lecturas.groupBy("sensor").agg(
    count("*").alias("eventos_recibidos"),
    avg("valor").alias("promedio_valor")
)

consulta = (resumen.writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", False)
            .start())

consulta.awaitTermination()
