from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

spark = (SparkSession.builder.appName("SensorStreaming")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Esquema del JSON que envia el productor
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", StringType()),
])

# Lectura del topic sensor_data de Kafka
crudo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "sensor_data")
         .option("startingOffsets", "latest")
         .load())

# Conversion del JSON a columnas
datos = (crudo.select(from_json(col("value").cast("string"), schema).alias("d"))
         .select("d.*")
         .withColumn("ts", to_timestamp("timestamp")))

# Promedio de temperatura y humedad por sensor en ventanas de 1 minuto
promedios = (datos.groupBy(window("ts", "1 minute"), "sensor_id")
             .agg(avg("temperature").alias("temp_promedio"),
                  avg("humidity").alias("hum_promedio")))

consulta = (promedios.writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .start())
consulta.awaitTermination()
