import os
import sys
from pyspark.sql import SparkSession, functions as F

# Uso: spark-submit batch_hurto.py archivo.csv
ruta = sys.argv[1]
spark = (SparkSession.builder.appName("HurtoPersonasBatch")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Carga del CSV como DataFrame
df = spark.read.option("header", True).csv(ruta)
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))
print("Registros cargados:", df.count())
df.printSchema()

# 2. Limpieza y transformacion
df = (df.withColumn("cantidad", F.col("cantidad").cast("int"))
        .withColumn("departamento", F.upper(F.trim("departamento")))
        .withColumn("municipio", F.upper(F.trim("municipio")))
        .withColumn("fecha", F.coalesce(
            F.to_date("fecha_hecho", "MM/dd/yyyy"),
            F.to_date("fecha_hecho", "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            F.to_date("fecha_hecho", "yyyy-MM-dd"))))
antes = df.count()
df = df.dropna(subset=["fecha", "departamento", "cantidad"])
df = (df.withColumn("anio", F.year("fecha"))
        .withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
        .withColumn("dia_num", F.dayofweek("fecha")))
print("Registros tras limpiar:", df.count(), "(antes:", antes, ")")

# 3. Analisis exploratorio
dias = {1: "Domingo", 2: "Lunes", 3: "Martes", 4: "Miercoles",
        5: "Jueves", 6: "Viernes", 7: "Sabado"}
mapa = F.create_map(*[x for k, v in dias.items() for x in (F.lit(k), F.lit(v))])
suma = F.sum("cantidad").alias("victimas")

print("Total de victimas:", df.agg(F.sum("cantidad")).first()[0])
print("\n== Top 10 departamentos ==")
por_depto = df.groupBy("departamento").agg(suma).orderBy(F.desc("victimas"))
por_depto.show(10, False)
print("== Top 10 municipios ==")
(df.groupBy("departamento", "municipio").agg(suma)
   .orderBy(F.desc("victimas")).show(10, False))
print("== Victimas por anio ==")
por_anio = df.groupBy("anio").agg(suma).orderBy("anio")
por_anio.show(50)
print("== Serie mensual (ultimos 24 meses) ==")
mensual = df.groupBy("anio_mes").agg(suma).orderBy("anio_mes")
mensual.orderBy(F.desc("anio_mes")).show(24)
print("== Dia de la semana ==")
por_dia = (df.groupBy("dia_num").agg(suma)
             .withColumn("dia", mapa[F.col("dia_num")]).orderBy("dia_num"))
por_dia.select("dia", "victimas").show()

# 4. Graficos (opcional: si falta matplotlib o pandas, el script sigue)
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    os.makedirs("graficos", exist_ok=True)
    d = por_depto.limit(10).toPandas()
    plt.figure(figsize=(9, 5))
    plt.barh(d["departamento"][::-1], d["victimas"][::-1])
    plt.title("Victimas de hurto a personas por departamento (top 10)")
    plt.tight_layout()
    plt.savefig("graficos/departamentos.png")
    plt.close()
    m = mensual.toPandas()
    plt.figure(figsize=(10, 4))
    plt.plot(m["anio_mes"], m["victimas"])
    plt.xticks(m["anio_mes"][::12], rotation=45)
    plt.title("Serie mensual de victimas")
    plt.tight_layout()
    plt.savefig("graficos/serie_mensual.png")
    plt.close()
    p = por_dia.toPandas()
    plt.figure(figsize=(8, 4))
    plt.bar(p["dia"], p["victimas"])
    plt.title("Victimas por dia de la semana")
    plt.tight_layout()
    plt.savefig("graficos/dia_semana.png")
    plt.close()
    print("Graficos guardados en la carpeta graficos/")
except Exception as e:
    print("Sin graficos:", e)

spark.stop()
