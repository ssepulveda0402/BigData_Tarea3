#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD)
Procesamiento en batch con Apache Spark: Tasa Representativa del Mercado (TRM)

Problema: ¿Cómo ha evolucionado la TRM (pesos colombianos por dólar) desde 1991,
en qué años fue más volátil y cuáles fueron los días de mayores saltos?

Flujo:
  1. Carga del CSV desde HDFS (fuente original: datos.gov.co, dataset mcec-87by)
  2. Limpieza y transformación (DataFrame)
  3. Análisis exploratorio (DataFrame + RDD)
  4. Visualización (gráficas PNG + resumen en consola)

Uso (en la máquina virtual, con el CSV ya cargado en HDFS):
    python3 trm_batch.py
    python3 trm_batch.py hdfs://localhost:9000/Tarea3/rows.csv salida
    python3 trm_batch.py ruta/local/rows.csv salida      # prueba local sin HDFS
"""
import os
import sys

from pyspark.sql import SparkSession, Window, functions as F

# --------------------------------------------------------------------------
# Parámetros
# --------------------------------------------------------------------------
RUTA_CSV = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/Tarea3/rows.csv"
DIR_SALIDA = sys.argv[2] if len(sys.argv) > 2 else "salida"
os.makedirs(DIR_SALIDA, exist_ok=True)

spark = (
    SparkSession.builder.appName("Tarea3_TRM_Batch")
    # Evita errores de interpretación al probar varios formatos de fecha
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def titulo(texto):
    print("\n" + "=" * 70)
    print(texto)
    print("=" * 70)


# --------------------------------------------------------------------------
# 1. CARGA
# --------------------------------------------------------------------------
titulo("1. CARGA DEL DATASET")
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(RUTA_CSV)
)
print(f"Ruta: {RUTA_CSV}")
df_raw.printSchema()
df_raw.show(5, truncate=False)
total_inicial = df_raw.count()
print(f"Registros leídos: {total_inicial}")


# --------------------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACIÓN
# --------------------------------------------------------------------------
titulo("2. LIMPIEZA Y TRANSFORMACIÓN")


def buscar_columna(df, fragmento):
    """Devuelve el nombre real de la primera columna que contiene `fragmento`
    (sin importar mayúsculas, tildes simples ni espacios/guiones bajos)."""
    objetivo = fragmento.upper().replace("_", "").replace(" ", "")
    for nombre in df.columns:
        if objetivo in nombre.upper().replace("_", "").replace(" ", ""):
            return nombre
    raise ValueError(
        f"No se encontró una columna parecida a '{fragmento}'. "
        f"Columnas disponibles: {df.columns}"
    )


col_valor = buscar_columna(df_raw, "VALOR")
col_fecha = buscar_columna(df_raw, "VIGENCIADESDE")
print(f"Columna de valor : {col_valor}")
print(f"Columna de fecha : {col_fecha}")

# El valor puede venir como texto con separadores: se deja solo dígitos, '.' y '-'
valor_limpio = F.regexp_replace(F.col(col_valor).cast("string"), r"[^0-9.\-]", "").cast("double")

# En este dataset la fecha viene como dd/MM/yyyy (ej. 27/10/2026). Ese formato va
# PRIMERO para no confundir día y mes en fechas ambiguas como 03/04/2026.
# Los demás formatos quedan como respaldo por si la fuente cambia.
fecha_limpia = F.coalesce(
    F.to_date(F.col(col_fecha).cast("string"), "dd/MM/yyyy"),
    F.to_date(F.col(col_fecha).cast("string"), "yyyy-MM-dd"),
    F.to_date(F.col(col_fecha).cast("string"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
    F.to_date(F.col(col_fecha).cast("string"), "MM/dd/yyyy"),
)

df = (
    df_raw.select(valor_limpio.alias("trm"), fecha_limpia.alias("fecha"))
    .dropna(subset=["trm", "fecha"])   # sin valor o sin fecha válida
    .filter(F.col("trm") > 0)          # una TRM no puede ser cero ni negativa
    .dropDuplicates(["fecha"])         # una sola TRM por fecha de vigencia
)

# Columnas derivadas: año, mes y variación diaria (ventana ordenada por fecha)
ventana = Window.orderBy("fecha")
df = (
    df.withColumn("anio", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("trm_anterior", F.lag("trm").over(ventana))
    .withColumn("variacion", F.col("trm") - F.col("trm_anterior"))
    .withColumn("variacion_pct", F.round(F.col("variacion") / F.col("trm_anterior") * 100, 3))
    .cache()
)

total_limpio = df.count()
print(f"Registros tras la limpieza: {total_limpio} "
      f"(descartados: {total_inicial - total_limpio})")
df.orderBy("fecha").show(5)


# --------------------------------------------------------------------------
# 3. ANÁLISIS EXPLORATORIO (DataFrame)
# --------------------------------------------------------------------------
titulo("3A. EDA CON DATAFRAMES")

print("Estadísticas descriptivas de la TRM:")
df.select("trm").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

rango = df.agg(F.min("fecha").alias("desde"), F.max("fecha").alias("hasta")).first()
print(f"Periodo cubierto: {rango['desde']}  ->  {rango['hasta']}")

print("TRM por año (promedio, mínimo, máximo y desviación estándar):")
por_anio = (
    df.groupBy("anio")
    .agg(
        F.round(F.avg("trm"), 2).alias("promedio"),
        F.round(F.min("trm"), 2).alias("minimo"),
        F.round(F.max("trm"), 2).alias("maximo"),
        F.round(F.stddev("trm"), 2).alias("desv_std"),
        F.count("*").alias("registros"),
    )
    .orderBy("anio")
)
por_anio.show(50, truncate=False)

print("Top 10 días con la TRM más alta:")
df.select("fecha", "trm").orderBy(F.desc("trm")).show(10)

print("Top 10 mayores alzas diarias (en pesos):")
(
    df.filter(F.col("variacion").isNotNull())  # el primer día no tiene día anterior
    .select("fecha", "trm", "variacion", "variacion_pct")
    .orderBy(F.desc("variacion"))
    .show(10)
)

print("Top 10 mayores caídas diarias (en pesos):")
(
    df.filter(F.col("variacion").isNotNull())
    .select("fecha", "trm", "variacion", "variacion_pct")
    .orderBy(F.asc("variacion"))
    .show(10)
)

print("Años más volátiles (mayor desviación estándar de la variación diaria):")
(
    df.groupBy("anio")
    .agg(F.round(F.stddev("variacion"), 2).alias("volatilidad_diaria"))
    .orderBy(F.desc("volatilidad_diaria"))
    .show(5)
)


# --------------------------------------------------------------------------
# 3B. EDA CON RDDs (map, filter, reduceByKey, take, count...)
# --------------------------------------------------------------------------
titulo("3B. ANÁLISIS CON RDDs")

# Cada elemento del RDD: (año, trm)
rdd = df.select("anio", "trm").rdd.map(lambda fila: (fila["anio"], fila["trm"]))
print(f"Particiones del RDD: {rdd.getNumPartitions()}")
print(f"Total de elementos (count): {rdd.count()}")
print(f"Primeros 3 elementos (take): {rdd.take(3)}")

# Transformación: filter -> solo registros de los últimos 10 años del dataset
anio_max = rdd.map(lambda t: t[0]).max()
rdd_reciente = rdd.filter(lambda t: t[0] > anio_max - 10)
print(f"Registros de los últimos 10 años: {rdd_reciente.count()}")

# map + reduceByKey: promedio por año con el patrón (suma, cantidad)
promedio_rdd = (
    rdd.map(lambda t: (t[0], (t[1], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda s: round(s[0] / s[1], 2))
    .sortByKey()
)
print("Promedio de TRM por año calculado con RDDs (últimos 5 años):")
for anio, prom in promedio_rdd.collect()[-5:]:
    print(f"  {anio}: {prom}")

# reduce (acción): TRM máxima de todo el dataset
trm_max = rdd.map(lambda t: t[1]).reduce(lambda a, b: a if a > b else b)
print(f"TRM máxima histórica del dataset (reduce): {trm_max}")


# --------------------------------------------------------------------------
# 4. VISUALIZACIÓN
# --------------------------------------------------------------------------
titulo("4. VISUALIZACIÓN DE RESULTADOS")

import matplotlib
matplotlib.use("Agg")  # sin ventana: se guardan imágenes PNG (VM por SSH)
import matplotlib.pyplot as plt

# Los resultados agregados son pequeños: es seguro traerlos al driver
serie = df.select("fecha", "trm").orderBy("fecha").collect()
fechas = [r["fecha"] for r in serie]
valores = [r["trm"] for r in serie]

anuales = por_anio.collect()
anios = [r["anio"] for r in anuales]
promedios = [r["promedio"] for r in anuales]

variaciones_pct = [
    r["variacion_pct"] for r in df.select("variacion_pct").dropna().collect()
]

# Gráfica 1: serie de tiempo
plt.figure(figsize=(11, 5))
plt.plot(fechas, valores, linewidth=0.9, color="#1f77b4")
plt.title("Evolución de la TRM (COP por USD)")
plt.xlabel("Fecha")
plt.ylabel("Pesos colombianos por dólar")
plt.grid(alpha=0.3)
plt.tight_layout()
ruta1 = os.path.join(DIR_SALIDA, "trm_serie_tiempo.png")
plt.savefig(ruta1, dpi=130)
plt.close()

# Gráfica 2: promedio anual
plt.figure(figsize=(11, 5))
plt.bar([str(a) for a in anios], promedios, color="#ff7f0e")
plt.title("TRM promedio por año")
plt.xlabel("Año")
plt.ylabel("Pesos colombianos por dólar")
plt.xticks(rotation=90, fontsize=7)
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
ruta2 = os.path.join(DIR_SALIDA, "trm_promedio_anual.png")
plt.savefig(ruta2, dpi=130)
plt.close()

# Gráfica 3: distribución de la variación diaria porcentual
plt.figure(figsize=(9, 5))
plt.hist(variaciones_pct, bins=80, range=(-6, 6), color="#2ca02c", edgecolor="white")
plt.title("Distribución de la variación diaria de la TRM (%)")
plt.xlabel("Variación diaria (%) - se recorta entre -6% y 6% para legibilidad")
plt.ylabel("Número de días")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
ruta3 = os.path.join(DIR_SALIDA, "trm_variacion_diaria.png")
plt.savefig(ruta3, dpi=130)
plt.close()

print("Gráficas guardadas:")
for r in (ruta1, ruta2, ruta3):
    print("  -", os.path.abspath(r))

# Resumen en texto (útil para mostrar en el video si no hay entorno gráfico)
print("\nPromedio anual (barras de texto, 1 '#' = 500 pesos):")
for anio, prom in zip(anios[-12:], promedios[-12:]):
    print(f"  {anio} | {'#' * int(prom // 500):<12} {prom:,.2f}")

# Exportar el resultado agregado a CSV (un solo archivo) para el repositorio
(
    por_anio.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(os.path.join(DIR_SALIDA, "resumen_anual_csv"))
)
print(f"\nResumen anual exportado a: {os.path.abspath(os.path.join(DIR_SALIDA, 'resumen_anual_csv'))}")

titulo("PROCESAMIENTO BATCH FINALIZADO")
spark.stop()
