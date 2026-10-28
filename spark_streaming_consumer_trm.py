#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD)
Consumidor Spark Structured Streaming: lee el topic `trm_stream` de Kafka y calcula,
en tiempo real:
  1) Estadísticas por ventana de 30 segundos y por casa de cambio:
     número de eventos, precio promedio / mínimo / máximo y volumen total.
  2) Alertas: eventos con una variación mayor o igual a 0,25 % respecto al
     evento anterior de la misma casa.

Ejecución (primero el productor, luego este consumidor):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        spark_streaming_consumer_trm.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

TOPIC = "trm_stream"
BROKER = "localhost:9092"
UMBRAL_ALERTA_PCT = 0.25

ESQUEMA = StructType([
    StructField("casa", StringType()),
    StructField("precio", DoubleType()),
    StructField("variacion_pct", DoubleType()),
    StructField("volumen_usd", IntegerType()),
    StructField("timestamp", TimestampType()),   # llega como segundos epoch
])


def parsear(df_kafka):
    """Convierte el valor binario de Kafka (JSON) en columnas tipadas."""
    return (
        df_kafka.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("d"))
        .select("d.*")
        .dropna(subset=["casa", "precio", "timestamp"])
    )


def estadisticas_por_ventana(df):
    """Agregación por ventana de 30 s y casa. El watermark limita el estado en memoria."""
    return (
        df.withWatermark("timestamp", "1 minute")
        .groupBy(F.window("timestamp", "30 seconds"), "casa")
        .agg(
            F.count("*").alias("eventos"),
            F.round(F.avg("precio"), 2).alias("precio_prom"),
            F.round(F.min("precio"), 2).alias("precio_min"),
            F.round(F.max("precio"), 2).alias("precio_max"),
            F.sum("volumen_usd").alias("volumen_usd"),
        )
        .select(
            F.col("window.start").alias("inicio"),
            F.col("window.end").alias("fin"),
            "casa", "eventos", "precio_prom", "precio_min", "precio_max", "volumen_usd",
        )
    )


def alertas(df, umbral=UMBRAL_ALERTA_PCT):
    """Eventos individuales con una variación fuerte."""
    return (
        df.filter(F.abs(F.col("variacion_pct")) >= umbral)
        .select("timestamp", "casa", "precio", "variacion_pct")
    )


def main():
    spark = SparkSession.builder.appName("TRM_Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    eventos = parsear(crudo)

    consulta_stats = (
        estadisticas_por_ventana(eventos)
        .writeStream.queryName("estadisticas_30s")
        .outputMode("update")           # solo imprime las ventanas que cambian
        .format("console")
        .option("truncate", "false")
        .option("numRows", 30)
        .start()
    )

    consulta_alertas = (
        alertas(eventos)
        .writeStream.queryName("alertas")
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    print("Streaming iniciado. Spark UI: http://<IP_VM>:4040  (pestaña Structured Streaming)")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
