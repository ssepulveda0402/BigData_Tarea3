# Tarea 3 – Procesamiento de datos con Apache Spark (TRM de Colombia)

Proyecto del curso **Big Data (código 202016911)** de la UNAD.
Autor: Sebastián Felipe Sepúlveda Gallo.

## ¿Qué problema resuelve?

La Tasa Representativa del Mercado (TRM) es el valor de referencia de un dólar en pesos colombianos.
Con la serie histórica publicada en datos abiertos se responden preguntas como:

- ¿Cómo ha evolucionado la TRM desde 1991?
- ¿En qué años fue más volátil?
- ¿Cuáles han sido los días de mayores alzas y caídas?

El proyecto tiene dos partes:

| Parte | Archivo | Qué hace |
|-------|---------|----------|
| Batch | `trm_batch.py` | Lee el histórico de la TRM desde HDFS, lo limpia, lo analiza (DataFrames y RDDs) y genera gráficas. |
| Tiempo real | `kafka_producer_trm.py` y `spark_streaming_consumer_trm.py` | Un generador envía cotizaciones simuladas a Kafka y Spark Structured Streaming calcula estadísticas por ventana de tiempo y alertas. |

## Datos

- **Fuente:** [Datos Abiertos Colombia – Tasa de Cambio Representativa del Mercado (TRM) – Histórico](https://www.datos.gov.co/browse) (identificador del dataset: `mcec-87by`).
- **Descarga directa del CSV:** `https://www.datos.gov.co/api/views/mcec-87by/rows.csv`
- Columnas esperadas: `VALOR`, `UNIDAD`, `VIGENCIADESDE`, `VIGENCIAHASTA`.
- Los datos del modo "tiempo real" **son simulados**: no son cotizaciones reales, sirven para demostrar la arquitectura.

## Requisitos

- Máquina virtual Ubuntu con Hadoop y Spark instalados (Anexos 1 y 2 de la guía).
- Apache Kafka 3.9.x y ZooKeeper (Anexo 3).
- Python 3 con las librerías de `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Parte 1 – Procesamiento en batch

1. Descargar el dataset y subirlo a HDFS (con el clúster Hadoop encendido):

```bash
wget https://www.datos.gov.co/api/views/mcec-87by/rows.csv
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put rows.csv /Tarea3
hdfs dfs -ls /Tarea3
```

2. Ejecutar el análisis:

```bash
python3 trm_batch.py
```

Por defecto lee `hdfs://localhost:9000/Tarea3/rows.csv`. También se puede indicar otra ruta y la carpeta de salida:

```bash
python3 trm_batch.py ruta/al/archivo.csv carpeta_salida
```

### Qué hace el script

1. **Carga** el CSV con `inferSchema` y muestra el esquema.
2. **Limpieza:** convierte el valor a número y la fecha a tipo fecha, elimina nulos, valores no positivos y fechas duplicadas.
3. **Transformación:** calcula año, mes, variación diaria en pesos y en porcentaje (función de ventana `lag`).
4. **EDA con DataFrames:** estadísticas descriptivas, resumen por año, días de TRM más alta, mayores alzas y caídas, años más volátiles.
5. **EDA con RDDs:** `map`, `filter`, `reduceByKey`, `count`, `take` y `reduce`.
6. **Visualización:** guarda tres gráficas PNG y un resumen anual en CSV dentro de la carpeta `salida/`.

## Parte 2 – Tiempo real con Kafka y Spark Streaming

Cada comando va en su propia terminal.

```bash
# 1. ZooKeeper y Kafka
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &

# 2. Crear el topic (3 particiones, 1 réplica porque hay un solo broker)
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
    --replication-factor 1 --partitions 3 --topic trm_stream

# 3. Generador de datos (terminal A)
python3 kafka_producer_trm.py

# 4. Consumidor Spark Streaming (terminal B, con el productor ya corriendo)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    spark_streaming_consumer_trm.py
```

El consumidor imprime en consola:

- **Estadísticas por ventana de 30 segundos y por casa de cambio:** número de eventos, precio promedio, mínimo, máximo y volumen.
- **Alertas:** eventos con una variación de 0,25 % o más frente al evento anterior de la misma casa.

La interfaz web de Spark (`http://IP_DE_LA_VM:4040`, pestaña *Structured Streaming*) muestra los jobs y el ritmo de procesamiento.

> Si el comando `spark-submit` falla por versión del paquete de Kafka, la versión del paquete debe coincidir con la de Spark instalada (`spark-submit --version`).

## Estructura del repositorio

```
.
├── README.md
├── requirements.txt
├── trm_batch.py
├── kafka_producer_trm.py
└── spark_streaming_consumer_trm.py
```
