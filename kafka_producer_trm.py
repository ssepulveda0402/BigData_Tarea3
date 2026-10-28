#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD)
Generador de datos (productor Kafka): simula cotizaciones del dólar en tiempo real.

Cada segundo envía al topic `trm_stream` un evento JSON como:
    {"casa": "CasaB", "precio": 4012.35, "variacion_pct": 0.031,
     "volumen_usd": 5400, "timestamp": 1790000000}

IMPORTANTE: los datos son SIMULADOS (caminata aleatoria alrededor de un valor
base). Sirven para demostrar la arquitectura Kafka + Spark Streaming; no son
cotizaciones reales.

Uso:
    python3 kafka_producer_trm.py                      # 1 evento por segundo
    python3 kafka_producer_trm.py --rate 5 --base 4000 # 5 eventos por segundo
Detener con Ctrl+C.
"""
import argparse
import json
import random
import time

from kafka import KafkaProducer

CASAS = ["CasaA", "CasaB", "CasaC", "CasaD", "CasaE"]


def crear_generador(base):
    """Devuelve una función que produce un evento nuevo en cada llamada."""
    ultimo = {casa: base for casa in CASAS}

    def generar():
        casa = random.choice(CASAS)
        previo = ultimo[casa]
        paso = random.gauss(0, 0.0005)            # variación normal: ~0.05 %
        if random.random() < 0.05:                # 5 % de eventos con salto fuerte
            paso += random.choice([-1, 1]) * 0.003
        nuevo = round(previo * (1 + paso), 2)
        ultimo[casa] = nuevo
        return {
            "casa": casa,
            "precio": nuevo,
            "variacion_pct": round((nuevo - previo) / previo * 100, 3),
            "volumen_usd": random.randint(500, 20000),
            "timestamp": int(time.time()),
        }

    return generar


def main():
    parser = argparse.ArgumentParser(description="Productor Kafka de TRM simulada")
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic", default="trm_stream")
    parser.add_argument("--base", type=float, default=4000.0,
                        help="valor inicial de referencia (ficticio)")
    parser.add_argument("--rate", type=float, default=1.0, help="eventos por segundo")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    generar = crear_generador(args.base)
    print(f"Enviando eventos al topic '{args.topic}' en {args.broker}. Ctrl+C para detener.")

    try:
        while True:
            evento = generar()
            # La clave = casa de cambio: los eventos de una misma casa van
            # siempre a la misma partición (se conserva su orden).
            producer.send(args.topic, key=evento["casa"], value=evento)
            print(f"Sent: {evento}")
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        print("\nDeteniendo productor...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
