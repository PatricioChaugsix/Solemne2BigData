import json
import random
import sys
import uuid
from datetime import datetime, timedelta

from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC = "topic_streaming"
BOOTSTRAP_SERVERS = "localhost:9092"
CANTIDAD_MINIMA = 5000
PORCENTAJE_ANOMALIA = 0.05

TIPOS_CONTENIDO = ["Película", "Serie", "Documental"]
DISPOSITIVOS = ["Smart TV", "Celular", "PC"]

PESOS_CONTENIDO = [0.35, 0.45, 0.20]

DURACIONES = {
    "Serie": (40, 60),
    "Película": (90, 120),
    "Documental": (20, 80),
}

PESOS_HORA = [
    0.2, 0.1, 0.1, 0.1, 0.1, 0.2,
    0.5, 0.8, 1.0, 1.0, 1.0, 1.0,
    1.2, 1.2, 1.2, 1.3, 1.4, 1.6,
    1.8, 2.0, 2.5, 3.0, 2.8, 2.0,
]


def generar_timestamp(hora=None):
    ahora = datetime.now()
    if hora is None:
        hora = random.choices(range(24), weights=PESOS_HORA, k=1)[0]
    base = ahora - timedelta(days=random.randint(0, 6))
    timestamp = base.replace(
        hour=hora,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=0,
    )
    if timestamp > ahora:
        timestamp -= timedelta(days=1)
    return timestamp


def elegir_dispositivo(hora):
    if 20 <= hora <= 22:
        return random.choices(DISPOSITIVOS, weights=[0.60, 0.30, 0.10], k=1)[0]
    if 6 <= hora <= 19:
        return random.choices(DISPOSITIVOS, weights=[0.20, 0.55, 0.25], k=1)[0]
    return random.choices(DISPOSITIVOS, weights=[0.40, 0.45, 0.15], k=1)[0]


def generar_evento(dispositivo_forzado=None, hora_forzada=None):
    tipo = random.choices(TIPOS_CONTENIDO, weights=PESOS_CONTENIDO, k=1)[0]
    timestamp = generar_timestamp(hora_forzada)
    dispositivo = dispositivo_forzado or elegir_dispositivo(timestamp.hour)
    minimo, maximo = DURACIONES[tipo]
    return {
        "id_reproduccion": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat(),
        "tipo_contenido": tipo,
        "minutos_reproducidos": random.randint(minimo, maximo),
        "dispositivo": dispositivo,
    }


def construir_eventos(cantidad):
    eventos = []
    extras = 0
    tv_noche = 0
    dispositivos = {}
    for _ in range(cantidad):
        evento = generar_evento()
        eventos.append(evento)
        dispositivos[evento["dispositivo"]] = dispositivos.get(evento["dispositivo"], 0) + 1
        hora = datetime.fromisoformat(evento["timestamp"]).hour
        if evento["dispositivo"] == "Smart TV" and 20 <= hora <= 23:
            tv_noche += 1
            evento_extra = generar_evento(
                dispositivo_forzado="Smart TV",
                hora_forzada=random.randint(20, 23),
            )
            eventos.append(evento_extra)
            dispositivos["Smart TV"] = dispositivos.get("Smart TV", 0) + 1
            extras += 1
            tv_noche += 1
    return eventos, extras, tv_noche, dispositivos


def inyectar_anomalias(eventos):
    cantidad = max(1, round(len(eventos) * PORCENTAJE_ANOMALIA))
    for indice in random.sample(range(len(eventos)), cantidad):
        eventos[indice]["minutos_reproducidos"] = random.choice([None, 0])
    return cantidad


def leer_cantidad():
    if len(sys.argv) < 2:
        return CANTIDAD_MINIMA
    try:
        cantidad = int(sys.argv[1])
    except ValueError:
        print(f"Cantidad inválida '{sys.argv[1]}'. Se usarán {CANTIDAD_MINIMA} eventos.")
        return CANTIDAD_MINIMA
    if cantidad < CANTIDAD_MINIMA:
        print(f"La cantidad mínima de eventos es {CANTIDAD_MINIMA}.")
        sys.exit(1)
    return cantidad


def main():
    cantidad = leer_cantidad()
    eventos, extra, tv_noche, dispositivos = construir_eventos(cantidad)
    cantidad_anomalias = inyectar_anomalias(eventos)

    try:
        productor = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            linger_ms=20,
        )
    except KafkaError as exc:
        print(f"No se pudo conectar a Kafka en {BOOTSTRAP_SERVERS}: {exc}")
        sys.exit(1)

    print(
        f"Enviando {len(eventos)} eventos al tópico '{TOPIC}' "
        f"({cantidad} base + {extra} extra por Smart TV de noche)..."
    )

    futures = []
    errores = 0
    for evento in eventos:
        try:
            futures.append(productor.send(TOPIC, value=evento))
        except Exception as exc:
            errores += 1
            print(f"Error al encolar evento: {exc}")

    enviados = 0
    for future in futures:
        try:
            future.get(timeout=30)
            enviados += 1
        except Exception as exc:
            errores += 1
            print(f"Error al enviar evento: {exc}")

    productor.flush()
    productor.close()

    print(f"Eventos enviados: {enviados}")
    print(f"Eventos con anomalía (minutos None/0): {cantidad_anomalias}")
    print(
        f"Smart TV de noche (20:00-23:59): {tv_noche} "
        f"({tv_noche / len(eventos):.1%} del total)"
    )
    print(f"Distribución de dispositivos: {dispositivos}")
    print(f"Errores: {errores}")


if __name__ == "__main__":
    main()