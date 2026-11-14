import json
import sys
from kafka import KafkaConsumer

TOPIC = "topic_streaming"
BOOTSTRAP_SERVERS = "localhost:9092"
ARCHIVO_SALIDA = sys.argv[1] if len(sys.argv) > 1 else "eventos_streaming.jsonl"
MUESTRA_LIMITE = 5


def main():
    consumidor = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="grupo_consumidor_solemne2",
    )

    eventos_guardados = 0
    errores_decodificacion = 0

    print(f"Consumidor iniciado. Escuchando el tópico '{TOPIC}'...")
    print(f"Guardando eventos en '{ARCHIVO_SALIDA}'. Presiona Ctrl+C para detener.\n")

    with open(ARCHIVO_SALIDA, mode="w", encoding="utf-8") as archivo:
        try:
            for mensaje in consumidor:
                try:
                    texto_crudo = mensaje.value.decode("utf-8")
                    evento = json.loads(texto_crudo)

                    archivo.write(json.dumps(evento, ensure_ascii=False) + "\n")
                    archivo.flush()
                    eventos_guardados += 1

                    if eventos_guardados <= MUESTRA_LIMITE:
                        print(
                            f"[Muestra #{eventos_guardados} | Offset: {mensaje.offset}] {evento}"
                        )

                    if eventos_guardados % 1000 == 0:
                        print(f"[...] {eventos_guardados} eventos recibidos y guardados...")

                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    errores_decodificacion += 1
                    print(
                        f"[ERROR DE DECODIFICACIÓN] Offset {mensaje.offset}: "
                        f"No se pudo interpretar el mensaje ({exc})"
                    )

        except KeyboardInterrupt:
            print("\nInterrupción manual detectada (Ctrl+C). Deteniendo consumidor...")

        finally:
            consumidor.close()
            print("\n" + "=" * 50)
            print("RESUMEN DE EJECUCIÓN DEL CONSUMIDOR")
            print(f"Tópico suscrito        : {TOPIC}")
            print(f"Archivo generado       : {ARCHIVO_SALIDA}")
            print(f"Total eventos guardados: {eventos_guardados}")
            print(f"Mensajes con error     : {errores_decodificacion}")
            print("=" * 50)


if __name__ == "__main__":
    main()