from kafka import KafkaConsumer

consumidor = KafkaConsumer(
    "topic_streaming",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,
)

total = sum(1 for _ in consumidor)
print(f"Mensajes en topic_streaming: {total}")
consumidor.close()