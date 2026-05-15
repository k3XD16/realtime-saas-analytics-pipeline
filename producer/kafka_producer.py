from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from loguru import logger
from .config import Config

class KafkaProducer:
    """Handles publishing Avro-serialized events to Confluent Kafka."""

    def __init__(self):
        # Schema Registry Client
        sr_conf = {
            'url': Config.SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': f"{Config.SCHEMA_REGISTRY_API_KEY}:{Config.SCHEMA_REGISTRY_API_SECRET}"
        }
        self.schema_registry_client = SchemaRegistryClient(sr_conf)

        # Avro Serializer
        with open("producer/schemas/saas_event.avsc") as f:
            schema_str = f.read()
        
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            schema_str
        )
        self.string_serializer = StringSerializer('utf_8')

        # Kafka Producer
        producer_conf = {
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': Config.KAFKA_API_KEY,
            'sasl.password': Config.KAFKA_API_SECRET,
            'client.id': 'saas-producer'
        }
        self.producer = Producer(producer_conf)

    def delivery_report(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
            # In a real app, you might want to retry or send to DLQ here
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def send_event(self, event: dict):
        """Serializes and sends an event to Kafka."""
        try:
            self.producer.produce(
                topic=Config.KAFKA_TOPIC_RAW,
                key=self.string_serializer(event['event_id']),
                value=self.avro_serializer(
                    event, 
                    SerializationContext(Config.KAFKA_TOPIC_RAW, MessageField.VALUE)
                ),
                on_delivery=self.delivery_report
            )
            # Serve delivery reports from previous produce() calls
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error producing message to Kafka: {e}")
            self.send_to_dlq(event, str(e))

    def send_to_dlq(self, event: dict, error: str):
        """Sends failed events to the Dead Letter Queue."""
        logger.warning(f"Sending event {event.get('event_id')} to DLQ due to: {error}")
        try:
            # Simple string producer for DLQ (or use another Avro schema if preferred)
            self.producer.produce(
                topic=Config.KAFKA_TOPIC_DLQ,
                key=str(event.get('event_id')),
                value=str({"event": event, "error": error}),
                on_delivery=self.delivery_report
            )
        except Exception as e:
            logger.critical(f"Failed to send to DLQ: {e}")

    def flush(self):
        """Flushes the producer buffer."""
        self.producer.flush()
