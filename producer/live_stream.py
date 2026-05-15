import argparse
import time
import sys
from loguru import logger
from .event_generator import EventGenerator
from .kafka_producer import KafkaProducer

def run_live_stream(events_per_minute: int):
    """
    Continuously generates and sends events to Kafka at a fixed rate.
    """
    logger.info(f"Starting live stream: {events_per_minute} events per minute...")
    
    try:
        producer = KafkaProducer()
    except Exception as e:
        logger.error(f"Failed to initialize Kafka Producer: {e}")
        sys.exit(1)

    delay = 60.0 / events_per_minute
    
    try:
        while True:
            event = EventGenerator.generate_event()
            producer.send_event(event)
            logger.info(f"Sent {event['event_type']} | {event['event_id']}")
            time.sleep(delay)
    except KeyboardInterrupt:
        logger.info("Live stream stopped by user.")
    finally:
        producer.flush()
        logger.info("Producer flushed. Exiting.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Live stream SaaS events to Kafka.")
    parser.add_argument("--rate", type=int, default=15, help="Events per minute")
    
    args = parser.parse_args()
    
    # Set log level
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    
    run_live_stream(args.rate)
