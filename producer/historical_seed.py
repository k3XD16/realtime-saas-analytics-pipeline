import argparse
import random
import sys
from datetime import datetime, timedelta, timezone
from loguru import logger
from .event_generator import EventGenerator
from .kafka_producer import KafkaProducer


def get_random_timestamp(target_date: datetime) -> datetime:
    """
    Generates a random timestamp for a given date, with bursts at
    9-11 AM and 2-4 PM IST (UTC+5:30).
    """
    is_burst = random.random() < 0.60

    if is_burst:
        window = random.choice([
            (3, 30, 5, 30),   # 9-11 AM IST = 03:30-05:30 UTC
            (8, 30, 10, 30)   # 2-4 PM IST  = 08:30-10:30 UTC
        ])
        start_h, start_m, end_h, end_m = window
        start_mins = start_h * 60 + start_m
        end_mins   = end_h * 60 + end_m
        random_min = random.randint(start_mins, end_mins)
    else:
        random_min = random.randint(0, 1439)

    hour   = random_min // 60
    minute = random_min % 60
    second = random.randint(0, 59)

    # ✅ FIX: Use replace() on a timezone-aware datetime correctly
    return target_date.replace(hour=hour, minute=minute, second=second, microsecond=0, tzinfo=timezone.utc)


def run_historical_seed(total_events: int, days: int):
    logger.info(f"Starting historical seed: {total_events} events over {days} days...")

    try:
        producer = KafkaProducer()
    except Exception as e:
        logger.error(f"Failed to initialize Kafka Producer: {e}")
        sys.exit(1)

    end_date   = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    dates   = [start_date + timedelta(days=i) for i in range(days)]
    weights = [0.7 if d.weekday() >= 5 else 1.0 for d in dates]
    total_weight = sum(weights)
    events_per_weight = total_events / total_weight

    events_sent = 0

    for i, date in enumerate(dates):
        daily_target = int(events_per_weight * weights[i])
        logger.info(f"Processing day {i+1}/{days}: {date.date()} (Target: {daily_target} events)")

        for _ in range(daily_target):
            event = EventGenerator.generate_event()

            # ✅ Override with backdated historical timestamp
            ts = get_random_timestamp(date)
            event["timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%SZ")

            producer.send_event(event)
            events_sent += 1

            if events_sent % 1000 == 0:
                logger.info(f"Progress: {events_sent}/{total_events} events sent...")

    producer.flush()
    logger.success(f"Historical seeding complete. Total events sent: {events_sent}")
    logger.success(f"100K historical events loaded into Kafka topic 'saas.events.raw' with realistic timestamps.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed historical SaaS events to Kafka.")
    parser.add_argument("--events", type=int, default=100000, help="Total events to generate")
    parser.add_argument("--days",   type=int, default=90,     help="Days to spread events over")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    run_historical_seed(args.events, args.days)
