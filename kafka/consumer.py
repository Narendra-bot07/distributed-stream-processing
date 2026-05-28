from confluent_kafka import Consumer
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'bms-tracker',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe(['movies', 'ipl2026', 'events'])

logger.info("Started consuming...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        topic = msg.topic()
        data = json.loads(msg.value().decode('utf-8'))

        if topic == "movies":
            logger.info(f"[MOVIE BOOKING] {data}")

        elif topic == "ipl2026":
            logger.info(f"[IPL MATCH] {data}")

        elif topic == "events":
            logger.info(f"[EVENT BOOKING] {data}")

        else:
            logger.warning(f"[UNKNOWN TOPIC] {topic} → {data}")

except KeyboardInterrupt:
    logger.info("Stopping consumer...")

finally:
    consumer.close()