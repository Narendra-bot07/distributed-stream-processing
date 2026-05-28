from confluent_kafka import Producer
from faker import Faker
import json
import uuid
import random
import time
import threading
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
)

logger = logging.getLogger(__name__)

fake = Faker()

producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(
            f"{msg.topic()} | partition {msg.partition()} | offset {msg.offset()}"
        )

def stream_movies():
    movies_list = [
        "Avengers: Endgame",
        "Inception",
        "Interstellar",
        "The Dark Knight",
        "John Wick",
        "Oppenheimer",
        "Dune",
        "The Matrix",
        "Gladiator",
        "Titanic"
    ]

    while True:
        data = {
            "booking_id": str(uuid.uuid4()),
            "user": fake.name(),
            "movie": random.choice(movies_list),
            "city": fake.city(),
            "seats": random.randint(1, 5),
            "price": random.randint(150, 500),
            "timestamp": fake.iso8601()
        }

        try:
            producer.produce(
                topic="movies",
                key=data["movie"],
                value=json.dumps(data).encode("utf-8"),
                callback=delivery_report
            )
        except BufferError as e:
            logger.warning(f"Producer buffer full: {e}")

        producer.poll(0)
        time.sleep(1)


def stream_ipl():
    teams = [
        "CSK", "MI", "RCB", "KKR", "SRH",
        "DC", "PBKS", "RR", "GT", "LSG"
    ]

    while True:
        t1, t2 = random.sample(teams, 2)

        data = {
            "match_id": str(uuid.uuid4()),
            "team1": t1,
            "team2": t2,
            "stadium": fake.city(),
            "tickets_sold": random.randint(1000, 50000),
            "price": random.randint(500, 3000),
            "timestamp": fake.iso8601()
        }

        try:
            producer.produce(
                topic="ipl2026",
                key=f"{t1}_vs_{t2}",
                value=json.dumps(data).encode("utf-8"),
                callback=delivery_report
            )
        except BufferError as e:
            logger.warning(f"Producer buffer full: {e}")

        producer.poll(0)
        time.sleep(1.5)


def stream_events():
    event_types = ["Concert", "Standup Comedy", "Tech Meetup", "Theatre"]

    while True:
        data = {
            "event_id": str(uuid.uuid4()),
            "event_type": random.choice(event_types),
            "artist": fake.name(),
            "location": fake.city(),
            "tickets": random.randint(50, 5000),
            "price": random.randint(200, 2000),
            "timestamp": fake.iso8601()
        }

        try:
            producer.produce(
                topic="events",
                key=data["event_type"],
                value=json.dumps(data).encode("utf-8"),
                callback=delivery_report
            )
        except BufferError as e:
            logger.warning(f"Producer buffer full: {e}")

        producer.poll(0)
        time.sleep(2)


if __name__ == "__main__":
    t1 = threading.Thread(target=stream_movies, name="Movies-Thread")
    t2 = threading.Thread(target=stream_ipl, name="IPL-Thread")
    t3 = threading.Thread(target=stream_events, name="Events-Thread")

    logger.info("Starting streaming threads...")

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()