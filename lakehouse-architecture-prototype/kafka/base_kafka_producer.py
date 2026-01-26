from confluent_kafka import Producer
import logging

class BaseKafkaProducer:
    def __init__(self, bootstrap_servers: str, config: dict = None):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
        }
        if config:
            self.config.update(config)
        self.producer = Producer(self.config)

    def delivery_callback(self, err, msg):
        if err:
            logging.error(f'Message delivery failed: {err}')
        else:
            logging.info(f'Message delivered to {msg.topic()} Partition #{msg.partition()}')

    def produce(self, topic: str, value: str, key: str = None):
        try:
            self.producer.produce(
                topic=topic,
                value=value.encode('utf-8'),
                key=key.encode('utf-8') if key else None,
                callback=self.delivery_callback
            )
            self.producer.poll(0)
        except BufferError as e:
            logging.error(f"Buffer error: {e}")
        except Exception as e:
            logging.error(f'Failed to produce message: {e}')

    def flush(self, timeout: float = 5.5):
        self.producer.flush(timeout)

    def close(self):
        self.flush()