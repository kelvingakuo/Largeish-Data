
import logging
import asyncio
import json
from base_kafka_producer import BaseKafkaProducer # If running script from host, use 'from .base_kafka_producer import BaseKafkaProducer'   
from coinbase_websockets_connector import CoinbaseConnector

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    producer = BaseKafkaProducer(bootstrap_servers='kafka:29092') # If running script from host, use 'localhost:9092'. From within Docker container, use 'kafka:29092'.
    
    async def message_handler(message):
        producer.produce(topic='coinbase', value=json.dumps(message), key="BTC-USD")

    connector = CoinbaseConnector(product_id="BTC-USD", channel="ticker")
    asyncio.run(connector.run(message_handler))