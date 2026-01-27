
import logging
import asyncio
import json
from base_kafka_producer import BaseKafkaProducer # If running script from host, use 'from .base_kafka_producer import BaseKafkaProducer'   
from kraken_websockets_connector import KrakenConnector

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    producer = BaseKafkaProducer(bootstrap_servers='kafka:29092') # If running script from host, use 'localhost:9092'. From within Docker container, use 'kafka:29092'.
    
    async def message_handler(message):
        key = message["data"][0].get("symbol") if "data" in message and len(message["data"]) > 0 else None
        producer.produce(topic='kraken', value=json.dumps(message), key=key)

    async def run_all_connectors():
        btc_connector = KrakenConnector(pair="BTC/USD", channel="ticker")
        eth_connector = KrakenConnector(pair="ETH/USD", channel="ticker")
        
        await asyncio.gather(
            btc_connector.run(message_handler),
            eth_connector.run(message_handler)
        )

    asyncio.run(run_all_connectors())