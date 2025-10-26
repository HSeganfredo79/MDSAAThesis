# flows/kafka_streaming.py
from prefect import flow
from flows.producer_flow import start_producer
from flows.consumer_flow import start_consumer

@flow(name="Kafka Streaming Loop")
def kafka_streaming_flow():
    start_producer()
    start_consumer()
