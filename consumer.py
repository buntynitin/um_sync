from abc import ABC, abstractmethod
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord


class Consumer(ABC):
    def __init__(self, props: dict):
        self.consumer = KafkaConsumer(
            props['topic_name'],
            bootstrap_servers=props['bootstrap_servers'],
            auto_offset_reset=props['auto_offset_reset'],
            group_id=props['group_id'])

    def start_consume(self):
        print("Consuming ...")
        for message in self.consumer:
            print(message)
            new_message = self.process(message)
            self.emit(new_message)

    @abstractmethod
    def process(self, message: ConsumerRecord) -> dict:
        pass

    @abstractmethod
    def emit(self, message: dict):
        pass
