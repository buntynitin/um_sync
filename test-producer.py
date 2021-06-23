from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.producer.kafka import log
import json


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)

future = producer.send("my_topic1", {
        "entityType": "group",
        "action": "update",
        "tenantId": "24214124",
        "uniqueIdentifier": "3424234-23423423-4234234",
        "data": {
            "id": "1",
            "name": "mygroup",
            "first_name": "hey",
            "middle_name": "there",
            "email": "email",
            "users": {
                'id':"132"
            }
        }
    })


try:
    record_metadata = future.get(timeout=10)
    print(record_metadata)
except KafkaError:
    log.exception()
    pass

# Successful result returns assigned partition and offset

