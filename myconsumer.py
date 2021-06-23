import json
import requests
import logging
from consumer import Consumer

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class Message:
    def __init__(self, props: dict):
        self.entityType = props['entityType']
        self.action = props['action']
        self.tenantId = props['tenantId']
        self.uniqueIdentifier = props['uniqueIdentifier']
        self.data = props['data']


class MyConsumer(Consumer):
    def __init__(self, props: dict):
        super(MyConsumer, self).__init__(props)

    def process(self, message) -> dict:
        message = Message(json.loads(message.value))
        entity_type = message.entityType
        action = message.action
        data = message.data
        payload = {}

        if entity_type == 'user':
            payload = [
                {
                    'id': data['id'],
                    'email': data['email']
                }
            ]

        elif entity_type == 'group':
            if action == 'create':
                payload = [
                    {
                        'id': data['id'],
                        'name': data['name']
                    }
                ]

            elif action == 'update' or action == 'delete':
                payload = [
                    {
                        'id': data['id'],
                        'name': data['name'],
                        'users': [user['id'] for user in data['users']] if type(data['users']) == list else data['users']['id']
                    }
                ]

        elif entity_type == 'acl':
            payload = data

        return {
            'entity_type': entity_type,
            'action': action,
            'payload': payload
        }

    def emit(self, message: dict):
        base_url = 'http://127.0.0.1:5000/actionHub/api/'

        ent_type = {'user': 'user_update/',
                    'group': 'um_sync/groups/',
                    'acl': 'um_sync/acl/'}

        if message['action'] == 'create':
            # todo : add retry mechanism, authentication and timeout
            res = requests.post(base_url + ent_type[message['entity_type']], json=message['payload'])
            logger.info(res.text)
        elif message['action'] == 'update':
            res = requests.put(base_url + ent_type[message['entity_type']], json=message['payload'])
            logger.info(res.text)
        elif message['payload'] == 'delete':
            res = requests.delete(base_url + ent_type[message['entity_type']], json=message['payload'])
            logger.info(res.text)


if __name__ == "__main__":
    consumer = MyConsumer({
        'topic_name': 'my_topic1',
        'bootstrap_servers': 'localhost:9092',
        'auto_offset_reset': 'earliest',
        'group_id': 'my_group'
    })
    consumer.start_consume()
