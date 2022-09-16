import contextlib
from json import dumps
import json
from time import sleep
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import os
import json

KAFKA_SERVERS = os.getenv(
    'KAFKA_SERVERS',
    'localhost:9091,localhost:9092,localhost:9093',
).split(',')

print('KAFKA_SERVERS', KAFKA_SERVERS)

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_SERVERS,
)

def create_kafka_topic(topic_name):
    topic_list = [
        NewTopic(
            name=topic_name,
            num_partitions=10,
            replication_factor=2,
            topic_configs={"retention.ms": "-1"},
        ),
    ]

    with contextlib.suppress(Exception):
        admin_client.create_topics(
            new_topics=topic_list,
            validate_only=False,
        )

RAW_TOPICS = [
    'twitter',
]

print('RAW_TOPICS', RAW_TOPICS)

def get_consumer(
    group_id='consumer-id-2',
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    key_deserializer=lambda x: json.loads(x.decode("utf-8")),
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=1000,
):
    consumer = KafkaConsumer(
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        bootstrap_servers=KAFKA_SERVERS,
        enable_auto_commit=enable_auto_commit,
        key_deserializer=key_deserializer,
        value_deserializer=value_deserializer,
        consumer_timeout_ms=consumer_timeout_ms,
    )

    consumer.subscribe(RAW_TOPICS)
    return consumer


def process(event):
    event_data = event.value
    print(event_data)
    
def treat_message(message):
    words = message.split()

    for word in words:
        print('WORD')
        print(word)
        producer.send(topic='twitter', value=a, key=a)

if __name__ == '__main__':

    consumer = get_consumer()

    create_kafka_topic('twitter_treated_messages')

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        request_timeout_ms=1000
    )

    while True:
        try:
            for event in consumer:
                print(f"{event.topic}: {event.value}" )
                message = json.loads(event.value)['data']['text']
                # print('MESSAGE')
                # print(message)
                # treat_message(message)

                words = message.split()

                for word in words:
                    print('WORD')
                    print(word)
                    producer.send(topic='twitter_treated_messages', value=word, key=word)

                sleep(2)
        
        except Exception as exc:
            print(exc)



