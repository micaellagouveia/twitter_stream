import contextlib
from json import dumps
import json
from time import sleep
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import os
import requests

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


def kafka_is_running():
    topics = admin_client.list_topics()
    return bool(topics)


if __name__ == '__main__':

    create_kafka_topic('twitter')
    create_kafka_topic('twitter_treated_messages')

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        request_timeout_ms=1000
    )

    url = "https://api.twitter.com/2/tweets/search/stream"

    payload={}
    headers = {}


    def get_stream(url):
        s = requests.Session()

        with s.get(url, headers=headers, stream=True) as resp:
            for line in resp.iter_lines():
                if line:
                    yield line.decode("utf-8")
                    #print(type(line.decode("utf-8")))
                    #producer.send(topic='twitter', value=str(line.decode("utf-8")), key=str(line.decode("utf-8")))



    for a in get_stream(url):
        print(a)
        producer.send(topic='twitter', value=a, key=a)

        sleep(2)
                
                    
                # producer.send(topic='send_words', value=word, key=word)
                # producer.send(topic='all_words_count', value=word, key=word)
                