import os
import logging
# from dotenv import load_dotenv
from kafka import (KafkaConsumer, TopicPartition)
import time


# load_dotenv()
# logging.basicConfig(level=logging.INFO)

kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = "SCRAM-SHA-512"

# redpanda_cloud_brokers = os.getenv("REDPANDA_BROKERS")
# redpanda_service_account = os.getenv("REDPANDA_SERVICE_ACCOUNT")
# redpanda_service_account_password = os.getenv("REDPANDA_SERVICE_ACCOUNT_PASSWORD")


redpanda_cloud_brokers = ["redpanda-0.customredpanda.local:31092"]
redpanda_service_account = "admin"
redpanda_service_account_password = "123456"
# logging.info(f"Connecting to: {redpanda_cloud_brokers}")

#
# Read from topic
#
consumer = KafkaConsumer(    
    bootstrap_servers=redpanda_cloud_brokers,    
    security_protocol=kafka_security_protocol,
    sasl_mechanism=kafka_sasl_mechanism,
    sasl_plain_username=redpanda_service_account,
    sasl_plain_password=redpanda_service_account_password,
    # group_id="g-twitch",
    group_id="try-gone",
    auto_offset_reset="earliest",
    enable_auto_commit="false",
    auto_commit_interval_ms=0,
    max_partition_fetch_bytes=1048576, 
    # ssl_cafile="ca.crt",
    # ssl_certfile="client.crt",
    # ssl_keyfile="client.key"
)

topic_name = "min_retention"
# topic_name = "testing1"
assignments = []
partitions = consumer.partitions_for_topic(topic_name)
try:
    for p in partitions:
        assignments.append(TopicPartition(topic_name, p))
except Exception as e:
    print(e)
    exit()

consumer.assign(assignments)
consumer.seek_to_beginning()

try:
    batch = consumer.poll(timeout_ms=10000)
    for records in batch.values():
        for r in records:
            print(r)
            # logging.info("topic: {}, partition: {}, offset: {}, value: {}".format(
            #     r.topic, r.partition, r.offset, r.value.decode()
            # ))
except Exception as e:
    print(e)
    # logging.error(e)
# finally:
#     consumer.close()