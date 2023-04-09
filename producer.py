import os
# import csv
# import glob
import logging
# import pathlib
import time

# from dotenv import load_dotenv
from kafka import (KafkaAdminClient, KafkaProducer)
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

# load_dotenv()
# logging.basicConfig(level=logging.INFO)

kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = "SCRAM-SHA-512"
redpanda_cloud_brokers = ["redpanda-0.customredpanda.local:31092"]
redpanda_service_account = "admin"
redpanda_service_account_password = "123456"

# logging.info(f"Connecting to: {redpanda_cloud_brokers}")

#
# Create topic
#
topic_name = "tes_pvc"
# logging.info(f"Creating topic: {topic_name}")

admin = KafkaAdminClient(
    bootstrap_servers=redpanda_cloud_brokers,
    security_protocol=kafka_security_protocol,
    sasl_mechanism=kafka_sasl_mechanism,
    sasl_plain_username=redpanda_service_account,
    sasl_plain_password=redpanda_service_account_password,
    # ssl_cafile="ca.crt",
    # ssl_certfile="client.crt",
    # ssl_keyfile="client.key"
)
# try:
#     admin.create_topics(new_topics=[
#         NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
# except Exception TopicAlreadyExistsError as e:
#     print(e)
    # logging.error(e)

# print("jalan")
#
# Write to topic
#
logging.info(f"Writing to topic: {topic_name}")
producer = KafkaProducer(
    bootstrap_servers=redpanda_cloud_brokers,
    security_protocol=kafka_security_protocol,
    sasl_mechanism=kafka_sasl_mechanism,
    sasl_plain_username=redpanda_service_account,
    sasl_plain_password=redpanda_service_account_password,
    acks="all",
    linger_ms=1,
    key_serializer=str.encode,
    value_serializer=str.encode,
    # batch_size=163,
    # ssl_cafile="ca.crt",
    # ssl_certfile="client.crt",
    # ssl_keyfile="client.key"
)

i = 12038
try: 
    for i in range(0, 10):
    # while True:
        i += 1
        key = i % 5
        f = producer.send(topic_name, key='key-{}'.format(key), value='val-{}'.format(i))
        f.get(timeout=10)
        time.sleep(1)
    # here = pathlib.Path(__file__).parent.resolve()
    # for file in glob.glob(f"{here}/*.csv"):
    #     logging.info(f"Processing file: {file}")
    #     with open(file) as csvfile:
    #         reader = csv.reader(csvfile)
    #         next(reader, None)  # skip the header
    #         for row in reader:
    #             msg = ",".join(row)
    #             f = producer.send(topic_name, str.encode(msg))
    #             f.get(timeout=100)
    #             logging.info(f"Produced: {msg}")
except Exception as e:
    print(e)
    # logging.error(e)
finally:
    producer.flush()
    producer.close()
