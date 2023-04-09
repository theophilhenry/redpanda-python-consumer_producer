from kafka import KafkaConsumer

consumer = KafkaConsumer(
  bootstrap_servers=["redpanda-0.customredpanda.local:31092"],
  group_id="demo-group",
  auto_offset_reset="earliest",
  enable_auto_commit=False,
  consumer_timeout_ms=1000
)
consumer.subscribe("twitch_chat")
for message in consumer:
  topic_info = f"topic: {message.topic} ({message.partition}|{message.offset})"
  message_info = f"key: {message.key}, {message.value}"
  print(f"{topic_info}, {message_info}")