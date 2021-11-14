from confluent_kafka import Producer


p = Producer({'bootstrap.servers': '3.236.64.137:9092'})

topic_info=p.list_topics()

print(topic_info)