## installation guide kafka
```
https://www.youtube.com/watch?v=lijWUsVN-mM

https://towardsdatascience.com/connecting-the-dots-python-spark-and-kafka-19e6beba6404

```

## run kafka and zookeeper server
```
zookeeper-server-start.sh config/zookeeper.properties

kafka-server-start.sh config/server.properties
```


## create topic in kafka
```
bin/kafka-topics.sh --create --topic INFERENCE --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

bin/kafka-topics.sh --create --topic EMAIL --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

bin/kafka-topics.sh --create --topic NOTIFICATION --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

```

## install kafka python
```
pip install kafka-python flask flask_cors
```

## spark-kafka
```
https://medium.com/data-arena/enabling-streaming-data-with-spark-structured-streaming-and-kafka-93ce91e5b435

https://github.com/cordon-thiago/spark-kafka-consumer
```