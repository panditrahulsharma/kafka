## if confluent not up(getting error schema registry)

https://stackoverflow.com/questions/54441162/unable-to-start-confluent-schema-registry

## confluent commands
confluent local destroy

confluent local service stop

confluent local service up

confluent local services connect log



## installation guide kafka
```
https://www.youtube.com/watch?v=lijWUsVN-mM

https://towardsdatascience.com/connecting-the-dots-python-spark-and-kafka-19e6beba6404

```

## run kafka and zookeeper server
```
./bin/zookeeper-server-start.sh config/zookeeper.properties

./bin/kafka-server-start.sh config/server.properties
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

https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams#Stream_selection

https://github.com/cordon-thiago/spark-kafka-consumer
```
## install elk
```
https://github.com/panditrahulsharma/docker-elk
```

## Single Node-Multiple Brokers Configuration
Create Multiple Kafka Brokers − We have one Kafka broker instance already in con-fig/server.properties. Now we need multiple broker instances, so copy the existing server.prop-erties file into two new config files and rename it as server-one.properties and server-two.prop-erties. Then edit both new files and assign the following changes −

        confluent loc=/home/rahul/confluent/confluent-7.0.0/etc/kafka/config/server-one.properties
        config/server-one.properties
        # The id of the broker. This must be set to a unique integer for each broker.
        broker.id=1
        # The port the socket server listens on
        port=9093
        # A comma seperated list of directories under which to store log files
        log.dirs=/tmp/kafka-logs-1


## databricks kafka deltalake 
https://databricks.com/blog/2017/04/04/real-time-end-to-end-integration-with-apache-kafka-in-apache-sparks-structured-streaming.html
