## before go into deep dive read the docs 
```
--------------------- how to add sink and source--------------------
https://hub.docker.com/r/landoop/fast-data-dev/dockerfile

-------------------------run docker image------------------------------------
https://github.com/lensesio/fast-data-dev
-----------------------------------------------------------------------------
```

## open port aws
```
i have open ssh 22 port
all tcp anywhere
all traffic anywhere
```

## let's deploy on aws
```
docker run -d --net=host -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e AWS_ACCESS_KEY_ID=dfddfdf -e AWS_SECRET_ACCESS_KEY=MiCdyT4w+v5wvnrAv3X4  -e USER=sinksdk -e PASSWORD=1234 -e ADV_HOST=3.14.27.172  landoop/fast-data-dev:1.0.1

```

## list all topic
```
docker run --rm -it --net=host landoop/fast-data-dev:1.0.1 kafka-topics --zookeeper 2.3.144.163.22:2181 --list
```
## create topic
```
docker run --rm -it --net=host landoop/fast-data-dev:1.0.1 kafka-topics --zookeeper 0.0.0.0:2181 --topic sinkaws --create --partitions 3 --replication-factor 1
```
